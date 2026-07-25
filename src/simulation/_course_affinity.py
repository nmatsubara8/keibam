"""Phase B: 出走馬 × コース相性 → RaceField.ability の馬別補正（「相性」）。

Phase A(_course_env) が「場の物理」を SimConfig（レース共通の物理定数）へ全馬一律に注入するのに対し、
本モジュールは「同じコースでも馬ごとに違う有利不利」を RaceField（馬別の能力ベクトル）へ注入する。
course_master の定性プロファイル（脚質バイアス/時計傾向 等）と各馬の as-of 特徴（脚質/スピード指数）
を掛け合わせ、field_from_featured が作った ability を馬別に微修正する。

設計思想（Phase A と同一の流儀）:
- **非侵襲**: field_from_featured / monte_carlo は無改変。その出力 RaceField を後段で補正する薄い純関数。
  呼び出し側は `field_r = field_for_course(field, race_df, ctx)` として per-race field を作るだけ。
- **参照点中立**: バイアス0（平均的コース）や馬側特徴が中立なら factor=1.0 で base field を厳密再現。
  → プロファイル未知/欠損なら補正0（base のまま）＝後方互換・スキーマ寛容。
- **前進安全**: course_* は静的コース属性、馬側は as-of 特徴（脚質/スピード指数）のみ。着順・当日結果は
  使わない（学習/ライブ列パリティ）。
- **中心化した加減点**: 両者を中心からの偏差で掛ける（run_style_bias × (0.5−脚質)）。参照（バイアス0
  or 平均的脚質）なら効果0で、校正済み ability レベルを壊さずコース相性の差だけ乗せる。

写像（Phase B の対象は「出走馬×コースの per-horse 相性」。全馬一律の場の物理は Phase A）:
- run_style_bias × 脚質 → ability: 前有利コース×先行馬→+ / ×追込馬→−（差し有利コースは符号反転）。
  ※Phase A「直線長→終盤到達」は全馬一律の後傾シフト（場）。本軸は「その馬の脚質が合うか」（相性）で
    別軸。二重計上を避けるため Phase B ゲインは Phase A 適用後の残差で校正する（calibrate 側の役割）。
- time_bias × スピード型 → ability: 高速馬場×スピード指数上位→+ / タフ馬場×スピード型→−（底力型が相対
    優位）。スピード型はレース内 z-score で測る（能力レベル交絡を除く相対量）。

予約（CourseContext に解決のみ・本 Phase では未写像。馬側の確実な列が揃い次第 Phase B+ で追加）:
- turf_type_code（芝種）× 洋芝実績、drainage_good（水はけ）× 道悪実績。
  ※going/wet 適性は既に RaceField.going_apt が担うため、drainage は going と二重計上しない設計に要注意。
"""
from __future__ import annotations

import dataclasses
from dataclasses import dataclass

import numpy as np
import pandas as pd

from src.simulation._course_env import CourseContext

# 較正ファイル内で CourseAffinityParams ゲインに使う接頭辞（Phase A の ce_ と対。ca_=course affinity）。
COURSE_AFFINITY_GAIN_PREFIX = "ca_"


@dataclass(frozen=True)
class CourseAffinityParams:
    """出走馬×コース相性 → RaceField.ability 補正の強さ（校正対象）。ゲイン0で当該軸 off。"""

    style_gain: float = 0.12   # run_style_bias × (0.5−脚質) の強さ
    time_gain: float = 0.06    # time_bias × スピード型(z) の強さ
    ability_lo: float = 0.85   # 馬別 ability 係数の下限（相性は穏当な相対補正に留める）
    ability_hi: float = 1.15   # 上限


def course_affinity_params_from_mapping(
        params: dict,
        prefix: str = COURSE_AFFINITY_GAIN_PREFIX) -> "CourseAffinityParams | None":
    """<prefix><field> 形式の較正値マップ → CourseAffinityParams（該当キーが無ければ None）。

    calibrate_sim.py が best_params に ca_* として保存したゲインを consumer が復元する共通経路。
    """
    kw = {k[len(prefix):]: v for k, v in params.items() if k.startswith(prefix)}
    return CourseAffinityParams(**kw) if kw else None


def _num(race_df: pd.DataFrame, col: str):
    return pd.to_numeric(race_df[col], errors="coerce") if col in race_df.columns else None


def _race_z(s: pd.Series) -> np.ndarray:
    """レース内 z-score（NaN→0 中立）。有効値<2 なら全 0（軸を無効化）。"""
    v = s.to_numpy(dtype=float)
    finite = np.isfinite(v)
    if finite.sum() < 2:
        return np.zeros_like(v)
    mu = float(np.nanmean(v))
    sd = float(np.nanstd(v))
    z = (v - mu) / (sd if sd > 1e-9 else 1.0)
    return np.where(finite, z, 0.0)


def field_for_course(field, race_df: pd.DataFrame, ctx: CourseContext,
                     params: CourseAffinityParams | None = None):
    """base field を出走馬×コース相性 ctx で補正した per-race field を返す（ability のみ修正）。

    race_df の行順は field 構築時と同一（field_from_featured が順序保存）＝馬別特徴を直接対応させる。
    プロファイル未知 or 馬側特徴が中立なら factor=1.0＝base を厳密再現。field 自体は変更しない。
    """
    params = params or CourseAffinityParams()
    n = field.n
    factor = np.ones(n)

    # (1) 脚質バイアス × 脚質: frontness = 0.5−leg_type（+先行/−追込, NaN→0 中立）。
    #     前有利コース(bias>0)×先行(frontness>0)→+、×追込→−。差し有利(bias<0)は符号反転。
    if ctx.run_style_bias is not None and params.style_gain:
        lt = _num(race_df, "leg_type_binary")
        if lt is not None:
            frontness = 0.5 - lt.to_numpy(dtype=float)
            frontness = np.where(np.isfinite(frontness), frontness, 0.0)
            factor *= 1.0 + params.style_gain * ctx.run_style_bias * frontness

    # (2) 時計傾向 × スピード型: speed_z（レース内 z, NaN→0）。高速馬場(bias>0)×速い馬→+、タフ×速い馬→−。
    if ctx.time_bias is not None and params.time_gain:
        sp = _num(race_df, "speed_fig_best")
        if sp is None:
            sp = _num(race_df, "speed_fig_mean5")
        if sp is not None:
            speed_z = _race_z(sp)
            factor *= 1.0 + params.time_gain * ctx.time_bias * speed_z

    if np.allclose(factor, 1.0):
        return field   # 相性効果なし＝完全後方互換（新インスタンスを作らない）

    factor = np.clip(factor, params.ability_lo, params.ability_hi)
    # ability は field_from_featured の定義域 [0.3, 2.0] に留める（base の clip と整合）。
    new_ability = np.clip(field.ability * factor, 0.3, 2.0)
    return dataclasses.replace(field, ability=new_ability)
