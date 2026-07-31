"""Phase 2: featured の as-of 特徴量から エージェントシミュレーションの入力パラメータを作る。

field_from_featured(race_df) は「1レース分の featured 行群」→ RaceField（能力μ/脚質/スタミナ/σ）。
使う列はすべて**発走前に既知の as-of 特徴**（過去走から算出済み）だけなので前進安全（着順・単勝は
使わない）。列が無い環境でも中立既定へフォールバックする（manji 因子と同じスキーマ寛容の作法）。

ヒューリスティック（v1・学習なし）:
- ability : 能力シグナル（speed_fig/勝率/相対着順/Elo）をレース内 z-score 合成し 1.0 中心へ。
- style   : leg_type_binary（<0.4 先行 / >0.6 追込 / 中間 差し）。無ければ全 stalker。
- stamina : キャリア厚み（出走数）と距離実績を 0.8–1.4 にマップ。無ければ 1.0。
- noise   : 過去着順のばらつき（大きいほど不安定）を σ に。無ければ既定。

これは確率生成の**入力**であり、質は特徴量の質で決まる。市場を越えるかは Phase3 で判定する。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.simulation._agent_race import (
    STYLE_CLOSER,
    STYLE_FRONT,
    STYLE_STALKER,
    RaceField,
)

# 能力シグナル（列名, 符号）。符号+1=大きいほど強い、-1=小さいほど強い。
_ABILITY_SIGNALS = [
    ("speed_fig_best", 1.0),
    ("speed_fig_mean5", 1.0),
    ("win_rate_5R", 1.0),
    ("place_rate_5R", 0.5),
    ("avg_rel_rank_5R", -1.0),
    ("elo_rating", 1.0),
    ("elo_vs_field", 1.0),
]


def _num(df: pd.DataFrame, col: str):
    return pd.to_numeric(df[col], errors="coerce") if col in df.columns else None


def _zscore(s: pd.Series) -> pd.Series:
    sd = float(s.std())
    return (s - float(s.mean())) / (sd if sd > 1e-9 else 1.0)


_GOING_WEIGHT = {"良": 0.0, "稍重": 0.33, "重": 0.67, "不良": 1.0}


def _going_level(race_df: pd.DataFrame) -> float:
    """ダミー化された ground_state{1,2}_{良/稍重/重/不良} から going∈[0,1] を復元する。

    芝は ground_state1_・ダートは ground_state2_ のどちらか一方のみ立つ設計なので、
    各レベルのダミー(0/1)平均に重みを掛けて総和すれば当該レースの馬場水準になる。
    列名は単/二重アンダースコア両対応。見つからなければ 0（良＝無効果）。
    """
    total = 0.0
    for pre in ("ground_state1_", "ground_state1__", "ground_state2_", "ground_state2__",
                "ground_state_", "ground_state__"):
        for lvl, w in _GOING_WEIGHT.items():
            col = pre + lvl
            if col in race_df.columns:
                frac = float(pd.to_numeric(race_df[col], errors="coerce").fillna(0.0).mean())
                total += w * frac
    return float(min(1.0, max(0.0, total)))


def _race_type_is(race_df: pd.DataFrame, kind: str) -> bool:
    """当該レースの race_type が kind（"ダート"/"障害"/"芝"）か。生列 or ダミー両対応。"""
    if "race_type" in race_df.columns:
        vals = race_df["race_type"].astype(str)
        return bool((vals == kind).mean() > 0.5)
    for col in (f"race_type_{kind}", f"race_type__{kind}"):
        if col in race_df.columns:
            return bool(pd.to_numeric(race_df[col], errors="coerce").fillna(0.0).mean() > 0.5)
    return False


def _ability_z(race_df: pd.DataFrame) -> pd.Series:
    """利用可能な能力シグナルをレース内 z-score して平均合成（無ければ 0）。"""
    parts = []
    for col, sign in _ABILITY_SIGNALS:
        s = _num(race_df, col)
        if s is not None and int(s.notna().sum()) >= 2:
            parts.append(sign * _zscore(s).fillna(0.0))
    if not parts:
        return pd.Series(0.0, index=race_df.index)
    return sum(parts) / len(parts)


def field_from_featured(
    race_df: pd.DataFrame,
    *,
    ability_spread: float = 0.20,
    stamina_lo: float = 0.8,
    stamina_hi: float = 1.4,
    noise_base: float = 0.04,
    noise_gain: float = 0.02,
    rank_gain: float = 0.0,
) -> RaceField:
    """1レースの featured 行群 → RaceField（前進安全・スキーマ寛容）。

    rank_gain: 騎手＋厩舎ランクの ability 加減点の強さ（既定 0＝opt-in）。featured に事前計算した
    `rank_bonus` 列（build_rank_bonus.py・_rank_bonus.attach_rank_bonus）がある場合のみ効く。
    ⚠ 単一スナップ全期間適用＝過去に未来ランクが混入する leak（探索用・live には transfer しない）。
    """
    n = len(race_df)
    z = _ability_z(race_df).to_numpy()
    ability = np.clip(1.0 + ability_spread * z, 0.3, 2.0)

    # 騎手＋厩舎ランク加減点（opt-in・rank_bonus 列がある時だけ）。ability に直接加点して再 clip。
    if rank_gain:
        rb = _num(race_df, "rank_bonus")
        if rb is not None:
            ability = np.clip(
                ability + rank_gain * np.nan_to_num(rb.to_numpy(dtype=float)), 0.3, 2.0)

    # 脚質: leg_type_binary から（無ければ全 stalker）
    style = np.full(n, STYLE_STALKER, dtype=int)
    lt = _num(race_df, "leg_type_binary")
    if lt is not None:
        v = lt.to_numpy()
        style = np.where(v < 0.4, STYLE_FRONT,
                         np.where(v > 0.6, STYLE_CLOSER, STYLE_STALKER))
        style = np.where(np.isnan(v), STYLE_STALKER, style).astype(int)

    # スタミナ: 出走数(経験) をレース内 z→[lo,hi]。無ければ 1.0。
    stamina = np.ones(n)
    exp = _num(race_df, "n_starts")
    if exp is None:
        exp = _num(race_df, "career_starts")
    if exp is not None and int(exp.notna().sum()) >= 2:
        ez = _zscore(exp).fillna(0.0).to_numpy()
        mid = 0.5 * (stamina_lo + stamina_hi)
        half = 0.5 * (stamina_hi - stamina_lo)
        stamina = np.clip(mid + half * np.tanh(ez), stamina_lo, stamina_hi)

    # ノイズ: 過去着順のばらつき（大きいほど不安定）。無ければ既定。
    noise = np.full(n, noise_base)
    sd = _num(race_df, "着順_std_5R")
    if sd is not None and int(sd.notna().sum()) >= 2:
        sdz = _zscore(sd).fillna(0.0).to_numpy()
        noise = np.clip(noise_base + noise_gain * sdz, 0.01, 0.2)

    # ゲート（枠順）: 馬番（無ければ枠番）をレース内で [0,1] に正規化（0=最内..1=最外）。
    # 序盤の位置取り優位に使う（前進安全: 発走前に確定する枠順は as-of 情報）。無ければ中立。
    gate = None
    draw = _num(race_df, "馬番")
    if draw is None or int(draw.notna().sum()) < 2:
        draw = _num(race_df, "枠番")
    if draw is not None and int(draw.notna().sum()) >= 2:
        dv = draw.to_numpy(dtype=float)
        lo, hi = np.nanmin(dv), np.nanmax(dv)
        if hi > lo:
            g = (dv - lo) / (hi - lo)
            gate = np.where(np.isnan(g), 0.5, g)

    # 回り(右/左)適性: around_rel_rank（低い=得意）をレース内 z で反転（+得意/−不得意）。
    # 発走前に確定する回り方向×as-of過去成績＝前進安全。無ければ中立(0)。
    turn_apt = None
    arr = _num(race_df, "around_rel_rank")
    if arr is not None and int(arr.notna().sum()) >= 2:
        turn_apt = (-_zscore(arr).fillna(0.0)).to_numpy()

    # コース状態（馬場）: ダミー化された ground_state{1,2}_{良/稍重/重/不良} から going∈[0,1] を復元
    # （良=0/稍重≈0.33/重≈0.67/不良=1）。芝は _1・ダートは _2 のどちらか一方のみ立つので和で取れる。
    going = _going_level(race_df)
    # 馬場適性: wet_rel_rank（道悪の相対着順・低い=得意）をレース内 z で反転（+得意/−不得意）。無ければ中立。
    going_apt = None
    wr = _num(race_df, "wet_rel_rank")
    if wr is not None and int(wr.notna().sum()) >= 2:
        going_apt = (-_zscore(wr).fillna(0.0)).to_numpy()

    # レース種別（イベント条件）: 砂被り＝ダート、落馬率大＝障害。生 race_type 列 or ダミーから判定。
    is_dirt = _race_type_is(race_df, "ダート")
    is_jump = _race_type_is(race_df, "障害")

    return RaceField(ability=ability, style=style, stamina=stamina, noise=noise,
                     gate=gate, turn_apt=turn_apt, going=going, going_apt=going_apt,
                     is_dirt=is_dirt, is_jump=is_jump)
