"""Phase A: コース幾何（レース内定数）→ per-race SimConfig 上書き（「レース場の物理」）。

field_from_featured が「出走馬 × as-of 特徴」を RaceField（能力/脚質/スタミナ/σ）にするのに対し、
本モジュールは「その競馬場コースそのものの物理」を SimConfig 側に注入する。course_shape が付与した
course_* 幾何列（直線長/高低差/幅員/一周/コーナー曲率/スパイラル）を 1 レース分に解決し
（CourseContext）、それを既存 SimConfig の物理定数へ写像する（sim_config_for_course）。

設計思想（SimConfig 自身のコメントが招く「将来コース別データで置換可」を実体化する）:
- **参照点で校正済み base を厳密再現**: 各幾何を平均的コース(参照値)からの相対で modulate し、
  参照ちょうどの馬なら factor=1.0 で base をそのまま返す。絶対水準の校正結果を壊さず、コース差だけ
  乗せる。→ 幾何が未知/欠損なら全 knob が base のまま＝**後方互換・スキーマ寛容**。
- **前進安全**: course_* はレース単位の静的コース属性（着順・当日ラップを使わない）。学習/ライブとも
  同じ course_master CSV を参照し列パリティを保つ（_course_shape と同じ作法）。
- **非侵襲**: monte_carlo / RaceField は無改変。呼び出し側がループ内で
  `cfg_r = sim_config_for_course(base_cfg, ctx)` として per-race cfg を作るだけ。

写像（Phase A の対象は「場の物理スカラー」。出走馬 × コース相性の per-horse 加減点は Phase B）:
- 幅員 width_min/max → course_width（有効レース幅 → lane_capacity）。狭いほど前列定員↓＝前列争い激化。
  ※ course_* は物理馬場幅[m]（20-40m台）、SimConfig.course_width は「内めの有効レース幅」(既定7.7m)
    で単位が違うため、物理幅を参照幅で正規化した比で base をスケールする（絶対代入しない）。
- 高低差 elevation_diff → stamina_cost。坂が大きいほど消耗↑（中山/中京）。
- 直線長 straight_length → closer_late / stalker_late。長い直線ほど終盤に差し・追込が届く（東京）。
- コーナー曲率 corner_radius_large + スパイラル has_spiral_curve → turn_k（外を回す距離ロス）。
  急コーナーほど外差しの距離ロス大。turn_k は opt-in（既定0）なので base>0 のときだけ modulate する
  （校正で未 on の機構を幾何が勝手に起こして EV を歪めない、という保守側の選択）。
"""
from __future__ import annotations

import dataclasses
from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class CourseContext:
    """1 レース分に解決したコース幾何のスナップショット（None=未知→中立）。

    値はレース単位の静的属性。course_* 列の最初の有限値を採る（レース内で定数のため）。
    すべて Optional[float]。None のフィールドは sim_config_for_course で「効果なし(factor=1.0)」
    に落ちる＝後方互換。
    """

    width: float | None = None            # 物理レース幅 [m]（width_min/max の代表値）
    elevation_diff: float | None = None   # 最大高低差 [m]
    straight_length: float | None = None  # ゴール前直線長 [m]
    lap_length: float | None = None       # 一周距離 [m]（Phase A では未写像・Phase C 予約）
    turn_direction: float | None = None   # 0=右, 1=左（場の物理スカラーではない・Phase B 用に保持）
    corner_radius_large: float | None = None  # 緩コーナー=1 / 急=0
    has_spiral: float | None = None       # スパイラルカーブ採用=1

    @property
    def is_empty(self) -> bool:
        """幾何が 1 つも解決できなかった（全 knob が base のまま）か。"""
        return all(
            getattr(self, f.name) is None for f in dataclasses.fields(self)
        )


@dataclass(frozen=True)
class CourseEnvParams:
    """コース幾何 → SimConfig 写像の参照点(pivot)とゲイン（校正対象・物理レンジで clip）。

    参照値は「平均的な JRA コース」。当該コースの幾何が参照ちょうどなら全 factor=1.0 で base を
    厳密再現する。ゲインは相対偏差 (x−ref)/ref に掛かる無次元係数。clip で物理的に妥当な範囲へ抑える。
    """

    # 参照点（JRA 実データ準拠のおおよその代表値）
    width_ref: float = 32.0       # 物理レース幅 [m]（幅員 min/max の平均のおよそ中央）
    straight_ref: float = 360.0   # ゴール前直線 [m]
    elevation_ref: float = 2.3    # 最大高低差 [m]

    # 幅員 → 有効レース幅（course_width）スケール（他 knob と同じ「ゲイン×相対偏差」形）
    width_gain: float = 1.0       # 幅比 (width/ref−1) への感度。1.0=物理幅そのまま比例、0=幅効果 off
    width_lo: float = 0.55        # 有効幅スケールの下限（最狭コース）
    width_hi: float = 1.50        # 上限（最広コース）

    # 高低差 → stamina_cost モジュレーション
    elevation_gain: float = 0.25
    elevation_lo: float = 0.75
    elevation_hi: float = 1.40

    # 直線長 → 終盤到達（closer_late / stalker_late）モジュレーション
    straight_gain: float = 0.18
    straight_lo: float = 0.88
    straight_hi: float = 1.15
    stalker_share: float = 0.5    # 直線効果を stalker_late に薄める割合（closer=1.0 に対し）

    # コーナー曲率/スパイラル → turn_k（外の距離ロス）モジュレーション（base>0 のときのみ）
    corner_gain: float = 0.60     # 急(tight)ほど turn_k を増やす
    spiral_relief: float = 0.15   # スパイラルは急コーナーを緩和＝距離ロスを減らす
    corner_lo: float = 0.60
    corner_hi: float = 1.60


# 較正ファイル(sim_calibration.json)内で CourseEnvParams ゲインに使う接頭辞。SimConfig knob と
# 名前空間を分けるため（calibrate_sim.py が ce_<field> で探索・保存し、consumer がこれで復元する）。
COURSE_ENV_GAIN_PREFIX = "ce_"


def course_env_params_from_mapping(params: dict,
                                   prefix: str = COURSE_ENV_GAIN_PREFIX) -> "CourseEnvParams | None":
    """<prefix><field> 形式の較正値マップ → CourseEnvParams（該当キーが無ければ None）。

    calibrate_sim.py が best_params に ce_* として保存したゲインを、sim_fidelity 等の consumer が
    CourseEnvParams へ復元する共通経路（ce_ 規約の単一の出所）。未指定フィールドは既定を使う。
    """
    kw = {k[len(prefix):]: v for k, v in params.items() if k.startswith(prefix)}
    return CourseEnvParams(**kw) if kw else None


def _clip(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def _first_finite(race_df: pd.DataFrame, col: str) -> float | None:
    """course_<col> 列の最初の有限値（レース内定数）。無い/全欠損なら None。"""
    if col not in race_df.columns:
        return None
    s = pd.to_numeric(race_df[col], errors="coerce")
    s = s[s.notna()]
    if len(s) == 0:
        return None
    return float(s.iloc[0])


def course_context_from_featured(race_df: pd.DataFrame) -> CourseContext:
    """1 レース分の featured 行群 → CourseContext（course_* 幾何を解決・スキーマ寛容）。

    幅員は width_min/max の平均（片方のみならその値）。列が無い環境でも None（中立）へフォールバック。
    """
    w_min = _first_finite(race_df, "course_width_min")
    w_max = _first_finite(race_df, "course_width_max")
    width: float | None
    if w_min is not None and w_max is not None:
        width = 0.5 * (w_min + w_max)
    else:
        width = w_min if w_min is not None else w_max

    return CourseContext(
        width=width,
        elevation_diff=_first_finite(race_df, "course_elevation_diff"),
        straight_length=_first_finite(race_df, "course_straight_length"),
        lap_length=_first_finite(race_df, "course_lap_length"),
        turn_direction=_first_finite(race_df, "course_turn_direction"),
        corner_radius_large=_first_finite(race_df, "course_corner_radius_large"),
        has_spiral=_first_finite(race_df, "course_has_spiral_curve"),
    )


def sim_config_for_course(base_cfg, ctx: CourseContext,
                          params: CourseEnvParams | None = None):
    """base_cfg（校正済み SimConfig）をコース幾何 ctx で modulate した per-race cfg を返す。

    参照点(pivot)で factor=1.0＝base を厳密再現。幾何が未知(None)な knob は base のまま。
    存在するフィールドだけ置換するので SimConfig / SimConfigFixed / SimConfig2D いずれにも使える
    （欠けたフィールドは黙ってスキップ）。base_cfg 自体は変更しない（frozen 相当・新インスタンス）。
    """
    params = params or CourseEnvParams()
    if ctx.is_empty:
        return base_cfg   # 幾何なし＝完全後方互換（新インスタンスすら作らない）

    field_names = {f.name for f in dataclasses.fields(base_cfg)}
    overrides: dict = {}

    # (1) 幅員 → 有効レース幅（course_width）。物理幅を参照幅で正規化した比で base をスケール。
    #     狭いほど lane_capacity↓＝前方バンド定員が減り前列争いが激化（外枠・先行集中に不利）。
    if ctx.width is not None and "course_width" in field_names:
        rel = ctx.width / params.width_ref - 1.0
        scale = _clip(1.0 + params.width_gain * rel, params.width_lo, params.width_hi)
        overrides["course_width"] = base_cfg.course_width * scale

    # (2) 高低差 → stamina_cost。坂が大きいほど v²消費が増える（終盤失速→差し台頭を強める）。
    if ctx.elevation_diff is not None and "stamina_cost" in field_names:
        rel = (ctx.elevation_diff - params.elevation_ref) / params.elevation_ref
        factor = _clip(1.0 + params.elevation_gain * rel,
                       params.elevation_lo, params.elevation_hi)
        overrides["stamina_cost"] = base_cfg.stamina_cost * factor

    # (3) 直線長 → 終盤到達（closer_late / stalker_late）。長い直線ほど差し・追込が終盤に伸びる。
    if ctx.straight_length is not None:
        rel = (ctx.straight_length - params.straight_ref) / params.straight_ref
        f_str = _clip(1.0 + params.straight_gain * rel,
                      params.straight_lo, params.straight_hi)
        if "closer_late" in field_names:
            overrides["closer_late"] = base_cfg.closer_late * f_str
        if "stalker_late" in field_names:
            # stalker はコーナー明けの位置が前寄り＝直線効果を薄めに乗せる
            overrides["stalker_late"] = base_cfg.stalker_late * (
                1.0 + params.stalker_share * (f_str - 1.0))

    # (4) コーナー曲率/スパイラル → turn_k（外を回す距離ロス）。急コーナーほど外差しの距離ロス大。
    #     turn_k は opt-in（既定0）。校正で on（base>0）のときだけ modulate＝未 on の機構を勝手に起こさない。
    if ("turn_k" in field_names and base_cfg.turn_k
            and (ctx.corner_radius_large is not None or ctx.has_spiral is not None)):
        # tightness_centered: 急(radius_large=0)→+0.5 / 緩(=1)→−0.5 / 未知→0（中立）
        if ctx.corner_radius_large is not None:
            tight = (1.0 - ctx.corner_radius_large) - 0.5
        else:
            tight = 0.0
        spiral = ctx.has_spiral if ctx.has_spiral is not None else 0.0
        f_corner = _clip(
            1.0 + params.corner_gain * tight - params.spiral_relief * spiral,
            params.corner_lo, params.corner_hi)
        overrides["turn_k"] = base_cfg.turn_k * f_corner

    if not overrides:
        return base_cfg
    return dataclasses.replace(base_cfg, **overrides)
