"""Layer2: 段階オッズ スナップショット系列から確定オッズ予測の学習データを構築する。

odds_scheduler が蓄積した `OddsSnapshot` 群（フェーズ × レース × 組合せ）を
ワイドテーブルに整形し、`LgbOddsPredictor` の学習・推論に渡せる特徴量を作る。

設計（_odds_predictor.py の段階拡張 ① に対応）:
- 目的変数: just_before フェーズのオッズ（締切直前 ≒ 確定オッズの代理）
- 説明変数: ある時点（current_phase）のオッズと、それ以前のフェーズからの変化率
  （オッズ・モメンタム。締切に向けた人気変動の方向と強さを表す）
- 純粋 pandas 実装（I/O なし）。pickle の読込は呼び出し側（train_odds_predictor）。
"""

from __future__ import annotations

import logging
from typing import Sequence

import numpy as np
import pandas as pd

from src.constants._bet_types import BetType
from src.constants._odds_phases import OddsPhase
from src.policies._arbitrage import FACTOR_CHIHO
from src.policies._arbitrage import ODDS_BASE
from src.policies._arbitrage import recover_pool_total
from src.preparing._odds_snapshot import OddsSnapshot
from src.preparing._odds_snapshot import combo_to_str

logger = logging.getLogger(__name__)

# 説明変数に使えるフェーズ（締切から遠い順）。current_phase より前のフェーズだけ使う。
_PHASE_SEQUENCE = (
    OddsPhase.PREV_DAY,
    OddsPhase.HOURS_BEFORE,
    OddsPhase.THIRTY_MIN,
    OddsPhase.T10,
    OddsPhase.T5,
)

# 目的変数のフェーズ（締切直前のオッズを確定オッズの代理とする）
TARGET_PHASE = OddsPhase.T0


def snapshots_to_phase_table(
    snapshots: Sequence[OddsSnapshot], bet_type: str = BetType.TANSHO
) -> pd.DataFrame:
    """スナップショット群を (race_id, combo) × フェーズのワイドテーブルにする（純粋関数）。

    返り値は MultiIndex (race_id, combo) を持ち、列は ``odds_<phase>``。
    combo はタプルではなく DB 表現と同じ文字列（例 ``"1"`` / ``"1-2"``）。
    同一 (race_id, combo, phase) が複数ある場合は取得時刻が最新のものを採用する。
    """
    rows = [
        {
            "race_id": s.race_id,
            "combo": combo_to_str(s.combo),
            "phase": s.phase,
            "odds": s.odds,
            "captured_at": s.captured_at,
        }
        for s in snapshots
        if s.bet_type == bet_type
    ]
    if not rows:
        return pd.DataFrame(index=pd.MultiIndex.from_tuples([], names=["race_id", "combo"]))

    df = pd.DataFrame(rows).sort_values("captured_at")
    wide = df.pivot_table(
        index=["race_id", "combo"], columns="phase", values="odds", aggfunc="last"
    )
    wide.columns = [f"odds_{c}" for c in wide.columns]
    return wide


def recover_pools_by_phase(
    snapshots: Sequence[OddsSnapshot],
    bet_type: str = BetType.SANRENTAN,
    *,
    base: float = ODDS_BASE,
    factor: float = FACTOR_CHIHO,
) -> dict[tuple[str, str], int]:
    """各 (race_id, phase) のオッズ集合から総投票数 S(≒出来高 V_t) を逆算する（芦谷 2012）。

    パリミュチュエル式 ``odds = base + (S/s)·factor`` を逆に解く。**価格系列だけの A_t では
    観測できなかった「総投票量」を公開オッズから復元**する（オッズ力学の流動性/κ に使える）。

    券種は組合せ数が多くオッズ分散が大きい**三連単/連単が逆算に好適**（既定 SANRENTAN）。
    単勝はオッズ分散が小さく逆算が不安定（芦谷 §3.2）。pool 成長率（log_ratio）は factor の
    取り方に依らず保存されるため、地方/JRA・控除率の差は成長特徴には影響しない。
    """
    wide = snapshots_to_phase_table(snapshots, bet_type)
    pools: dict[tuple[str, str], int] = {}
    if wide.empty:
        return pools
    for race_id, grp in wide.groupby(level=0):
        for col in grp.columns:  # "odds_<phase>"
            phase = col[len("odds_"):]
            odds = [o for o in grp[col].tolist() if pd.notna(o)]
            if len(odds) >= 2:  # 逆算には複数組合せが要る
                s = recover_pool_total(odds, base=base, factor=factor)
                if s:
                    pools[(str(race_id), phase)] = s
    return pools


def build_training_frame(
    snapshots: Sequence[OddsSnapshot],
    bet_type: str = BetType.TANSHO,
    current_phase: str = OddsPhase.THIRTY_MIN,
    *,
    include_pool: bool = False,
    pool_bet_type: str | None = None,
) -> tuple[pd.DataFrame, pd.Series, list[str]]:
    """学習用の (features, final_odds, feature_cols) を構築する（純粋関数）。

    - features: index は (race_id, combo)。``current_odds`` と、current_phase より
      前の各フェーズの ``log_ratio_<phase>``（= log(current / earlier)。欠損は 0 =
      「変化なし」として補完）を持つ。
    - final_odds: TARGET_PHASE（just_before）のオッズ。current と target の両方が
      観測できた行だけ残す。
    - feature_cols: `LgbOddsPredictor(feature_cols=...)` にそのまま渡せる列名リスト。
    """
    if current_phase not in _PHASE_SEQUENCE:
        raise ValueError(f"current_phase は {_PHASE_SEQUENCE} のいずれかを指定してください: {current_phase}")

    wide = snapshots_to_phase_table(snapshots, bet_type)
    current_col = f"odds_{current_phase}"
    target_col = f"odds_{TARGET_PHASE}"
    if wide.empty or current_col not in wide.columns or target_col not in wide.columns:
        empty_idx = pd.MultiIndex.from_tuples([], names=["race_id", "combo"])
        return pd.DataFrame(index=empty_idx), pd.Series(dtype=float, index=empty_idx), []

    # current と target の両方が観測できた行だけが学習に使える
    usable = wide.dropna(subset=[current_col, target_col])

    features = pd.DataFrame(index=usable.index)
    features["current_odds"] = usable[current_col].astype(float)

    feature_cols = ["current_odds"]
    earlier_phases = _PHASE_SEQUENCE[: _PHASE_SEQUENCE.index(current_phase)]
    for phase in earlier_phases:
        col = f"odds_{phase}"
        ratio_col = f"log_ratio_{phase}"
        if col in usable.columns:
            # log(現在 / 過去): 正 = オッズ上昇（人気離れ）、負 = 下降（人気集中）
            ratio = np.log(features["current_odds"] / usable[col].astype(float))
            features[ratio_col] = ratio.fillna(0.0)
        else:
            features[ratio_col] = 0.0
        feature_cols.append(ratio_col)

    # 出来高（プール）特徴: パリミュチュエル式の逆算で総投票数 V_t を復元し、
    # current の絶対量 + 過去フェーズからの成長率（資金流入の方向と強さ）を付与する。
    # これは価格系列だけの log_ratio では観測できない「総投票量」の情報（芦谷 2012）。
    if include_pool:
        src_bt = pool_bet_type or bet_type
        pools = recover_pools_by_phase(snapshots, src_bt)
        races = features.index.get_level_values("race_id")

        def _pool(phase: str) -> pd.Series:
            return pd.Series([pools.get((str(r), phase), np.nan) for r in races], index=features.index)

        cur_pool = _pool(current_phase)
        features["pool_current"] = cur_pool.astype(float)
        feature_cols.append("pool_current")
        for phase in earlier_phases:
            ratio_col = f"pool_log_ratio_{phase}"
            earlier_pool = _pool(phase)
            # log(現在プール / 過去プール): 正 = 資金流入加速（出来高モメンタム）
            ratio = np.log(cur_pool.astype(float) / earlier_pool.astype(float))
            features[ratio_col] = ratio.replace([np.inf, -np.inf], np.nan).fillna(0.0)
            feature_cols.append(ratio_col)

    final_odds = usable[target_col].astype(float)
    return features, final_odds, feature_cols


def train_odds_predictor(
    snapshots: Sequence[OddsSnapshot],
    bet_type: str = BetType.TANSHO,
    current_phase: str = OddsPhase.THIRTY_MIN,
    min_rows: int = 100,
    *,
    include_pool: bool = False,
    pool_bet_type: str | None = None,
    **lgb_params,
):
    """蓄積スナップショットから `LgbOddsPredictor` を学習して返す。

    学習可能な行数が min_rows 未満の場合は None を返す（呼び出し側は
    `IdentityOddsPredictor` にフォールバックする想定）。``include_pool=True`` で
    プール逆算（V_t）由来の出来高特徴を加える（pool_bet_type 既定は三連単＝逆算に好適）。
    """
    from src.training._odds_predictor import LgbOddsPredictor

    features, final_odds, feature_cols = build_training_frame(
        snapshots, bet_type, current_phase,
        include_pool=include_pool,
        pool_bet_type=pool_bet_type or BetType.SANRENTAN,
    )
    if len(features) < min_rows:
        logger.info(
            "train_odds_predictor: 学習データ不足 %d < %d 行（IdentityOddsPredictor を使用してください）",
            len(features), min_rows,
        )
        return None

    predictor = LgbOddsPredictor(feature_cols=feature_cols, current_odds_col="current_odds", **lgb_params)
    predictor.fit(features, final_odds)
    logger.info(
        "train_odds_predictor: bet_type=%s current_phase=%s rows=%d features=%s",
        bet_type, current_phase, len(features), feature_cols,
    )
    return predictor
