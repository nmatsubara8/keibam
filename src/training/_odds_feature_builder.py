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


def build_training_frame(
    snapshots: Sequence[OddsSnapshot],
    bet_type: str = BetType.TANSHO,
    current_phase: str = OddsPhase.THIRTY_MIN,
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

    final_odds = usable[target_col].astype(float)
    return features, final_odds, feature_cols


def train_odds_predictor(
    snapshots: Sequence[OddsSnapshot],
    bet_type: str = BetType.TANSHO,
    current_phase: str = OddsPhase.THIRTY_MIN,
    min_rows: int = 100,
    **lgb_params,
):
    """蓄積スナップショットから `LgbOddsPredictor` を学習して返す。

    学習可能な行数が min_rows 未満の場合は None を返す（呼び出し側は
    `IdentityOddsPredictor` にフォールバックする想定）。
    """
    from src.training._odds_predictor import LgbOddsPredictor

    features, final_odds, feature_cols = build_training_frame(snapshots, bet_type, current_phase)
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
