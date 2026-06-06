"""ExpectedValueBetPolicy のテスト。"""

import pandas as pd
import pytest

from src.constants._bet_thresholds import RiskLimits
from src.constants._bet_types import BetType
from src.constants._results_cols import ResultsCols
from src.policies._bet_policy import ExpectedValueBetPolicy
from src.policies._odds_provider import AbstractOddsProvider


class _FixedOddsProvider(AbstractOddsProvider):
    """全馬券に一定オッズを返すスタブ（選定ロジックの検証用）。"""

    def __init__(self, odds: float) -> None:
        self._odds = odds

    def get_odds(self, race_id, bet_type, combo) -> float:
        return self._odds


def _prob_table(race_id, umaban_probs):
    rows = [{ResultsCols.UMABAN: u, "prob": p} for u, p in umaban_probs]
    return pd.DataFrame(rows, index=[race_id] * len(rows))


def test_threshold_filters_low_ev():
    # 単勝: prob=0.5, odds=1.0 -> EV=0.5 < 閾値1.0 なので選ばれない
    table = _prob_table("r1", [(1, 0.5), (2, 0.5)])
    policy = ExpectedValueBetPolicy(_FixedOddsProvider(1.0), thresholds={BetType.TANSHO: 1.0})
    assert policy.select(table) == []


def test_high_ev_selected():
    # odds=4.0, prob(tansho,1)=0.5 -> EV=2.0 > 1.0
    table = _prob_table("r1", [(1, 0.5), (2, 0.5)])
    policy = ExpectedValueBetPolicy(_FixedOddsProvider(4.0), thresholds={BetType.TANSHO: 1.0})
    selected = policy.select(table)
    assert len(selected) == 2
    assert all(c.expected_value > 1.0 for c in selected)
    assert all(c.expected_value == pytest.approx(c.probability * c.odds) for c in selected)


def test_max_tickets_cap():
    # 10頭・馬連、全EVが閾値超 -> 組合せ45通りだが上限でキャップ
    table = _prob_table("r1", [(i, 0.1) for i in range(1, 11)])
    limits = RiskLimits(MAX_TICKETS_PER_RACE=5)
    policy = ExpectedValueBetPolicy(
        _FixedOddsProvider(1000.0), thresholds={BetType.UMAREN: 1.0}, risk_limits=limits
    )
    selected = policy.select(table)
    assert len(selected) == 5


def test_min_win_prob_filter():
    # MIN_WIN_PROB 未満の馬は候補から除外され、組合せに現れない
    table = _prob_table("r1", [(1, 0.5), (2, 0.49), (3, 0.001)])
    limits = RiskLimits(MIN_WIN_PROB=0.01)
    policy = ExpectedValueBetPolicy(
        _FixedOddsProvider(100.0), thresholds={BetType.TANSHO: 1.0}, risk_limits=limits
    )
    selected = policy.select(table)
    umaban_used = {c.combo[0] for c in selected}
    assert 3 not in umaban_used


def test_multiple_races():
    table = pd.concat(
        [_prob_table("r1", [(1, 0.6), (2, 0.4)]), _prob_table("r2", [(1, 0.7), (2, 0.3)])]
    )
    policy = ExpectedValueBetPolicy(_FixedOddsProvider(4.0), thresholds={BetType.TANSHO: 1.0})
    selected = policy.select(table)
    race_ids = {c.race_id for c in selected}
    assert race_ids == {"r1", "r2"}
