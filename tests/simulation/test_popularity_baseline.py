"""§8 人気順ベースライン・シミュレータのテスト。"""

import pandas as pd
import pytest

from src.constants._bet_types import BetType
from src.constants._results_cols import ResultsCols
from src.simulation._popularity_baseline import PopularityBaselineSimulator


class _StubSimulator:
    """build_actions の出力を記録し、固定の成績を返すスタブ。"""

    def __init__(self):
        self.received_actions = None

    def calc_returns(self, actions: dict) -> dict:
        self.received_actions = actions
        return {"return_rate": 0.75, "n_races": len(actions)}


def _table(rows):
    # rows: (race_id, umaban, popularity, odds)
    data = [
        {
            ResultsCols.UMABAN: u,
            ResultsCols.POPULARITY: pop,
            ResultsCols.TANSHO_ODDS: odds,
        }
        for _, u, pop, odds in rows
    ]
    return pd.DataFrame(data, index=[r for r, *_ in rows])


class TestBuildActions:
    def test_picks_top_popularity(self):
        table = _table([("r1", 1, 3, 8.0), ("r1", 2, 1, 2.0), ("r1", 3, 2, 5.0)])
        sim = _StubSimulator()
        baseline = PopularityBaselineSimulator(sim, top_n=1)
        actions = baseline.build_actions(table)
        # 1番人気 = umaban 2
        assert actions["r1"][BetType.TANSHO] == [2]

    def test_top_n_multiple(self):
        table = _table([("r1", 1, 3, 8.0), ("r1", 2, 1, 2.0), ("r1", 3, 2, 5.0)])
        sim = _StubSimulator()
        baseline = PopularityBaselineSimulator(sim, top_n=2)
        actions = baseline.build_actions(table)
        # 1,2番人気 = umaban 2, 3
        assert set(actions["r1"][BetType.TANSHO]) == {2, 3}

    def test_falls_back_to_odds_when_no_popularity(self):
        table = pd.DataFrame(
            [
                {ResultsCols.UMABAN: 1, ResultsCols.TANSHO_ODDS: 8.0},
                {ResultsCols.UMABAN: 2, ResultsCols.TANSHO_ODDS: 2.0},
            ],
            index=["r1", "r1"],
        )
        sim = _StubSimulator()
        baseline = PopularityBaselineSimulator(sim, top_n=1)
        actions = baseline.build_actions(table)
        # 最低オッズ = umaban 2
        assert actions["r1"][BetType.TANSHO] == [2]

    def test_multiple_races(self):
        table = _table(
            [("r1", 1, 1, 2.0), ("r1", 2, 2, 5.0), ("r2", 3, 2, 6.0), ("r2", 4, 1, 1.5)]
        )
        sim = _StubSimulator()
        baseline = PopularityBaselineSimulator(sim, top_n=1)
        actions = baseline.build_actions(table)
        assert actions["r1"][BetType.TANSHO] == [1]
        assert actions["r2"][BetType.TANSHO] == [4]

    def test_no_popularity_or_odds_raises(self):
        table = pd.DataFrame([{ResultsCols.UMABAN: 1}], index=["r1"])
        sim = _StubSimulator()
        baseline = PopularityBaselineSimulator(sim, top_n=1)
        with pytest.raises(ValueError):
            baseline.build_actions(table)


class TestCalcReturns:
    def test_delegates_to_simulator(self):
        table = _table([("r1", 1, 1, 2.0), ("r1", 2, 2, 5.0)])
        sim = _StubSimulator()
        baseline = PopularityBaselineSimulator(sim, top_n=1)
        result = baseline.calc_returns(table)
        assert result["return_rate"] == 0.75
        # Simulator received the built actions
        assert sim.received_actions["r1"][BetType.TANSHO] == [1]


class TestBetTypeValidation:
    def test_fukusho_allowed(self):
        sim = _StubSimulator()
        baseline = PopularityBaselineSimulator(sim, top_n=1, bet_type=BetType.FUKUSHO)
        table = _table([("r1", 1, 1, 2.0)])
        actions = baseline.build_actions(table)
        assert BetType.FUKUSHO in actions["r1"]

    def test_umaren_raises(self):
        sim = _StubSimulator()
        with pytest.raises(ValueError):
            PopularityBaselineSimulator(sim, top_n=2, bet_type=BetType.UMAREN)
