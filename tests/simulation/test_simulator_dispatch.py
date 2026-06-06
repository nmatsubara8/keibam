"""Simulator.calc_returns_per_race のキャラクタリゼーションテスト。

リファクタリング #8（8分岐 if-elif → ディスパッチ辞書）の回帰ガード。
betting_tickets をスタブ化し、各馬券種が正しい bet_* メソッドにルーティングされ、
レース内で n_bets / bet_amount / return_amount が合算されることを固定する。
"""

from __future__ import annotations

import pandas as pd

from src.simulation._simulator import Simulator


class _StubTickets:
    """bet_* 呼び出しを記録し、決め打ちの (n_bets, bet, return) を返すスタブ。"""

    def __init__(self, returns_by_method=None):
        self.calls = []
        # メソッド名 -> (n_bets, bet_amount, return_amount)
        self._returns = returns_by_method or {}

    def _make(self, name):
        def fn(race_id, umaban, amount):
            self.calls.append((name, race_id, tuple(umaban), amount))
            return self._returns.get(name, (1, 1, 0))

        return fn

    def __getattr__(self, name):
        if name.startswith("bet_"):
            return self._make(name)
        raise AttributeError(name)


def _make_sim(stub):
    sim = object.__new__(Simulator)
    sim.betting_tickets = stub
    return sim


# action 名 -> 期待される bet_* メソッド名
_ROUTING = {
    "tansho": "bet_tansho",
    "fukusho": "bet_fukusho",
    "wakuren": "bet_wakuren_box",
    "umaren": "bet_umaren_box",
    "umatan": "bet_umatan_box",
    "wide": "bet_wide_box",
    "sanrenpuku": "bet_sanrenpuku_box",
    "sanrentan": "bet_sanrentan_box",
}


class TestRouting:
    def test_each_action_routes_to_correct_method(self):
        for action, method in _ROUTING.items():
            stub = _StubTickets()
            sim = _make_sim(stub)
            sim.calc_returns_per_race({"r1": {action: [1, 2]}})
            assert stub.calls[0][0] == method, f"{action} → {stub.calls[0][0]} (expected {method})"

    def test_passes_umaban_and_amount(self):
        stub = _StubTickets()
        sim = _make_sim(stub)
        sim.calc_returns_per_race({"r1": {"tansho": [3, 7]}})
        name, race_id, umaban, amount = stub.calls[0]
        assert race_id == "r1"
        assert umaban == (3, 7)
        assert amount == 1


class TestAggregation:
    def test_single_action_values(self):
        stub = _StubTickets({"bet_tansho": (2, 2, 50)})
        sim = _make_sim(stub)
        df = sim.calc_returns_per_race({"r1": {"tansho": [1, 2]}})
        row = df.loc["r1"]
        assert row["n_bets"] == 2
        assert row["bet_amount"] == 2
        assert row["return_amount"] == 50
        assert row["hit_or_not"] == 1

    def test_multiple_actions_summed(self):
        stub = _StubTickets({"bet_tansho": (1, 1, 0), "bet_umaren_box": (3, 3, 90)})
        sim = _make_sim(stub)
        df = sim.calc_returns_per_race({"r1": {"tansho": [1], "umaren": [1, 2, 3]}})
        row = df.loc["r1"]
        assert row["n_bets"] == 4  # 1 + 3
        assert row["bet_amount"] == 4  # 1 + 3
        assert row["return_amount"] == 90  # 0 + 90
        assert row["hit_or_not"] == 1

    def test_no_hit_when_zero_return(self):
        stub = _StubTickets({"bet_tansho": (1, 1, 0)})
        sim = _make_sim(stub)
        df = sim.calc_returns_per_race({"r1": {"tansho": [1]}})
        assert df.loc["r1"]["hit_or_not"] == 0

    def test_multiple_races(self):
        stub = _StubTickets({"bet_tansho": (1, 1, 10)})
        sim = _make_sim(stub)
        df = sim.calc_returns_per_race({"r1": {"tansho": [1]}, "r2": {"tansho": [2]}})
        assert set(df.index) == {"r1", "r2"}
        assert df.loc["r1"]["return_amount"] == 10
        assert df.loc["r2"]["return_amount"] == 10

    def test_empty_actions_empty_df(self):
        sim = _make_sim(_StubTickets())
        df = sim.calc_returns_per_race({})
        assert len(df) == 0
