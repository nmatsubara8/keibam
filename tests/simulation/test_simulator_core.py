"""Simulator.calc_returns / calc_returns_per_race の集計ロジックテスト。

test_simulator_dispatch.py が「どのメソッドに振られるか」を検証するのに対し、
本テストは「返戻金の集計・要約統計が正しく計算されるか」を検証する。
_StubTickets / _make_sim は test_simulator_dispatch.py のパターンを踏襲する。
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.simulation._simulator import Simulator


# ──────────────────────────────────────────────────────
# スタブ
# ──────────────────────────────────────────────────────


class _FixedTickets:
    """全 bet_* メソッドが固定値 (n_bets, bet_amount, return_amount) を返すスタブ。"""

    def __init__(self, n: int = 1, bet: int = 100, ret: int = 0):
        self._n, self._bet, self._ret = n, bet, ret

    def _fixed(self, *args, **kwargs):
        return self._n, self._bet, self._ret

    bet_tansho = _fixed
    bet_fukusho = _fixed
    bet_wakuren_box = _fixed
    bet_umaren_box = _fixed
    bet_umatan_box = _fixed
    bet_wide_box = _fixed
    bet_sanrenpuku_box = _fixed
    bet_sanrentan_box = _fixed


def _make_sim(n: int = 1, bet: int = 100, ret: int = 0) -> Simulator:
    """ファイル I/O なしで Simulator を生成する。"""
    sim = object.__new__(Simulator)
    sim.betting_tickets = _FixedTickets(n, bet, ret)
    return sim


# ──────────────────────────────────────────────────────
# calc_returns_per_race のテスト
# ──────────────────────────────────────────────────────


def test_calc_returns_per_race_columns():
    sim = _make_sim(n=1, bet=100, ret=0)
    df = sim.calc_returns_per_race({"R001": {"tansho": [3]}})
    assert set(df.columns) == {"n_bets", "bet_amount", "return_amount", "hit_or_not"}


def test_calc_returns_per_race_index_is_race_id():
    sim = _make_sim()
    df = sim.calc_returns_per_race({"R001": {"tansho": [3]}})
    assert "R001" in df.index


def test_calc_returns_per_race_hit():
    sim = _make_sim(n=1, bet=100, ret=200)
    df = sim.calc_returns_per_race({"R001": {"tansho": [3]}})
    assert df.loc["R001", "hit_or_not"] == 1
    assert df.loc["R001", "return_amount"] == 200


def test_calc_returns_per_race_miss():
    sim = _make_sim(n=1, bet=100, ret=0)
    df = sim.calc_returns_per_race({"R001": {"tansho": [3]}})
    assert df.loc["R001", "hit_or_not"] == 0
    assert df.loc["R001", "return_amount"] == 0


def test_calc_returns_per_race_empty_actions():
    sim = _make_sim()
    df = sim.calc_returns_per_race({})
    assert df.empty


def test_calc_returns_per_race_multiple_bets_in_race_summed():
    sim = _make_sim(n=1, bet=100, ret=150)
    df = sim.calc_returns_per_race({"R001": {"tansho": [3], "fukusho": [3]}})
    # 2 馬券種 → n_bets=2, bet_amount=200, return_amount=300
    assert df.loc["R001", "n_bets"] == 2
    assert df.loc["R001", "bet_amount"] == 200
    assert df.loc["R001", "return_amount"] == 300


def test_calc_returns_per_race_multiple_races():
    sim = _make_sim(n=1, bet=100, ret=0)
    df = sim.calc_returns_per_race({"R001": {"tansho": [1]}, "R002": {"tansho": [2]}})
    assert len(df) == 2
    assert "R001" in df.index and "R002" in df.index


# ──────────────────────────────────────────────────────
# calc_returns のテスト
# ──────────────────────────────────────────────────────


def test_calc_returns_empty_actions_returns_empty_dict():
    sim = _make_sim()
    result = sim.calc_returns({})
    assert result == {}


def test_calc_returns_includes_required_keys():
    sim = _make_sim(n=1, bet=100, ret=0)
    result = sim.calc_returns({"R001": {"tansho": [1]}})
    for key in ("n_bets", "n_races", "return_rate", "hit_rate", "sharpe_ratio", "max_drawdown"):
        assert key in result, f"キー '{key}' が見つからない: {result.keys()}"


def test_calc_returns_return_rate_formula():
    # bet=100, ret=120 → return_rate = 1.2
    sim = _make_sim(n=1, bet=100, ret=120)
    result = sim.calc_returns({"R001": {"tansho": [1]}})
    assert abs(result["return_rate"] - 1.2) < 1e-9


def test_calc_returns_zero_return_rate():
    sim = _make_sim(n=1, bet=100, ret=0)
    result = sim.calc_returns({"R001": {"tansho": [1]}})
    assert result["return_rate"] == 0.0


def test_calc_returns_hit_rate_single_hit():
    sim = _make_sim(n=1, bet=100, ret=200)
    result = sim.calc_returns({"R001": {"tansho": [1]}})
    assert result["hit_rate"] == 1.0


def test_calc_returns_hit_rate_single_miss():
    sim = _make_sim(n=1, bet=100, ret=0)
    result = sim.calc_returns({"R001": {"tansho": [1]}})
    assert result["hit_rate"] == 0.0


def test_calc_returns_n_races():
    sim = _make_sim(n=1, bet=100, ret=0)
    result = sim.calc_returns({"R001": {"tansho": [1]}, "R002": {"tansho": [2]}})
    assert result["n_races"] == 2


def test_calc_returns_profit():
    sim = _make_sim(n=1, bet=100, ret=130)
    result = sim.calc_returns({"R001": {"tansho": [1]}})
    assert result["profit"] == 30


def test_calc_returns_max_drawdown_nonnegative():
    sim = _make_sim(n=1, bet=100, ret=0)
    result = sim.calc_returns({"R001": {"tansho": [1]}, "R002": {"tansho": [2]}})
    assert result["max_drawdown"] >= 0
