"""成績指標 summarize_returns のテスト。"""

import numpy as np
import pandas as pd
import pytest

from src.simulation._metrics import max_drawdown
from src.simulation._metrics import summarize_returns


def _returns_df(rows):
    # rows: list of (race_id, n_bets, bet_amount, return_amount)
    df = pd.DataFrame(
        [{"n_bets": n, "bet_amount": b, "return_amount": r, "hit_or_not": 1 if r > 0 else 0} for _, n, b, r in rows],
        index=[rid for rid, *_ in rows],
    )
    return df


def test_empty_returns_empty():
    assert summarize_returns(pd.DataFrame()) == {}


def test_basic_metrics():
    df = _returns_df([("r1", 1, 100, 0), ("r2", 1, 100, 300)])
    s = summarize_returns(df)
    assert s["n_races"] == 2
    assert s["n_hits"] == 1
    assert s["total_bet_amount"] == 200
    assert s["return_rate"] == pytest.approx(300 / 200)
    assert s["hit_rate"] == pytest.approx(0.5)
    assert s["profit"] == pytest.approx(100)


def test_zero_bet_safe():
    df = _returns_df([("r1", 0, 0, 0)])
    s = summarize_returns(df)
    assert s["return_rate"] == 0.0
    assert s["sharpe_ratio"] == 0.0
    assert s["std"] == 0.0


def test_max_drawdown_simple():
    # 損益系列: +100, -200, +50 -> 累積 100, -100, -50; ピーク100からの最大下落=200
    profit = pd.Series([100, -200, 50])
    assert max_drawdown(profit) == pytest.approx(200)


def test_sharpe_sign_matches_profitability():
    win = _returns_df([("r1", 1, 100, 300), ("r2", 1, 100, 250)])
    s = summarize_returns(win)
    assert s["return_rate"] > 1.0
    assert s["sharpe_ratio"] > 0.0
