"""ケリー配分のテスト。"""

import pytest

from src.policies._bet_candidate import BetCandidate
from src.portfolio._kelly import KellyPortfolioOptimizer
from src.portfolio._kelly import kelly_fraction


def _candidate(prob, odds, confidence=1.0):
    return BetCandidate(
        race_id="r1",
        bet_type="tansho",
        combo=(1,),
        probability=prob,
        odds=odds,
        expected_value=prob * odds,
        confidence=confidence,
    )


def test_kelly_fraction_known_value():
    # p=0.5, odds=3 (b=2) -> (2*0.5 - 0.5)/2 = 0.25
    assert kelly_fraction(0.5, 3.0) == pytest.approx(0.25)


def test_kelly_fraction_no_edge_is_zero():
    # 期待値1.0以下ならベットしない
    assert kelly_fraction(0.3, 3.0) == 0.0  # EV=0.9
    assert kelly_fraction(0.5, 1.0) == 0.0  # b=0


def test_allocate_uses_fraction_and_confidence():
    opt = KellyPortfolioOptimizer(kelly_fraction_ratio=1.0, per_bet_cap_ratio=1.0, max_daily_ratio=1.0)
    [c] = opt.allocate([_candidate(0.5, 3.0, confidence=1.0)], bankroll=1000.0)
    # 1.0 * 0.25 * 1.0 * 1000 = 250
    assert c.stake == pytest.approx(250.0)


def test_confidence_scales_stake():
    opt = KellyPortfolioOptimizer(kelly_fraction_ratio=1.0, per_bet_cap_ratio=1.0, max_daily_ratio=1.0)
    [c] = opt.allocate([_candidate(0.5, 3.0, confidence=0.4)], bankroll=1000.0)
    assert c.stake == pytest.approx(100.0)


def test_per_bet_cap_respected():
    opt = KellyPortfolioOptimizer(kelly_fraction_ratio=1.0, per_bet_cap_ratio=0.05, max_daily_ratio=1.0)
    [c] = opt.allocate([_candidate(0.9, 5.0)], bankroll=1000.0)
    assert c.stake <= 0.05 * 1000.0 + 1e-9


def test_daily_budget_not_exceeded():
    opt = KellyPortfolioOptimizer(kelly_fraction_ratio=1.0, per_bet_cap_ratio=1.0, max_daily_ratio=0.3)
    cands = [_candidate(0.5, 3.0) for _ in range(10)]
    allocated = opt.allocate(cands, bankroll=1000.0)
    total = sum(c.stake for c in allocated)
    assert total <= 0.3 * 1000.0 + 1e-6


def test_no_edge_gets_zero_stake():
    opt = KellyPortfolioOptimizer()
    [c] = opt.allocate([_candidate(0.3, 3.0)], bankroll=1000.0)
    assert c.stake == 0.0
