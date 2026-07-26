"""ケリー配分のプール影響（自己購入のオッズ低下）opt-in 配線のテスト（芦谷/ベンター）。"""

import pytest

from src.policies._bet_candidate import BetCandidate
from src.portfolio._kelly import KellyPortfolioOptimizer
from src.portfolio._kelly import pool_capped_stake


def _cand(prob, odds, race_id="r1", stake=0.0, conf=1.0):
    return BetCandidate(
        race_id=race_id, bet_type="tansho", combo=(1,),
        probability=prob, odds=odds, expected_value=prob * odds, confidence=conf,
    )


class TestPoolCappedStake:
    def test_no_pool_returns_stake_unchanged(self):
        assert pool_capped_stake(5000.0, 0.3, 5.0, 0.0) == 5000.0

    def test_large_pool_barely_caps(self):
        # 大きなプールでは影響が小さく、ケリー枚数がそのまま通る
        s = pool_capped_stake(300.0, 0.5, 4.0, pool_total=1_000_000.0)
        assert s == pytest.approx(300.0)  # 3枚=300円

    def test_small_pool_caps_below_kelly(self):
        # 小さいプールでは最適購入枚数 < ケリー枚数 → 上限される
        big_kelly = 1_000_000.0
        capped = pool_capped_stake(big_kelly, 0.5, 4.0, pool_total=5_000.0)
        assert 0.0 < capped < big_kelly

    def test_negative_impact_ev_zeroed(self):
        # impact 後に EV<=1 になるなら賭けない
        s = pool_capped_stake(100.0, 0.2, 5.0, pool_total=50.0)
        assert s == 0.0

    def test_sub_unit_stake_zeroed(self):
        assert pool_capped_stake(50.0, 0.3, 5.0, pool_total=1_000_000.0) == 0.0


class TestAllocateWithPool:
    def test_pool_impact_off_ignores_pool(self):
        opt = KellyPortfolioOptimizer(per_bet_cap_ratio=1.0, max_daily_ratio=1.0)
        cands = [_cand(0.5, 4.0)]
        a = opt.allocate(cands, 100_000.0, pool_by_race={"r1": 100.0})
        b = opt.allocate(cands, 100_000.0)
        assert a[0].stake == b[0].stake  # pool_impact=False なら pool 無視

    def test_pool_impact_caps_stake(self):
        opt = KellyPortfolioOptimizer(per_bet_cap_ratio=1.0, max_daily_ratio=1.0, pool_impact=True)
        cands = [_cand(0.5, 4.0)]
        base = opt.allocate(cands, 100_000.0)[0].stake          # pool 未指定＝素のケリー
        capped = opt.allocate(cands, 100_000.0, pool_by_race={"r1": 3_000.0})[0].stake
        assert capped < base
        assert capped > 0.0

    def test_pool_impact_no_pool_for_race_uses_plain(self):
        opt = KellyPortfolioOptimizer(per_bet_cap_ratio=1.0, max_daily_ratio=1.0, pool_impact=True)
        cands = [_cand(0.5, 4.0, race_id="rX")]
        plain = KellyPortfolioOptimizer(per_bet_cap_ratio=1.0, max_daily_ratio=1.0)
        a = opt.allocate(cands, 100_000.0, pool_by_race={"other": 100.0})[0].stake
        b = plain.allocate(cands, 100_000.0)[0].stake
        assert a == pytest.approx(b)  # 当該レースの pool が無ければ素のケリー
