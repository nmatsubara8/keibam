"""パリミュチュエル プール逆算・裁定判定（src.policies._arbitrage）のテスト。"""

import pytest

from src.policies import _arbitrage as A


class TestOddsFormula:
    def test_odds_round_trip(self):
        # S=54252, s=8 → オッズ → §3.2 の例（163 のオッズ 5004.8 付近）
        o = A.odds_of(54252, 8)
        assert o == pytest.approx(5004.8, abs=0.1)

    def test_truncation_to_one_decimal(self):
        # base + (S/s)*factor を小数第2位切り捨て
        o = A.odds_of(1000, 7, base=0.1, factor=0.738)
        raw = 0.1 + (1000 / 7) * 0.738
        assert o == int(raw * 10) / 10.0

    def test_cost_per_unit_payout(self):
        assert A.cost_per_unit_payout(32.0) == pytest.approx(1 / 32.0)
        assert A.cost_per_unit_payout(0) == float("inf")


class TestPoolRecovery:
    def test_recovers_known_pool(self):
        # 既知 S と枚数から（切り捨て前の）オッズを作り、S を逆算できる。
        # 実オッズは切り捨てがあるため近似だが、アルゴリズムの正しさは厳密オッズで検証する。
        S = 4288
        counts = [3, 5, 8, 13, 21, 50, 100, 240]
        odds = [A.odds_of(S, s, truncate=False) for s in counts]
        recovered = A.recover_pool_total(odds)
        assert recovered == S

    def test_count_from_odds_inverse(self):
        S, s = 9372, 26
        o = A.odds_of(S, s)
        assert A.count_from_odds(o, S) == pytest.approx(s, abs=0.5)

    def test_empty_returns_none(self):
        assert A.recover_pool_total([]) is None


class TestOddsImpact:
    def test_buying_lowers_odds(self):
        S, s = 1776, 50  # 薄いプールの単勝
        o0 = A.odds_of(S, s)
        o1 = A.odds_after_purchase(o0, S, s, added=205)
        assert o1 < o0  # 自己購入でオッズ低下


class TestArbitrage:
    def test_lock_exists_when_indicator_below_one(self):
        # 過剰ラウンド（Σ 1/odds < 1）= 裁定あり
        win = {1: 4.0, 2: 4.0, 3: 4.0}  # Σ 1/4 *3 = 0.75 < 1
        ind = A.arbitrage_indicator([1, 2, 3], win)
        assert ind == pytest.approx(0.75)
        assert A.has_arbitrage(ind) is True

    def test_no_arbitrage_when_indicator_at_least_one(self):
        # 控除込みの通常市場（Σ 1/odds > 1）
        win = {1: 2.0, 2: 3.0, 3: 5.0}  # 0.5+0.333+0.2 = 1.033 > 1
        ind = A.arbitrage_indicator([1, 2, 3], win)
        assert ind > 1.0
        assert A.has_arbitrage(ind) is False

    def test_min_win_cost_picks_cheapest_synthesis(self):
        # 単勝が割高、連単総流しが割安 → 連単側が選ばれる
        win = {1: 2.0, 2: 5.0, 3: 5.0}
        # 1 が1着のとき: 連単 1→2, 1→3。各 odds 10 → 合成費用 0.1+0.1=0.2 < 単勝 0.5
        exacta = {(1, 2): 10.0, (1, 3): 10.0}
        c = A.min_win_cost(1, win, exacta_odds=exacta, horses=[1, 2, 3])
        assert c == pytest.approx(0.2)

    def test_min_win_cost_skips_incomplete_synthesis(self):
        # 連単に欠損（1→3 が無い）→ 連単合成は不可、単勝にフォールバック
        win = {1: 2.0, 2: 5.0, 3: 5.0}
        exacta = {(1, 2): 10.0}  # (1,3) 欠損
        c = A.min_win_cost(1, win, exacta_odds=exacta, horses=[1, 2, 3])
        assert c == pytest.approx(0.5)  # 単勝 1/2.0
