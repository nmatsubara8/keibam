"""_place_prob の共有確率プリミティブ（normalize / implied_from_odds）のテスト。"""

import pytest

from src.preprocessing._place_prob import implied_from_odds
from src.preprocessing._place_prob import normalize


class TestImpliedFromOdds:
    def test_inverse_unnormalized(self):
        out = implied_from_odds({1: 2.0, 2: 4.0})
        assert out == {1: 0.5, 2: 0.25}

    def test_normalized_sums_to_one(self):
        out = implied_from_odds({1: 2.0, 2: 4.0, 3: 4.0}, normalized=True)
        assert sum(out.values()) == pytest.approx(1.0)
        # 1/2 : 1/4 : 1/4 → 0.5 : 0.25 : 0.25
        assert out[1] == pytest.approx(0.5)

    def test_nonpositive_and_none_excluded(self):
        out = implied_from_odds({1: 2.0, 2: 0.0, 3: -1.0, 4: None})
        assert set(out) == {1}

    def test_empty_returns_empty(self):
        assert implied_from_odds({}) == {}
        assert implied_from_odds({1: 0.0}, normalized=True) == {}

    def test_keys_preserved(self):
        out = implied_from_odds({7: 5.0, 12: 10.0})
        assert set(out) == {7, 12}

    def test_matches_manual_normalize(self):
        odds = {1: 3.0, 2: 6.0, 5: 2.0}
        manual = normalize({k: 1.0 / v for k, v in odds.items()})
        assert implied_from_odds(odds, normalized=True) == pytest.approx(manual)
