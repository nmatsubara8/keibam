"""Harville モデルの確率整合性テスト。"""

from itertools import combinations
from itertools import permutations

import pytest

from src.policies import _harville as harville


@pytest.fixture
def win_probs():
    # 総和は 1 でなくてよい（内部で正規化される）
    return {1: 0.5, 2: 0.3, 3: 0.15, 5: 0.05}


def test_normalize_sums_to_one(win_probs):
    p = harville.normalize(win_probs)
    assert pytest.approx(sum(p.values()), abs=1e-12) == 1.0


def test_exacta_all_orders_sum_to_one(win_probs):
    horses = list(win_probs)
    total = sum(harville.prob_exacta(win_probs, i, j) for i, j in permutations(horses, 2))
    assert pytest.approx(total, abs=1e-9) == 1.0


def test_trifecta_all_orders_sum_to_one(win_probs):
    horses = list(win_probs)
    total = sum(harville.prob_trifecta(win_probs, i, j, k) for i, j, k in permutations(horses, 3))
    assert pytest.approx(total, abs=1e-9) == 1.0


def test_trio_all_combos_sum_to_one(win_probs):
    horses = list(win_probs)
    total = sum(harville.prob_trio(win_probs, *combo) for combo in combinations(horses, 3))
    assert pytest.approx(total, abs=1e-9) == 1.0


def test_quinella_equals_two_exactas(win_probs):
    assert harville.prob_quinella(win_probs, 1, 2) == pytest.approx(
        harville.prob_exacta(win_probs, 1, 2) + harville.prob_exacta(win_probs, 2, 1)
    )


def test_two_horse_quinella_is_certain():
    # 2頭立てなら必ず両馬が1・2着になる
    assert harville.prob_quinella({1: 0.6, 2: 0.4}, 1, 2) == pytest.approx(1.0)


def test_wide_greater_than_quinella(win_probs):
    # ワイド（共に3着以内）は馬連（共に2着以内）より緩いので確率が大きい
    for a, b in combinations(win_probs, 2):
        assert harville.prob_wide(win_probs, a, b) >= harville.prob_quinella(win_probs, a, b)


def test_wide_in_three_horse_field_is_certain():
    # 3頭立てなら任意の2頭は必ず共に3着以内
    probs = {1: 0.6, 2: 0.3, 3: 0.1}
    for a, b in combinations(probs, 2):
        assert harville.prob_wide(probs, a, b) == pytest.approx(1.0)


def test_wide_sum_equals_three_pairs(win_probs):
    # 全ペアのワイド確率の総和 = 期待される「top3 内ペア数」= C(3,2) = 3
    total = sum(harville.prob_wide(win_probs, a, b) for a, b in combinations(win_probs, 2))
    assert pytest.approx(total, abs=1e-9) == 3.0


def test_place_in_small_field_is_certain():
    # 3頭立てで複勝3着以内は確実
    probs = {1: 0.6, 2: 0.3, 3: 0.1}
    for horse in probs:
        assert harville.prob_place(probs, horse, n_places=3) == pytest.approx(1.0)


def test_place_probabilities_sum_to_n_places(win_probs):
    # 各馬が top-k に入る確率の総和は k になる（席が k 個）
    n_places = 3
    total = sum(harville.prob_place(win_probs, horse, n_places) for horse in win_probs)
    assert pytest.approx(total, abs=1e-9) == n_places


def test_zero_total_raises():
    with pytest.raises(ValueError):
        harville.normalize({1: 0.0, 2: 0.0})
