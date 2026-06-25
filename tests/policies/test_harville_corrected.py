"""Benter (1994) べき乗補正 Harville（三連単/三連複/馬単/馬連）のテスト。"""

import math

import pytest

from src.constants._bet_types import BetType
from src.policies import _harville as H
from src.policies._harville import PlaceExponents


def _wp():
    # 4頭・人気差あり（補正の効きが見える分布）
    return {1: 0.5, 2: 0.3, 3: 0.15, 4: 0.05}


class TestPlaceAdjusted:
    def test_identity_when_exp_1(self):
        p = H.place_adjusted(_wp(), 1.0)
        assert p == pytest.approx(H.normalize(_wp()))

    def test_normalized_sum_one(self):
        p = H.place_adjusted(_wp(), 0.65)
        assert sum(p.values()) == pytest.approx(1.0)

    def test_flattens_favorite_when_exp_lt_1(self):
        # γ<1 で人気馬(1)の比重が下がり、人気薄(4)が上がる
        base = H.normalize(_wp())
        adj = H.place_adjusted(_wp(), 0.5)
        assert adj[1] < base[1]
        assert adj[4] > base[4]

    def test_zero_prob_stays_zero(self):
        p = H.place_adjusted({1: 0.7, 2: 0.3, 3: 0.0}, 0.5)
        assert p[3] == 0.0


class TestBackwardCompat:
    def test_trifecta_matches_plain_at_exp_1(self):
        exp = PlaceExponents(1.0, 1.0)
        for f, s, t in [(1, 2, 3), (2, 1, 4), (3, 4, 1)]:
            assert H.prob_trifecta_corrected(_wp(), f, s, t, exp) == pytest.approx(
                H.prob_trifecta(_wp(), f, s, t)
            )

    def test_trio_matches_plain_at_exp_1(self):
        exp = PlaceExponents(1.0, 1.0)
        assert H.prob_trio_corrected(_wp(), 1, 2, 3, exp) == pytest.approx(
            H.prob_trio(_wp(), 1, 2, 3)
        )

    def test_exacta_quinella_match_plain_at_exp_1(self):
        exp = PlaceExponents(1.0, 1.0)
        assert H.prob_exacta_corrected(_wp(), 1, 2, exp) == pytest.approx(H.prob_exacta(_wp(), 1, 2))
        assert H.prob_quinella_corrected(_wp(), 1, 2, exp) == pytest.approx(
            H.prob_quinella(_wp(), 1, 2)
        )


class TestCorrectionDirection:
    def test_correction_changes_result(self):
        # γ,δ≠1 で素の Harville と異なる確率になる（補正が効いている）。
        plain = H.prob_trifecta(_wp(), 4, 3, 1)
        corrected = H.prob_trifecta_corrected(_wp(), 4, 3, 1, PlaceExponents(0.81, 0.65))
        assert corrected != pytest.approx(plain)

    def test_trifecta_distribution_sums_to_one(self):
        # 補正後も全順列の確率は 1 に正規化される（proper distribution）。
        from itertools import permutations

        exp = PlaceExponents(0.81, 0.65)
        total = sum(H.prob_trifecta_corrected(_wp(), *o, exp) for o in permutations((1, 2, 3, 4), 3))
        assert total == pytest.approx(1.0)

    def test_trio_is_sum_of_permutations(self):
        exp = PlaceExponents(0.81, 0.65)
        from itertools import permutations

        expect = sum(H.prob_trifecta_corrected(_wp(), *o, exp) for o in permutations((1, 2, 3)))
        assert H.prob_trio_corrected(_wp(), 1, 2, 3, exp) == pytest.approx(expect)


class TestComboDispatch:
    def test_exponents_routed_for_ordered_bets(self):
        exp = PlaceExponents(0.81, 0.65)
        # 三連単/三連複は exponents 指定で補正版に分岐
        assert H.combo_probability(BetType.SANRENTAN, _wp(), [4, 3, 1], exp) == pytest.approx(
            H.prob_trifecta_corrected(_wp(), 4, 3, 1, exp)
        )
        assert H.combo_probability(BetType.SANRENPUKU, _wp(), [1, 2, 3], exp) == pytest.approx(
            H.prob_trio_corrected(_wp(), 1, 2, 3, exp)
        )

    def test_no_exponents_uses_plain(self):
        assert H.combo_probability(BetType.SANRENTAN, _wp(), [1, 2, 3]) == pytest.approx(
            H.prob_trifecta(_wp(), 1, 2, 3)
        )

    def test_tansho_fukusho_wide_unaffected(self):
        exp = PlaceExponents(0.5, 0.4)
        # 補正は順序券種のみ。単勝は不変
        assert H.combo_probability(BetType.TANSHO, _wp(), [1], exp) == pytest.approx(
            H.normalize(_wp())[1]
        )


class TestBenterDefaults:
    def test_benter_hk_reference(self):
        assert PlaceExponents.BENTER_HK.gamma == 0.81
        assert PlaceExponents.BENTER_HK.delta == 0.65

    def test_default_is_identity(self):
        assert PlaceExponents() == PlaceExponents(1.0, 1.0)


class TestFit:
    def test_recovers_reasonable_exponents(self):
        # 既知 (γ,δ)=(0.7,0.5) の補正モデルから各レースの最尤着順を観測として与え、
        # fit が正の有限値を返すことを確認（厳密回復でなく配線・健全性のガード）。
        true_exp = PlaceExponents(0.7, 0.5)
        wp = _wp()
        races = []
        # 代表的な着順をいくつか（人気順・一部荒れ）
        for order in [(1, 2, 3), (1, 3, 2), (2, 1, 3), (1, 2, 4), (3, 1, 2)]:
            races.append((wp, order))
        fitted = H.fit_place_exponents(races, init=(true_exp.gamma, true_exp.delta))
        assert math.isfinite(fitted.gamma) and fitted.gamma > 0
        assert math.isfinite(fitted.delta) and fitted.delta > 0

    def test_empty_returns_init(self):
        assert H.fit_place_exponents([], init=(0.8, 0.6)) == PlaceExponents(0.8, 0.6)


# ──────────────────────────────────────────
# (γ,δ) MLE 較正の recovery と永続化
# ──────────────────────────────────────────

class TestFitRecoveryAndPersist:
    def _weighted_races(self, wp, true_exp, scale=4000):
        """既知 (γ,δ) の真の確率に比例した頻度で着順を並べた決定論的レース群。

        各順列を round(prob*scale) 回ずつ与えると、MLE は母集団 (γ,δ) を回復する。
        """
        from itertools import permutations
        races = []
        for order in permutations(wp.keys(), 3):
            p = H.prob_trifecta_corrected(wp, *order, true_exp)
            n = round(p * scale)
            races.extend([(wp, order)] * n)
        return races

    def test_mle_recovers_known_exponents(self):
        wp = {1: 0.45, 2: 0.25, 3: 0.18, 4: 0.12}
        true_exp = PlaceExponents(0.75, 0.55)
        races = self._weighted_races(wp, true_exp)
        fitted = H.fit_place_exponents(races, init=(1.0, 1.0))
        # 母集団 MLE なので真値近傍に回復する（離散丸めぶんの許容）
        assert fitted.gamma == pytest.approx(true_exp.gamma, abs=0.12)
        assert fitted.delta == pytest.approx(true_exp.delta, abs=0.12)

    def test_save_load_round_trip(self, tmp_path):
        path = str(tmp_path / "place_exponents.json")
        exp = PlaceExponents(0.81, 0.65)
        H.save_place_exponents(exp, path)
        loaded = H.load_place_exponents(path)
        assert loaded == exp

    def test_load_missing_returns_none(self, tmp_path):
        assert H.load_place_exponents(str(tmp_path / "nope.json")) is None
