"""EV パイプラインへの opt-in 配線（補正Harville/較正/合成）のテスト。"""

import pandas as pd
import pytest

from src.constants._bet_types import BetType
from src.constants._results_cols import ResultsCols
from src.policies import _harville as H
from src.policies._bet_policy import ExpectedValueBetPolicy
from src.policies._blend import BlendWeights
from src.policies._blend import combine_logpool
from src.policies._calibration import IsotonicCalibrator
from src.policies._harville import PlaceExponents
from src.policies._odds_provider import AbstractOddsProvider


class _FixedOdds(AbstractOddsProvider):
    def __init__(self, odds=1000.0):
        self._odds = odds

    def get_odds(self, race_id, bet_type, combo):
        return self._odds


class _DictOdds(AbstractOddsProvider):
    """(bet_type, combo) → オッズの辞書スタブ。未登録は既定値。"""

    def __init__(self, table, default=1000.0):
        self._t = table
        self._d = default

    def get_odds(self, race_id, bet_type, combo):
        return self._t.get((bet_type, tuple(combo)), self._d)


def _table(race_id, umaban_probs):
    rows = [{ResultsCols.UMABAN: u, "prob": p} for u, p in umaban_probs]
    return pd.DataFrame(rows, index=[race_id] * len(rows))


def _wp(umaban_probs):
    return {u: p for u, p in umaban_probs}


class TestPlaceExponentsWiring:
    def test_trifecta_uses_corrected_harville(self):
        probs = [(1, 0.5), (2, 0.3), (3, 0.15), (4, 0.05)]
        exp = PlaceExponents(0.81, 0.65)
        policy = ExpectedValueBetPolicy(
            _FixedOdds(1e6), thresholds={BetType.SANRENTAN: 0.0}, place_exponents=exp,
        )
        cands = {c.combo: c.probability for c in policy.select(_table("r1", probs))}
        # 候補確率が「補正 Harville」と一致（素の Harville とは異なる）
        combo = (1, 2, 3)
        assert cands[combo] == pytest.approx(H.prob_trifecta_corrected(_wp(probs), 1, 2, 3, exp))
        assert cands[combo] != pytest.approx(H.prob_trifecta(_wp(probs), 1, 2, 3))

    def test_none_is_plain_harville(self):
        probs = [(1, 0.5), (2, 0.3), (3, 0.15), (4, 0.05)]
        policy = ExpectedValueBetPolicy(_FixedOdds(1e6), thresholds={BetType.SANRENTAN: 0.0})
        cands = {c.combo: c.probability for c in policy.select(_table("r1", probs))}
        assert cands[(1, 2, 3)] == pytest.approx(H.prob_trifecta(_wp(probs), 1, 2, 3))


class TestCalibratorWiring:
    def test_calibration_changes_win_prob(self):
        probs = [(1, 0.5), (2, 0.3), (3, 0.2)]
        # 本命(高raw)を持ち上げる較正写像
        cal = IsotonicCalibrator(x=(0.2, 0.5), y=(0.15, 0.8))
        policy = ExpectedValueBetPolicy(
            _FixedOdds(10.0), thresholds={BetType.TANSHO: 0.0}, win_calibrator=cal,
        )
        cands = {c.combo: c.probability for c in policy.select(_table("r1", probs))}
        # 期待値: 較正後を Σ=1 正規化した値
        raw = [0.5, 0.3, 0.2]
        c = cal.predict(raw)
        expected1 = c[0] / sum(c)
        assert cands[(1,)] == pytest.approx(expected1)
        assert cands[(1,)] != pytest.approx(0.5)  # 素の値とは異なる


class TestBlendWiring:
    def test_blend_mixes_model_and_market(self):
        probs = [(1, 0.2), (2, 0.3), (3, 0.5)]  # モデルは3番強い
        # 市場（単勝オッズ）は1番強い: 実 implied = 1/odds 正規化
        odds = {
            (BetType.TANSHO, (1,)): 2.0,
            (BetType.TANSHO, (2,)): 4.0,
            (BetType.TANSHO, (3,)): 5.0,
        }
        w = BlendWeights(alpha=1.0, beta=1.0)
        policy = ExpectedValueBetPolicy(
            _DictOdds(odds), thresholds={BetType.TANSHO: 0.0}, blend_weights=w,
        )
        cands = {c.combo: c.probability for c in policy.select(_table("r1", probs))}
        # 期待値: combine_logpool(model, public)
        public_raw = {1: 1 / 2.0, 2: 1 / 4.0, 3: 1 / 5.0}
        s = sum(public_raw.values())
        public = {u: p / s for u, p in public_raw.items()}
        expected = combine_logpool(_wp(probs), public, 1.0, 1.0)
        assert cands[(1,)] == pytest.approx(expected[1])
        # 合成後は純モデル(0.2)より市場寄りに上昇
        assert cands[(1,)] > 0.2


class TestBackwardCompat:
    def test_all_none_matches_plain(self):
        probs = [(1, 0.5), (2, 0.5)]
        base = ExpectedValueBetPolicy(_FixedOdds(4.0), thresholds={BetType.TANSHO: 1.0})
        wired = ExpectedValueBetPolicy(
            _FixedOdds(4.0), thresholds={BetType.TANSHO: 1.0},
            place_exponents=None, win_calibrator=None, blend_weights=None,
        )
        a = {c.combo: c.probability for c in base.select(_table("r1", probs))}
        b = {c.combo: c.probability for c in wired.select(_table("r1", probs))}
        assert a == b
