"""HistoricalOddsProvider のテスト。"""

import pytest

from src.constants._bet_types import BetType
from src.policies._odds_provider import HistoricalOddsProvider


@pytest.fixture
def provider():
    tansho_odds = {
        "race1": {1: 2.0, 2: 4.0, 3: 8.0, 4: 16.0},
    }
    return HistoricalOddsProvider(tansho_odds, takeout=0.2)


def test_tansho_returns_actual_odds(provider):
    assert provider.get_odds("race1", BetType.TANSHO, [1]) == 2.0
    assert provider.get_odds("race1", BetType.TANSHO, [3]) == 8.0


def test_combo_odds_are_positive(provider):
    for bet_type, combo in [
        (BetType.UMAREN, [1, 2]),
        (BetType.UMATAN, [1, 2]),
        (BetType.SANRENPUKU, [1, 2, 3]),
        (BetType.SANRENTAN, [1, 2, 3]),
        (BetType.FUKUSHO, [1]),
    ]:
        odds = provider.get_odds("race1", bet_type, combo)
        assert odds > 0


def test_rarer_combo_has_higher_odds(provider):
    # 人気薄同士の馬連の方が、人気同士より推定オッズが高い
    odds_favorites = provider.get_odds("race1", BetType.UMAREN, [1, 2])
    odds_longshots = provider.get_odds("race1", BetType.UMAREN, [3, 4])
    assert odds_longshots > odds_favorites


def test_umatan_more_than_umaren(provider):
    # 馬単（順序あり）は馬連（順不同）より的中しにくく、オッズが高い
    assert provider.get_odds("race1", BetType.UMATAN, [1, 2]) > provider.get_odds("race1", BetType.UMAREN, [1, 2])


class TestPerBetTypeTakeout:
    """takeout を券種別 Mapping で渡すと、券種ごとに控除率が反映される。"""

    def _provider(self, takeout):
        tansho = {"r1": {1: 2.0, 2: 4.0, 3: 8.0, 4: 16.0}}
        return HistoricalOddsProvider(tansho, takeout=takeout)

    def test_mapping_applies_per_type(self):
        # 三連単の控除率だけ 0.25 に上げると、同 combo の推定オッズが下がる
        base = self._provider(0.2).get_odds("r1", BetType.SANRENTAN, [1, 2, 3])
        higher_takeout = self._provider({BetType.SANRENTAN: 0.25}).get_odds(
            "r1", BetType.SANRENTAN, [1, 2, 3]
        )
        # (1-0.25)/P < (1-0.2)/P
        assert higher_takeout < base
        assert higher_takeout / base == pytest.approx(0.75 / 0.8, rel=1e-6)

    def test_mapping_missing_type_uses_default(self):
        # Mapping に無い券種は default_takeout=0.2 にフォールバック
        p = HistoricalOddsProvider(
            {"r1": {1: 2.0, 2: 4.0}},
            takeout={BetType.SANRENTAN: 0.25},
            default_takeout=0.2,
        )
        odds = p.get_odds("r1", BetType.UMAREN, [1, 2])
        ref = HistoricalOddsProvider({"r1": {1: 2.0, 2: 4.0}}, takeout=0.2).get_odds(
            "r1", BetType.UMAREN, [1, 2]
        )
        assert odds == pytest.approx(ref)

    def test_scalar_takeout_still_works(self):
        p = self._provider(0.25)
        odds = p.get_odds("r1", BetType.UMAREN, [1, 2])
        assert odds > 0


class TestPredictedOddsProvider:
    """オッズ力学モデルの予測確定オッズを EV 計算へ供給するプロバイダ。"""

    def _fallback(self):
        from src.policies._odds_provider import HistoricalOddsProvider

        return HistoricalOddsProvider({"r1": {1: 3.0, 2: 6.0}, "r2": {1: 10.0}})

    def test_tansho_uses_prediction(self):
        from src.policies._odds_provider import PredictedOddsProvider

        provider = PredictedOddsProvider({("r1", 1): 2.4}, fallback=self._fallback())
        assert provider.get_odds("r1", BetType.TANSHO, [1]) == 2.4

    def test_missing_horse_falls_back(self):
        from src.policies._odds_provider import PredictedOddsProvider

        provider = PredictedOddsProvider({("r1", 1): 2.4}, fallback=self._fallback())
        assert provider.get_odds("r1", BetType.TANSHO, [2]) == 6.0

    def test_missing_race_falls_back(self):
        from src.policies._odds_provider import PredictedOddsProvider

        provider = PredictedOddsProvider({("r1", 1): 2.4}, fallback=self._fallback())
        assert provider.get_odds("r2", BetType.TANSHO, [1]) == 10.0

    def test_combination_uses_harville_on_predictions(self):
        from src.policies._odds_provider import PredictedOddsProvider

        provider = PredictedOddsProvider(
            {("r1", 1): 2.0, ("r1", 2): 4.0, ("r1", 3): 8.0},
            fallback=self._fallback(),
        )
        odds = provider.get_odds("r1", BetType.UMAREN, [1, 2])
        assert odds > 0
