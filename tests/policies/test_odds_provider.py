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
