"""オッズ供給の抽象化。

期待値計算に必要な「馬券のオッズ」を供給する境界。EV 馬券選定（_bet_policy）は
この抽象にのみ依存し、過去推定（Harville）／ライブ実オッズ／予測オッズ（Layer2）の
実装差し替えに影響されない（依存性逆転）。

レイヤ: policies（ドメイン）。I/O は持たず、必要なデータはコンストラクタで注入する。
"""

from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from typing import Mapping
from typing import Sequence

from src.constants._bet_types import BetType
from src.policies import _harville as harville


class AbstractOddsProvider(ABC):
    """指定レース・馬券種・組合せのオッズ（払戻倍率）を返す契約。"""

    @abstractmethod
    def get_odds(self, race_id, bet_type: str, combo: Sequence[int]) -> float:
        """combo は馬番のシーケンス。bet_type に応じた払戻倍率を返す。"""
        raise NotImplementedError


class HistoricalOddsProvider(AbstractOddsProvider):
    """過去レース向けのオッズ供給。

    - 単勝: 確定単勝オッズ（実値）をそのまま返す。
    - 連系: 単勝オッズから市場勝率を逆算し、Harville で組合せ確率を推定して
      推定オッズ ≈ (1 - takeout) / P(combo) を返す（過去の連オッズは遡及取得不可のため）。

    Parameters
    ----------
    tansho_odds_by_race : {race_id: {馬番: 単勝オッズ}}
    takeout : 控除率（連系の推定オッズに反映）。
    """

    def __init__(self, tansho_odds_by_race: Mapping, takeout: float = 0.2) -> None:
        self._tansho_odds_by_race = tansho_odds_by_race
        self._takeout = takeout

    def _market_win_probs(self, race_id) -> dict[int, float]:
        """単勝オッズの逆数を控除率で正規化した市場勝率。"""
        odds_map = self._tansho_odds_by_race[race_id]
        implied = {umaban: 1.0 / odds for umaban, odds in odds_map.items() if odds and odds > 0}
        return harville.normalize(implied)

    def get_odds(self, race_id, bet_type: str, combo: Sequence[int]) -> float:
        combo = list(combo)
        if bet_type == BetType.TANSHO:
            return float(self._tansho_odds_by_race[race_id][combo[0]])

        # 連系は市場勝率から Harville で組合せ確率を推定し、控除後オッズに換算する。
        win_probs = self._market_win_probs(race_id)
        prob = harville.combo_probability(bet_type, win_probs, combo)
        if prob <= 0:
            return 0.0
        return (1.0 - self._takeout) / prob
