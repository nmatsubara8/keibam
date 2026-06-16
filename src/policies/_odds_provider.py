"""オッズ供給の抽象化。

期待値計算に必要な「馬券のオッズ」を供給する境界。EV 馬券選定（_bet_policy）は
この抽象にのみ依存し、過去推定（Harville）／ライブ実オッズ／予測オッズ（Layer2）の
実装差し替えに影響されない（依存性逆転）。

レイヤ: policies（ドメイン）。I/O は持たず、必要なデータはコンストラクタで注入する。
"""

from __future__ import annotations

import logging
from abc import ABC
from abc import abstractmethod
from typing import Mapping
from typing import Sequence

from src.constants._bet_types import BetType
from src.policies import _harville as harville

logger = logging.getLogger(__name__)


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

    @classmethod
    def from_score_table(cls, table, umaban_col: str, odds_col: str, takeout: float = 0.2) -> "HistoricalOddsProvider":
        """race_id を index に持つテーブルから {race_id: {馬番: 単勝オッズ}} を構築する。"""
        odds_by_race: dict = {}
        for race_id, race_df in table.groupby(level=0):
            odds_by_race[race_id] = dict(
                zip(race_df[umaban_col], race_df[odds_col].astype(float), strict=False)
            )
        return cls(odds_by_race, takeout=takeout)

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


class PredictedOddsProvider(AbstractOddsProvider):
    """オッズ力学モデルの「予測確定オッズ」を供給する（Layer2 → EV 連携）。

    予測値は {(race_id, 馬番): 予測確定単勝オッズ} の lookup として DI する
    （pipeline.odds_watch.latest_final_odds_lookup の出力。policies → training の
    依存を作らないためデータだけを受け取る）。予測が無い (race_id, 馬番) は
    fallback（通常は現在オッズの HistoricalOddsProvider）へ委譲する。

    単勝は予測値そのもの、連系は予測単勝オッズから市場勝率を逆算して
    Harville 推定する（HistoricalOddsProvider と同じ換算）。
    """

    def __init__(
        self,
        predicted_final_odds: Mapping,
        fallback: AbstractOddsProvider,
        takeout: float = 0.2,
    ) -> None:
        self._predicted = dict(predicted_final_odds)
        self._fallback = fallback
        self._takeout = takeout

    def _race_predictions(self, race_id) -> dict[int, float]:
        return {
            umaban: odds
            for (rid, umaban), odds in self._predicted.items()
            if str(rid) == str(race_id) and odds and odds > 0
        }

    def get_odds(self, race_id, bet_type: str, combo: Sequence[int]) -> float:
        combo = list(combo)
        preds = self._race_predictions(race_id)
        if not preds:
            logger.debug(
                "[PredictedOdds] race=%s bet=%s combo=%s: 予測なし→現在オッズへ fallback",
                race_id, bet_type, combo,
            )
            return self._fallback.get_odds(race_id, bet_type, combo)

        if bet_type == BetType.TANSHO:
            odds = preds.get(int(combo[0]))
            if odds is None:
                return self._fallback.get_odds(race_id, bet_type, combo)
            return float(odds)

        # 連系: 予測単勝オッズ → 市場勝率 → Harville 組合せ確率 → 控除後オッズ
        implied = {umaban: 1.0 / o for umaban, o in preds.items()}
        win_probs = harville.normalize(implied)
        prob = harville.combo_probability(bet_type, win_probs, combo)
        if prob <= 0:
            return self._fallback.get_odds(race_id, bet_type, combo)
        return (1.0 - self._takeout) / prob
