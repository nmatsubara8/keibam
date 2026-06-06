"""フラクショナル・ケリー基準によるポートフォリオ（掛け金）配分。

各馬券候補の (的中確率, オッズ) からケリー比率を算出し、確信度でスケールした上で、
1レース・1馬券・1日総予算の制約下に配分する。入力 BetCandidate は不変なので、
stake を設定した新インスタンスを返す（副作用なし）。

レイヤ: portfolio（ドメイン）。I/O は持たない。
"""

from __future__ import annotations

import dataclasses
from abc import ABC
from abc import abstractmethod

from src.policies._bet_candidate import BetCandidate


def kelly_fraction(probability: float, odds: float) -> float:
    """単一馬券のケリー比率 f* = (b*p - q) / b （b = odds-1, q = 1-p）。

    エッジが無い（期待値<=1）場合は 0 を返す（賭けない）。
    """
    b = odds - 1.0
    if b <= 0:
        return 0.0
    p = probability
    q = 1.0 - p
    f = (b * p - q) / b
    return max(0.0, f)


class AbstractPortfolioOptimizer(ABC):
    @abstractmethod
    def allocate(self, candidates: list, bankroll: float) -> list:
        """候補に stake を設定した BetCandidate のリストを返す。"""
        raise NotImplementedError


class KellyPortfolioOptimizer(AbstractPortfolioOptimizer):
    """確信度スケール付きフラクショナル・ケリー。

    Parameters
    ----------
    kelly_fraction_ratio : ケリー比率に掛ける係数（0<r<=1。1未満で保守化）。
    per_bet_cap_ratio : 1馬券あたりの上限（bankroll 比）。
    max_daily_ratio : 1日の総投資上限（bankroll 比）。超過時は全体を比例縮小。
    """

    def __init__(
        self,
        kelly_fraction_ratio: float = 0.5,
        per_bet_cap_ratio: float = 0.05,
        max_daily_ratio: float = 1.0,
    ) -> None:
        self._kelly_ratio = kelly_fraction_ratio
        self._per_bet_cap_ratio = per_bet_cap_ratio
        self._max_daily_ratio = max_daily_ratio

    def allocate(self, candidates: list, bankroll: float) -> list:
        per_bet_cap = self._per_bet_cap_ratio * bankroll
        stakes = []
        for c in candidates:
            f = kelly_fraction(c.probability, c.odds)
            fraction = self._kelly_ratio * f * c.confidence
            stake = min(bankroll * fraction, per_bet_cap)
            stakes.append(max(0.0, stake))

        # 1日総予算の制約: 超過すれば全体を比例縮小（相対配分を維持）。
        total = sum(stakes)
        budget = self._max_daily_ratio * bankroll
        if total > budget and total > 0:
            scale = budget / total
            stakes = [s * scale for s in stakes]

        return [dataclasses.replace(c, stake=s) for c, s in zip(candidates, stakes)]
