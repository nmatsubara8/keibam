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


def pool_capped_stake(
    stake: float,
    probability: float,
    odds: float,
    pool_total: float,
    *,
    ticket_unit: float = 100.0,
    base: float = 0.1,
    factor: float = 0.788,
) -> float:
    """ケリー stake をパリミュチュエルのプール影響で上限する（芦谷/ベンター）。

    自己購入は確定オッズを下げる（パリミュチュエル）。`_arbitrage.max_profit_bet` が与える
    期待利益最大の購入枚数 k* を上限に、ケリー由来の枚数を抑える。さらに低下後オッズで
    impact-EV<=1 になる買い目は 0（賭けない）。pool_total は復元プール（≒出来高 V_t）。

    base/factor は券種の控除係数（既定 JRA 単勝）。pool_total<=0 なら影響を無視し stake を返す。
    """
    if pool_total <= 0:
        return stake
    from src.policies._arbitrage import effective_odds, max_profit_bet

    k_kelly = int(stake // ticket_unit)
    if k_kelly < 1:
        return 0.0
    k_opt, _ = max_profit_bet(probability, odds, pool_total, base=base, factor=factor)
    k = min(k_kelly, k_opt)
    if k < 1:
        return 0.0
    # 低下後オッズで EV を再確認（impact 込みで負なら賭けない）
    eo = effective_odds(odds, pool_total, k, base=base, factor=factor)
    if probability * eo <= 1.0:
        return 0.0
    return k * ticket_unit


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
        pool_impact: bool = False,
    ) -> None:
        self._kelly_ratio = kelly_fraction_ratio
        self._per_bet_cap_ratio = per_bet_cap_ratio
        self._max_daily_ratio = max_daily_ratio
        # プール影響（自己購入のオッズ低下）でケリー stake を上限する（opt-in・芦谷/ベンター）。
        # True かつ allocate に pool_by_race を渡したときのみ作動。pool 不明な券種は素のケリー。
        self._pool_impact = pool_impact

    def allocate(self, candidates: list, bankroll: float, pool_by_race: dict | None = None) -> list:
        """候補に stake を設定する。pool_by_race={race_id: 復元プール} で自己購入影響を反映。"""
        per_bet_cap = self._per_bet_cap_ratio * bankroll
        pools = pool_by_race or {}
        stakes = []
        for c in candidates:
            f = kelly_fraction(c.probability, c.odds)
            fraction = self._kelly_ratio * f * c.confidence
            stake = min(bankroll * fraction, per_bet_cap)
            stake = max(0.0, stake)
            # プール影響: 自己購入のオッズ低下で stake を上限（pool が分かる券種のみ）。
            if self._pool_impact and stake > 0:
                pool = pools.get(c.race_id)
                if pool:
                    stake = pool_capped_stake(stake, c.probability, c.odds, float(pool))
            stakes.append(stake)

        # 1日総予算の制約: 超過すれば全体を比例縮小（相対配分を維持）。
        total = sum(stakes)
        budget = self._max_daily_ratio * bankroll
        if total > budget and total > 0:
            scale = budget / total
            stakes = [s * scale for s in stakes]

        return [dataclasses.replace(c, stake=s) for c, s in zip(candidates, stakes, strict=False)]
