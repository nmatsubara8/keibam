"""ポートフォリオ（資金配分）シミュレーション。

配分済み BetCandidate と「実際の払戻倍率」を返す関数から、資金推移・成長率・
最大ドローダウン等を計算する純粋関数。決済データの取得方法には依存しない
（payout_fn を注入）ため、固定額ベットとケリー配分の比較が容易。
"""

from __future__ import annotations

import math
from typing import Callable

from src.policies._bet_candidate import BetCandidate

# 当たり時の払戻倍率（外れは0）を返す関数の型
PayoutFn = Callable[[BetCandidate], float]


def simulate(allocated: list, payout_fn: PayoutFn, initial_bankroll: float) -> dict:
    """配分済み候補を順に決済し、成績を集計する。

    payout_fn(candidate) は的中時の払戻倍率（=オッズ）、外れ時 0 を返す。
    返戻金 = stake * payout、損益 = 返戻金 - stake。
    """
    bankroll = initial_bankroll
    curve = [bankroll]
    total_staked = 0.0
    total_return = 0.0
    n_bets = 0
    n_hits = 0
    peak = bankroll
    max_dd = 0.0

    for c in allocated:
        if c.stake <= 0:
            continue
        payout = payout_fn(c)
        ret = c.stake * payout
        bankroll += ret - c.stake
        curve.append(bankroll)
        total_staked += c.stake
        total_return += ret
        n_bets += 1
        if payout > 0:
            n_hits += 1
        peak = max(peak, bankroll)
        max_dd = max(max_dd, peak - bankroll)

    growth = math.log(bankroll / initial_bankroll) if bankroll > 0 and initial_bankroll > 0 else float("-inf")
    return {
        "initial_bankroll": initial_bankroll,
        "final_bankroll": bankroll,
        "total_staked": total_staked,
        "total_return": total_return,
        "return_rate": (total_return / total_staked) if total_staked > 0 else 0.0,
        "profit": total_return - total_staked,
        "log_growth": growth,
        "max_drawdown": max_dd,
        "n_bets": n_bets,
        "n_hits": n_hits,
        "hit_rate": (n_hits / n_bets) if n_bets else 0.0,
        "bankroll_curve": curve,
    }
