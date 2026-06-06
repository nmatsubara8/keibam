"""回収成績の指標計算（純粋関数）。

レースごとの収支 DataFrame から回収率・シャープレシオ・最大ドローダウン等を算出する。
I/O や馬券ロジックに依存しないため単体テストが容易（Simulator から委譲して利用）。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# returns_per_race に期待する列
N_BETS = "n_bets"
BET_AMOUNT = "bet_amount"
RETURN_AMOUNT = "return_amount"
HIT_OR_NOT = "hit_or_not"


def max_drawdown(profit_per_race: pd.Series) -> float:
    """レース順の損益系列から最大ドローダウン（正の値）を算出する。"""
    cumulative = profit_per_race.cumsum()
    running_peak = cumulative.cummax()
    drawdown = running_peak - cumulative
    if len(drawdown) == 0:
        return 0.0
    return float(drawdown.max())


def summarize_returns(returns_per_race: pd.DataFrame) -> dict:
    """成績指標を集計して dict で返す。

    既存キー（n_bets/n_races/n_hits/total_bet_amount/return_rate/std）に加え、
    hit_rate / sharpe_ratio / max_drawdown / profit を追加する。
    """
    summary: dict = {}
    if returns_per_race is None or len(returns_per_race) == 0:
        return summary

    total_bet = float(returns_per_race[BET_AMOUNT].sum())
    total_return = float(returns_per_race[RETURN_AMOUNT].sum())
    n_races = int(returns_per_race.index.nunique())

    summary[N_BETS] = int(returns_per_race[N_BETS].sum())
    summary["n_races"] = n_races
    summary["n_hits"] = int(returns_per_race[HIT_OR_NOT].sum())
    summary["total_bet_amount"] = total_bet
    summary["profit"] = total_return - total_bet
    summary["hit_rate"] = summary["n_hits"] / n_races if n_races else 0.0

    if total_bet == 0:
        summary["return_rate"] = 0.0
        summary["std"] = 0.0
        summary["sharpe_ratio"] = 0.0
    else:
        summary["return_rate"] = total_return / total_bet
        std = returns_per_race[RETURN_AMOUNT].std() * np.sqrt(n_races) / total_bet
        summary["std"] = float(std) if not np.isnan(std) else 0.0
        # シャープレシオ = (回収率 - 1) / リスク（回収率標準偏差）
        summary["sharpe_ratio"] = (summary["return_rate"] - 1.0) / summary["std"] if summary["std"] > 0 else 0.0

    profit_per_race = returns_per_race[RETURN_AMOUNT] - returns_per_race[BET_AMOUNT]
    summary["max_drawdown"] = max_drawdown(profit_per_race)
    return summary
