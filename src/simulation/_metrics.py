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


def classification_metrics(
    y_true: np.ndarray,
    prob: np.ndarray,
    top_n: int = 3,
) -> dict:
    """確率予測の分類性能を多面評価する（§8）。

    回収率と LogLoss は乖離しうる（KB shard-01）ため、AUC・LogLoss・Brier・F1 を
    並列追跡してモデル比較の判断材料にする。不均衡データ（正例率 ~1/16）では
    F1 が特に重要（KB context）。

    Returns
    -------
    dict: log_loss / brier_score / auc / f1_score_top1 / f1_score_topN を含む。
        計算不能な指標（単一クラスしかない等）は NaN を返す。
    """
    from sklearn.metrics import (
        brier_score_loss,
        log_loss,
        roc_auc_score,
    )

    y = np.asarray(y_true).astype(int)
    p = np.clip(np.asarray(prob, dtype=float), 1e-15, 1.0 - 1e-15)

    summary: dict = {}
    n_pos = int(y.sum())

    # LogLoss / Brier は y に両クラス無くても計算可能（labels 明示）
    try:
        summary["log_loss"] = float(log_loss(y, p, labels=[0, 1]))
    except (ValueError, IndexError):
        summary["log_loss"] = float("nan")
    summary["brier_score"] = float(brier_score_loss(y, p)) if len(y) else float("nan")

    # AUC は両クラス必要
    if 0 < n_pos < len(y):
        summary["auc"] = float(roc_auc_score(y, p))
    else:
        summary["auc"] = float("nan")

    # F1: 確率を「上位 N 位以内」で二値化して評価
    summary["f1_score_top1"] = _f1_at_topk(y, p, 1)
    summary[f"f1_score_top{top_n}"] = _f1_at_topk(y, p, top_n)
    return summary


def _f1_at_topk(y_true: np.ndarray, prob: np.ndarray, k: int) -> float:
    """確率上位 k 件を正例予測として F1 を計算する。"""
    from sklearn.metrics import f1_score

    n = len(prob)
    if n == 0 or k <= 0:
        return float("nan")
    k = min(k, n)
    # 上位 k 件を 1、それ以外を 0
    threshold_idx = np.argsort(prob)[::-1][:k]
    y_pred = np.zeros(n, dtype=int)
    y_pred[threshold_idx] = 1
    if y_true.sum() == 0 and y_pred.sum() == 0:
        return float("nan")
    return float(f1_score(y_true, y_pred, zero_division=0))
