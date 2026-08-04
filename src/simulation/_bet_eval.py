"""賭け評価の純ロジック（3層: Tier1 ROI / Tier2 診断 / Tier3 予測精度）。

投資モデルとしての採否は ROI。log-loss 等は参考（Tier3）。rank_gain sweep では
「各 gain について threshold も最適化した max_t ROI(gain, t)」を比較すべき——threshold を固定すると
gain の真価を見逃すため。本モジュールはその評価を pandas/sklearn 非依存の純関数で提供する。

races: 各レース (p_sim, odds, winner_idx) のリスト。p_sim/odds は同順の1次元配列、winner_idx は
勝ち馬の位置（0始まり）。
"""
from __future__ import annotations

import math


def ev_bet_metrics(races, threshold: float, *, min_odds: float = 1.0,
                   max_odds: float = 100.0, stake: float = 100.0) -> dict:
    """EV=p_sim·odds>threshold かつ odds∈[min,max] の馬を stake 均等買い→ROI＋Tier2 診断。

    返す: roi(=回収率), hit_rate, n_bets, avg_odds, max_dd(円), sharpe(1ベット純収益率の平均/標準偏差),
    total_stake, total_return。買い目0なら roi=0・診断は None/0。
    """
    n_bets = hits = 0
    total_stake = total_return = 0.0
    sum_odds = 0.0
    cum = peak = 0.0
    max_dd = 0.0
    rets: list[float] = []          # 1ベットあたり純収益率 r = odds-1(的中) / -1(外れ)
    for p_sim, odds, w in races:
        for j in range(len(p_sim)):
            o = odds[j]
            if not (o >= min_odds and o <= max_odds):
                continue
            if p_sim[j] * o <= threshold:
                continue
            n_bets += 1
            total_stake += stake
            sum_odds += o
            win = (j == w)
            payout = stake * o if win else 0.0
            total_return += payout
            hits += int(win)
            r = (o - 1.0) if win else -1.0
            rets.append(r)
            cum += payout - stake         # 純収支（円）
            peak = max(peak, cum)
            max_dd = max(max_dd, peak - cum)
    roi = (total_return / total_stake) if total_stake > 0 else 0.0
    hit_rate = (hits / n_bets) if n_bets else 0.0
    avg_odds = (sum_odds / n_bets) if n_bets else 0.0
    sharpe = _sharpe(rets)
    return {
        "threshold": threshold, "roi": roi, "hit_rate": hit_rate, "n_bets": n_bets,
        "avg_odds": avg_odds, "max_dd": max_dd, "sharpe": sharpe,
        "total_stake": total_stake, "total_return": total_return,
    }


def _sharpe(rets: list[float]) -> float | None:
    n = len(rets)
    if n < 2:
        return None
    mean = sum(rets) / n
    var = sum((r - mean) ** 2 for r in rets) / (n - 1)
    sd = math.sqrt(var)
    return (mean / sd) if sd > 0 else None


def best_threshold(races, thresholds, *, min_odds: float = 1.0, max_odds: float = 100.0,
                   stake: float = 100.0, min_bets: int = 30) -> dict:
    """max_t ROI(gain, t) を返す（Tier1）。買い目 < min_bets の t は薄すぎとして除外。

    全 t が min_bets 未満なら、最も買い目が多い t を返す（roi は参考）。
    """
    mets = [ev_bet_metrics(races, t, min_odds=min_odds, max_odds=max_odds, stake=stake)
            for t in thresholds]
    ok = [m for m in mets if m["n_bets"] >= min_bets]
    if ok:
        return max(ok, key=lambda m: m["roi"])
    return max(mets, key=lambda m: m["n_bets"])


def quality_metrics(races, *, eps: float = 1e-6) -> dict:
    """Tier3（threshold 非依存の予測精度）: log-loss(sim/market)・AUC・Brier。

    market 確率は 1/odds をレース内で正規化（オーバーラウンド除去）。AUC は全馬 (勝ち=1) を対象。
    """
    ll_sim = ll_mkt = 0.0
    n_races = 0
    brier_sum = 0.0
    n_horses = 0
    scores: list[tuple[float, int]] = []       # (p_sim, is_winner) 全馬
    for p_sim, odds, w in races:
        m = len(p_sim)
        if m < 2 or not (0 <= w < m):
            continue
        inv = [1.0 / o if o > 0 else 0.0 for o in odds]
        s = sum(inv) or 1.0
        p_mkt = [x / s for x in inv]
        ll_sim += -math.log(min(max(p_sim[w], eps), 1.0))
        ll_mkt += -math.log(min(max(p_mkt[w], eps), 1.0))
        n_races += 1
        for j in range(m):
            y = 1 if j == w else 0
            brier_sum += (p_sim[j] - y) ** 2
            n_horses += 1
            scores.append((float(p_sim[j]), y))
    return {
        "logloss_sim": (ll_sim / n_races) if n_races else None,
        "logloss_market": (ll_mkt / n_races) if n_races else None,
        "brier": (brier_sum / n_horses) if n_horses else None,
        "auc": _auc(scores),
        "n_races": n_races,
    }


def _auc(scores: list[tuple[float, int]]) -> float | None:
    """(score, label) 群の ROC-AUC（Mann-Whitney U・タイは 0.5）。正例/負例が無ければ None。"""
    pos = [s for s, y in scores if y == 1]
    neg = [s for s, y in scores if y == 0]
    if not pos or not neg:
        return None
    # 順位法: 昇順 rank 平均で U を算出（同値は平均順位）。
    allv = sorted((s, y) for s, y in scores)
    ranks = _avg_ranks([s for s, _ in allv])
    rank_sum_pos = sum(r for r, (_, y) in zip(ranks, allv, strict=False) if y == 1)
    n_pos, n_neg = len(pos), len(neg)
    u = rank_sum_pos - n_pos * (n_pos + 1) / 2.0
    return u / (n_pos * n_neg)


def _avg_ranks(sorted_vals: list[float]) -> list[float]:
    """昇順ソート済み値の平均順位（1始まり・同値は平均）。"""
    n = len(sorted_vals)
    ranks = [0.0] * n
    i = 0
    while i < n:
        j = i
        while j + 1 < n and sorted_vals[j + 1] == sorted_vals[i]:
            j += 1
        avg = (i + 1 + j + 1) / 2.0
        for k in range(i, j + 1):
            ranks[k] = avg
        i = j + 1
    return ranks
