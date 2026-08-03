"""レース内 listwise softmax の純関数群（OBJ_COMPARE 実験の核・LightGBM 非依存で単体テスト可能）。

docs/objective_comparison_design.md の RACE_SOFTMAX_CE / OOF temperature / NDCG を提供する。
すべてレースを group_ids（各行の race ラベル）で束ねる純 numpy 実装。順序に依存しない
（np.unique の inverse で group 化）。LightGBM の custom fobj はこの grad/hess を薄く包むだけ。
"""
from __future__ import annotations

import numpy as np


def _inverse_groups(group_ids):
    """group_ids → (inv 0..G-1, G)。行順に依存せず race を 0 始まり index へ写す。"""
    g = np.asarray(group_ids)
    if len(g) == 0:
        return np.asarray([], dtype=int), 0
    _, inv = np.unique(g, return_inverse=True)
    return inv, int(inv.max()) + 1


def race_softmax_probs(scores, group_ids):
    """各 race 内で softmax した確率（Σ_{i∈r} p_i = 1）。数値安定化に per-group max を引く。純関数。"""
    s = np.asarray(scores, dtype=float)
    inv, G = _inverse_groups(group_ids)
    if G == 0:
        return np.asarray([], dtype=float)
    gmax = np.full(G, -np.inf)
    np.maximum.at(gmax, inv, s)
    ex = np.exp(s - gmax[inv])
    gsum = np.zeros(G)
    np.add.at(gsum, inv, ex)
    return ex / gsum[inv]


def race_softmax_ce_grad_hess(scores, y, group_ids):
    """listwise softmax CE の勾配・対角 Hessian（LightGBM custom fobj 用）。純関数。

    L_r=−log p_{winner,r}。g_i=p_i−y_i、h_i=p_i(1−p_i)（y: 1着=1 他0）。
    """
    p = race_softmax_probs(scores, group_ids)
    yv = np.asarray(y, dtype=float)
    grad = p - yv
    hess = p * (1.0 - p)
    return grad, hess


def race_softmax_nll(scores, y, group_ids, *, temperature: float = 1.0, eps: float = 1e-12):
    """レース内 softmax(scores/T) の勝者 NLL の race 等重み平均（nats/race）。純関数。

    各 race に勝者(y==1)が1頭ある前提。NLL_r=−log p_{winner}。返す平均は勝者行の −log p の平均。
    """
    s = np.asarray(scores, dtype=float) / float(temperature)
    p = race_softmax_probs(s, group_ids)
    yv = np.asarray(y, dtype=float)
    mask = yv == 1
    if not mask.any():
        return float("nan")
    return float(-np.log(p[mask] + eps).mean())


def fit_race_temperature(scores, y, group_ids, *, lo: float = 0.05, hi: float = 20.0,
                         n_grid: int = 60, n_refine: int = 2):
    """勝者 race-NLL を最小化する単一 temperature T>0 を推定（coarse→fine grid・純関数）。

    ranker score は任意スケールゆえ、fold 内 training-OOF からこの T を推定し `p=softmax(s/T)` にする
    （馬単位 isotonic→再正規化は使わない・設計 §較正）。Date/random 非依存。
    """
    best_T, best = 1.0, np.inf
    for _ in range(max(1, n_refine)):
        grid = np.geomspace(lo, hi, n_grid)
        for T in grid:
            nll = race_softmax_nll(scores, y, group_ids, temperature=float(T))
            if nll < best:
                best, best_T = nll, float(T)
        # refine 窓を best_T の周辺へ
        span = (hi / lo) ** (1.0 / n_grid)
        lo, hi = best_T / span**2, best_T * span**2
    return float(best_T)


def ndcg_at_k(relevance, scores, group_ids, k: int):
    """レース内 NDCG@k の race 等重み平均（順位品質の参考指標・純関数）。

    relevance r_i（例 1着=3/2着=2/3着=1/他0）、scores で降順に並べ DCG=Σ(2^r−1)/log2(rank+1)、
    IDCG（relevance 降順）で正規化。IDCG=0 の race は除外。
    """
    rel = np.asarray(relevance, dtype=float)
    s = np.asarray(scores, dtype=float)
    inv, G = _inverse_groups(group_ids)
    if G == 0:
        return float("nan")
    vals = []
    for gid in range(G):
        m = inv == gid
        r = rel[m]
        order = np.argsort(-s[m], kind="stable")
        topk = order[:k]
        gains = (2.0 ** r[topk] - 1.0)
        discounts = 1.0 / np.log2(np.arange(2, len(topk) + 2))
        dcg = float((gains * discounts).sum())
        ideal_order = np.argsort(-r, kind="stable")[:k]
        igains = (2.0 ** r[ideal_order] - 1.0)
        idcg = float((igains * discounts[:len(ideal_order)]).sum())
        if idcg > 0:
            vals.append(dcg / idcg)
    return float(np.mean(vals)) if vals else float("nan")


def make_race_softmax_fobj(group_ids):
    """LightGBM の custom objective `fobj(preds, dataset)` を返す（group_ids を閉じ込める）。

    preds は raw score、ラベルは dataset.get_label()。行順は学習データと同順である前提。
    """
    gids = np.asarray(group_ids)

    def _fobj(preds, dataset):
        y = dataset.get_label()
        return race_softmax_ce_grad_hess(preds, y, gids)

    return _fobj
