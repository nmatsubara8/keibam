"""レース内 listwise softmax 純関数の単体テスト（OBJ_COMPARE 核・LightGBM 非依存）。"""
from __future__ import annotations

import numpy as np

from src.training._listwise_objective import (
    fit_race_temperature,
    make_race_softmax_fobj,
    ndcg_at_k,
    race_softmax_ce_grad_hess,
    race_softmax_nll,
    race_softmax_probs,
)


def test_softmax_sums_to_one_per_race():
    scores = np.array([1.0, 2.0, 0.0, 5.0, 5.0])
    gids = np.array(["r1", "r1", "r1", "r2", "r2"])
    p = race_softmax_probs(scores, gids)
    assert abs(p[:3].sum() - 1.0) < 1e-12
    assert abs(p[3:].sum() - 1.0) < 1e-12
    assert abs(p[3] - 0.5) < 1e-12 and abs(p[4] - 0.5) < 1e-12   # 同スコア→均等


def test_grad_hess_shapes_and_values():
    scores = np.array([0.0, 0.0])          # 1 race 2頭・均等 p=0.5
    y = np.array([1.0, 0.0])
    g, h = race_softmax_ce_grad_hess(scores, y, np.array(["r", "r"]))
    assert np.allclose(g, [0.5 - 1.0, 0.5 - 0.0])   # p - y
    assert np.allclose(h, [0.25, 0.25])             # p(1-p)


def test_nll_is_race_weighted_mean_of_winner():
    # 2 race、winner が確率 0.5/0.25 → NLL = mean(-log 0.5, -log 0.25)
    scores = np.array([0.0, 0.0, 0.0, np.log(3.0), 0.0, 0.0])   # r2: winner score log3 → p=3/5
    y = np.array([1.0, 0.0, 0.0, 1.0, 0.0, 0.0])
    gids = np.array(["a", "a", "a", "b", "b", "b"])
    p = race_softmax_probs(scores, gids)
    expect = float((-np.log(p[0]) - np.log(p[3])) / 2)
    assert abs(race_softmax_nll(scores, y, gids) - expect) < 1e-9


def test_temperature_recovers_scale_toward_min_nll():
    rng = np.random.RandomState(0)
    # 真の勝率構造をスケール s*5 に膨らませた score → T>1 が NLL を下げるはず
    n_race = 200
    gids = np.repeat(np.arange(n_race), 6)
    logits = rng.randn(n_race * 6)
    p = race_softmax_probs(logits, gids)
    y = np.zeros_like(p)
    for r in range(n_race):                          # 各 race の勝者を p から **サンプル**（argmax でなく）
        idx = np.where(gids == r)[0]
        pr = p[idx] / p[idx].sum()
        y[idx[rng.choice(len(idx), p=pr)]] = 1.0
    inflated = logits * 5.0                           # 真スケールを5倍に膨らませた過信 score
    T = fit_race_temperature(inflated, y, gids)
    assert T > 1.5                                   # T≈5 付近へ＝過信スケールを平坦化
    assert race_softmax_nll(inflated, y, gids, temperature=T) <= \
        race_softmax_nll(inflated, y, gids, temperature=1.0)


def test_ndcg_perfect_and_reversed():
    rel = np.array([3.0, 2.0, 1.0, 0.0])
    gids = np.array(["r"] * 4)
    assert abs(ndcg_at_k(rel, np.array([9, 8, 7, 6]), gids, 3) - 1.0) < 1e-9   # 完全順
    ndcg_bad = ndcg_at_k(rel, np.array([6, 7, 8, 9]), gids, 3)                 # 逆順
    assert 0.0 <= ndcg_bad < 1.0


def test_fobj_factory_matches_grad_hess():
    scores = np.array([0.3, -0.1, 0.8, 0.0])
    gids = np.array(["r1", "r1", "r2", "r2"])
    y = np.array([1.0, 0.0, 0.0, 1.0])

    class _DS:
        def get_label(self):
            return y

    g1, h1 = make_race_softmax_fobj(gids)(scores, _DS())
    g2, h2 = race_softmax_ce_grad_hess(scores, y, gids)
    assert np.allclose(g1, g2) and np.allclose(h1, h2)
