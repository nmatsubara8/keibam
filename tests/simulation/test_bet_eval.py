"""賭け評価3層ロジック（_bet_eval）のテスト。"""
from __future__ import annotations

import numpy as np

from src.simulation._bet_eval import (
    best_threshold,
    ev_bet_metrics,
    quality_metrics,
)


def _races():
    # 2レース×3頭。R1: 馬0が勝ち(odds2.0)、R2: 馬1が勝ち(odds5.0)。
    return [
        (np.array([0.6, 0.3, 0.1]), np.array([2.0, 4.0, 12.0]), 0),
        (np.array([0.2, 0.5, 0.3]), np.array([6.0, 5.0, 4.0]), 1),
    ]


def test_ev_bet_metrics_roi_and_diagnostics():
    # threshold=1.0: EV>1 の馬を買う。R1馬0 EV=1.2(買・的中,払戻200)、R2馬1 EV=2.5(買・的中,払戻500)
    m = ev_bet_metrics(_races(), threshold=1.0, min_odds=1.0, max_odds=100.0)
    assert m["n_bets"] >= 2
    assert m["total_return"] >= 700.0 - 1e-9        # 200+500 の的中を含む
    assert m["roi"] == m["total_return"] / m["total_stake"]
    assert 0.0 <= m["hit_rate"] <= 1.0
    assert m["max_dd"] >= 0.0


def test_ev_bet_metrics_odds_band_filters():
    m = ev_bet_metrics(_races(), threshold=1.0, min_odds=1.0, max_odds=3.0)
    # odds<=3 のみ → R1馬0(2.0)のみ購入（R2馬1は5.0で除外）
    assert m["n_bets"] == 1 and m["avg_odds"] == 2.0


def test_best_threshold_maximizes_roi():
    best = best_threshold(_races(), [1.0, 1.5, 2.0, 3.0], min_bets=1)
    assert "threshold" in best and best["roi"] >= 0.0
    # 返るのは実際にその threshold で測った ROI
    recomputed = ev_bet_metrics(_races(), best["threshold"], min_odds=1.0, max_odds=100.0)
    assert abs(best["roi"] - recomputed["roi"]) < 1e-9


def test_quality_metrics_tier3():
    q = quality_metrics(_races())
    assert q["n_races"] == 2
    assert q["logloss_sim"] is not None and q["logloss_market"] is not None
    assert 0.0 <= q["brier"] <= 2.0
    assert q["auc"] is None or 0.0 <= q["auc"] <= 1.0


def test_auc_perfect_separation():
    # 勝ち馬に最高 score → AUC=1.0
    races = [(np.array([0.9, 0.1, 0.05]), np.array([2.0, 3.0, 4.0]), 0)]
    q = quality_metrics(races)
    assert q["auc"] == 1.0
