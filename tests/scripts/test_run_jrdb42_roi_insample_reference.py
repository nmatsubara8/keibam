"""JRDB42 in-sample ROI 参考の純部テスト（本命/EV 集計・非証拠ハーネス）。"""
from __future__ import annotations

import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "roi_ref", Path(__file__).resolve().parents[2] / "scripts" / "run_jrdb42_roi_insample_reference.py")
roi = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(roi)


def _races():
    return [
        {"race_id": "2018010101", "year": 2018, "winner": 1,
         "odds": {1: 2.0, 2: 4.0, 3: 8.0}, "feats": {1: {}, 2: {}, 3: {}}},
        {"race_id": "2018010102", "year": 2018, "winner": 3,
         "odds": {1: 2.0, 2: 4.0, 3: 5.0}, "feats": {1: {}, 2: {}, 3: {}}},
    ]


def test_honmei_picks_market_favorite_when_theta_zero():
    pnl, blk, hit, ev = roi._bet_metrics(_races(), {})   # theta={} → 市場本命=最小オッズ(1番)
    assert hit == [1, 0]                 # R1 本命1=winner・R2 本命1≠winner3
    assert pnl == [1.0, -1.0]            # payout-1（的中 odds2.0→+1.0 / 外れ→-1.0）
    assert blk == ["2018010101", "2018010102"]


def test_honmei_hit_rate_and_roi():
    pnl, _, hit, _ = roi._bet_metrics(_races(), {})
    assert sum(hit) / len(hit) == 0.5
    assert sum(p + 1.0 for p in pnl) / len(pnl) == 1.0   # (2.0 + 0.0)/2


def test_ev_accumulates_bets_and_wins():
    _, _, _, ev = roi._bet_metrics(_races(), {})
    for th, acc in ev.items():
        assert acc["n"] >= acc["win"] >= 0
        assert acc["payout"] >= 0.0


def test_rolling_folds_reused_selection_domain():
    folds = roi.rolling_folds(range(2015, 2027), first_eval_year=2018)
    assert folds and max(e for _, e in folds) == 2024   # 2025+ を eval にしない（selection 域）


def test_ratio_block_ci_recomputes_roi_per_block():
    # block 単位で ROI=Σpayout/Σbets を再計算。全 payout 一定なら CI はその値近傍に集中。
    lo, hi = roi._ratio_block_ci([1.0, 1.0, 1.0, 1.0], ["b1", "b1", "b2", "b2"],
                                 n_boot=1000, seed=0)
    assert abs(lo - 1.0) < 1e-9 and abs(hi - 1.0) < 1e-9    # 全て 1.0 → ROI=1.0 で不変
    # 高配当が1 block に集中→再標本化で ROI がばらつき CI 幅>0
    lo2, hi2 = roi._ratio_block_ci([0.0, 0.0, 30.0, 0.0], ["b1", "b1", "b2", "b2"],
                                   n_boot=2000, seed=0)
    assert hi2 > lo2                                        # 少数高配当は不安定＝CI 幅あり
    # block 1 個は判定不能（NaN）
    import math
    lo3, hi3 = roi._ratio_block_ci([1.0, 2.0], ["b1", "b1"], n_boot=100, seed=0)
    assert math.isnan(lo3) and math.isnan(hi3)
