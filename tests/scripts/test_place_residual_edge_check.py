"""複勝残差エッジ検定の純ロジック（複勝市場q・logit・複勝binning）単体テスト。"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

_MOD = Path(__file__).resolve().parents[2] / "scripts" / "place_residual_edge_check.py"
_spec = importlib.util.spec_from_file_location("place_residual_edge_check", _MOD)
p = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(p)


def test_place_market_q_sums_to_nplace_per_race():
    # 3頭・複勝オッズ均等 → 各 P(top3)=1.0（Σ=3）。clip 0.99。
    fo = pd.Series([2.0, 2.0, 2.0])
    rids = pd.Series(["A", "A", "A"])
    q = p.place_market_q(fo, rids, n_place=3)
    assert np.allclose(q.to_numpy(), 0.99)          # 各1.0→clip0.99
    # 傾斜: 低オッズほど高い入着確率
    fo2 = pd.Series([1.2, 3.0, 6.0, 9.0])
    q2 = p.place_market_q(fo2, pd.Series(["B"] * 4), n_place=3)
    assert q2.iloc[0] > q2.iloc[1] > q2.iloc[2] > q2.iloc[3]


def test_place_bin_stats_roi_uses_payoff():
    # 単調Δ、上位のみ入着かつ複勝払戻2.0倍 → 上位ビン ROI 高
    n = 100
    rng = np.arange(n)
    delta = rng / n - 0.5
    won = (rng >= 70).astype(float)          # 上位30%入着
    q = np.full(n, 0.3)
    pay = np.full(n, 2.0)                     # 複勝2.0倍
    stats = p.place_bin_stats(delta, won, q, pay, n_bins=10)
    assert len(stats) == 10
    assert stats.iloc[-1]["ROI"] > stats.iloc[0]["ROI"]
    # 最上位ビンは全入着×2.0倍 → ROI≈2.0
    assert abs(stats.iloc[-1]["ROI"] - 2.0) < 1e-9
    assert stats.iloc[0]["ROI"] == 0.0       # 最下位は非入着→払戻0


def test_field_interactions_capture_composition():
    # R1: 逃げ2頭(pm .1/.15, ten高)＝競合。R2: 逃げ0頭(pm .6/.7)＝平穏。
    df = pd.DataFrame([
        {"rid": "R1", "pace_median": 0.10, "ten_idx": 60},
        {"rid": "R1", "pace_median": 0.15, "ten_idx": 58},
        {"rid": "R1", "pace_median": 0.60, "ten_idx": 40},
        {"rid": "R2", "pace_median": 0.60, "ten_idx": 50},
        {"rid": "R2", "pace_median": 0.70, "ten_idx": 48},
    ])
    out, feats = p.add_field_interactions(df)
    r1 = out[out["rid"] == "R1"]
    r2 = out[out["rid"] == "R2"]
    assert set(feats) == {"nige_cnt", "front_ratio", "pace_med_var", "ten_var",
                          "escape_pressure", "front_conflict"}
    assert r1["nige_cnt"].iloc[0] == 2 and r2["nige_cnt"].iloc[0] == 0  # 逃げ頭数
    assert r1["front_conflict"].iloc[0] >= 1                            # 速い逃げ競合あり
    assert r2["front_conflict"].iloc[0] == 0                            # 平穏
    assert not any(c.startswith("_") for c in out.columns)             # 一時列は掃除


def test_logit_endpoints():
    assert p.logit(np.array([0.5]))[0] == 0.0
    assert np.isfinite(p.logit(np.array([0.0, 1.0]))).all()
