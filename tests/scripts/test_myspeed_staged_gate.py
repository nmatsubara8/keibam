"""本格MySpeed 段階ゲートの純ロジック（履歴leak-safe・条件内標準化・ROI）テスト。"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

_MOD = Path(__file__).resolve().parents[2] / "scripts" / "myspeed_staged_gate.py"
_spec = importlib.util.spec_from_file_location("myspeed_staged_gate", _MOD)
m = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(m)


def test_build_hist_is_leak_safe_and_counts_past():
    df = pd.DataFrame({
        "horse_id": ["h1"] * 4,
        "rid": ["202401", "202402", "202403", "202404"],
        "v": [50.0, 60.0, 70.0, 80.0],
    })
    out = m.build_hist(df, "v", "raw").sort_values("rid")
    assert np.isnan(out["raw_last"].iloc[0])               # 初戦は過去なし
    assert out["raw_last"].iloc[1] == 50.0                 # 前走
    assert out["raw_max5"].iloc[3] == 70.0                 # 当該走80は含まぬ
    assert abs(out["raw_trend"].iloc[3] - 15.0) < 1e-9     # 70 - mean(60,50)
    assert list(out["raw_npast"]) == [0, 1, 2, 3]          # 過去走数（当該除外）


def test_dist_band_fixed_edges():
    b = m.dist_band(pd.Series([1000, 1200, 1600, 2000, 3000]))
    assert list(b) == [0, 1, 3, 5, 7]                      # 固定境界での帯index


def test_bucket_stats_uses_only_fit_rows_and_min_n():
    vals = np.array([10.0, 12.0, 100.0, 200.0])            # k=A の2件は<Y、B は評価行
    keys = np.array(["A", "A", "B", "B"])
    fit = np.array([True, True, False, False])
    stats = m.bucket_stats(vals, keys, fit, min_n=2)
    assert "A" in stats and "B" not in stats               # B は fit 行が無い→除外
    assert abs(stats["A"][0] - 11.0) < 1e-9                # 平均は fit の2件のみ


def test_condition_zscore_falls_back_to_global_for_unknown_bucket():
    vals = np.array([11.0, 5.0])
    keys = np.array(["A", "ZZZ"])                          # ZZZ は未知バケット
    stats = {"A": (10.0, 2.0)}
    z = m.condition_zscore(vals, keys, stats, gmu=0.0, gsd=1.0)
    assert abs(z[0] - 0.5) < 1e-9                          # (11-10)/2
    assert abs(z[1] - 5.0) < 1e-9                          # 未知→(5-0)/1


def test_roi_helpers():
    pay = np.array([0.0, 0.0, 0.0, 6.0])
    score = np.array([0.1, 0.2, 0.3, 0.9])
    assert abs(m.roi_top_pct(pay, score, 25.0) - 6.0) < 1e-9
    assert abs(m.roi_top_pct(pay, score, 50.0) - 3.0) < 1e-9
    assert m.roi_excl_top(np.array([0.0] * 9 + [100.0]), k=5) == 0.0
    assert np.isnan(m.roi_excl_top(np.array([1.0, 2.0]), k=5))
