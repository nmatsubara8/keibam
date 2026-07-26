"""calibrate_sim.py の純関数（目的距離・順位相関）の回帰テスト。

Optuna/実データ非依存の部分だけを検証する（較正本体はデータのある環境で実行）。
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    "calibrate_sim", Path(__file__).resolve().parents[2] / "calibrate_sim.py")
cal = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cal)


def test_spearman_sign():
    assert cal._spearman([1, 2, 3, 4, 5], [1, 2, 3, 4, 5]) == pytest.approx(1.0)
    assert cal._spearman([1, 2, 3, 4, 5], [5, 4, 3, 2, 1]) == pytest.approx(-1.0)


def test_objective_prefers_matching_aggregate_stats():
    """実測に近い集約統計＋高い直接一致の方が距離が小さい（＝良い較正）。"""
    real = {"style_pos": 0.37, "draw_bias": 0.20, "backness": 0.05}
    good = {"style_pos": 0.38, "draw_bias": 0.19, "backness": 0.05,
            "pos_direct": 0.30, "pace_shape": 0.15}
    bad = {"style_pos": 0.61, "draw_bias": 0.02, "backness": 0.00,
           "pos_direct": 0.10, "pace_shape": 0.05}
    assert cal.objective_distance(good, real, True) < cal.objective_distance(bad, real, True)


def test_param_bounds_are_physical():
    # 較正パラメータは物理prior bounds 内（探索範囲の健全性）
    for _k, (lo, hi) in cal.PARAM_BOUNDS.items():
        assert lo <= hi and lo >= 0.0
    assert cal.PARAM_BOUNDS["turn_k"][1] <= 0.05     # 大外距離ロスは数%まで
