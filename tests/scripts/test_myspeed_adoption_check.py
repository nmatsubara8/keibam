"""myspeed_adoption_check の純ロジック（ターゲット化・ECE・特徴量選択）テスト。"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

_MOD = Path(__file__).resolve().parents[2] / "scripts" / "myspeed_adoption_check.py"
_spec = importlib.util.spec_from_file_location("myspeed_adoption_check", _MOD)
m = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(m)


def test_make_target_place_and_win():
    rank = pd.Series([1, 2, 3, 4, np.nan])
    place = m.make_target(rank, win=False)  # ≤3
    win = m.make_target(rank, win=True)     # ==1
    assert list(place[:4]) == [1.0, 1.0, 1.0, 0.0]
    assert list(win[:4]) == [1.0, 0.0, 0.0, 0.0]
    assert np.isnan(place[4]) and np.isnan(win[4])  # 着順欠損は NaN


def test_ece_perfect_calibration_is_zero():
    # 予測=実現率のビンが揃えば ECE≈0
    y = np.array([0.0, 1.0] * 50)
    p = np.full(100, 0.5)  # ビン内平均予測0.5・実現率0.5
    assert m.ece(p, y) < 1e-9


def test_ece_detects_miscalibration():
    y = np.zeros(100)          # 実現率0
    p = np.full(100, 0.9)      # 予測0.9 → 誤差0.9
    assert abs(m.ece(p, y) - 0.9) < 1e-9


def test_select_feature_cols_excludes_target_and_myspeed():
    df = pd.DataFrame({
        "rank": [1, 2], "date": ["2020-01-01", "2020-01-02"], "単勝": [3.0, 4.0],
        "feat_a": [0.1, 0.2], "feat_b": [1, 2],
        "jrdb_ms_last": [50.0, 60.0], "jrdb_ms_npast": [1, 2],
        "name": ["x", "y"],  # 非数値は自動除外
    })
    cols = m.select_feature_cols(df, ["jrdb_ms_last", "jrdb_ms_npast"])
    assert cols == ["feat_a", "feat_b"]              # 順序保存・target/myspeed/非数値を除外
    assert "rank" not in cols and "単勝" not in cols
    assert not any(c.startswith("jrdb_ms_") for c in cols)
