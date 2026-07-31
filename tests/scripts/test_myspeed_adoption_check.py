"""myspeed_adoption_check の純ロジック（目的変数直用・ECE・特徴量選択）テスト。"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

_MOD = Path(__file__).resolve().parents[2] / "scripts" / "myspeed_adoption_check.py"
_spec = importlib.util.spec_from_file_location("myspeed_adoption_check", _MOD)
m = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(m)


def _df():
    # featured の目的変数は既に二値（rank=着順<4, rank_win=着順==1）。
    return pd.DataFrame({
        "rank": [1, 1, 0, np.nan],       # 複勝(top3)
        "rank_win": [1, 0, 0, np.nan],   # 単勝(1着)
        "date": ["2020-01-01"] * 4, "着順": [1, 3, 8, 2], "通過": ["1-1"] * 4,
        "単勝": [3.0, 5.0, 20.0, 4.0],  # 市場オッズ（本番は学習から除外）
        "horse_id": [1, 2, 3, 4], "race_id": ["r"] * 4,
        "feat_a": [0.1, 0.2, 0.3, 0.4], "feat_b": [1, 2, 3, 4],
        "jrdb_ms_last": [50.0, 60.0, 40.0, 55.0], "jrdb_ms_npast": [1, 2, 3, 1],
        "name": ["x", "y", "z", "w"],  # 非数値は自動除外
    })


def test_target_series_place_and_win_use_binary_columns_directly():
    df = _df()
    place = m.target_series(df, win=False)  # rank をそのまま
    win = m.target_series(df, win=True)     # rank_win をそのまま
    assert list(place[:3]) == [1.0, 1.0, 0.0]   # <=3 の再変換をしない（二値直用）
    assert list(win[:3]) == [1.0, 0.0, 0.0]
    assert np.isnan(place[3]) and np.isnan(win[3])  # 目的変数欠損は NaN で残す


def test_target_series_missing_column_raises():
    try:
        m.target_series(pd.DataFrame({"date": ["2020-01-01"]}), win=False)
        raised = False
    except KeyError:
        raised = True
    assert raised


def test_select_feature_cols_excludes_targets_ids_and_prefix():
    df = _df()
    full = m.select_feature_cols(df)
    base = m.select_feature_cols(df, drop_prefixes=("jrdb_ms_",))
    # 目的変数・ID・事後情報・非数値・市場オッズは既定で除外（本番準拠）
    for leak in ["rank", "rank_win", "着順", "通過", "単勝", "horse_id", "race_id", "date", "name"]:
        assert leak not in full
    assert "feat_a" in full and "feat_b" in full
    # full は jrdb_ms_* を含み、base は含まない（＝アブレーション差分が MySpeed だけ）
    assert "jrdb_ms_last" in full and "jrdb_ms_npast" in full
    assert not any(c.startswith("jrdb_ms_") for c in base)
    assert set(full) - set(base) == {"jrdb_ms_last", "jrdb_ms_npast"}


def test_select_feature_cols_keep_odds_toggles_tansho():
    df = _df()
    assert "単勝" not in m.select_feature_cols(df)                      # 既定=本番準拠で除外
    assert "単勝" in m.select_feature_cols(df, keep_odds=True)          # 明示指定で残す


def test_ece_perfect_calibration_is_zero():
    y = np.array([0.0, 1.0] * 50)
    p = np.full(100, 0.5)
    assert m.ece(p, y) < 1e-9


def test_ece_detects_miscalibration():
    y = np.zeros(100)
    p = np.full(100, 0.9)
    assert abs(m.ece(p, y) - 0.9) < 1e-9
