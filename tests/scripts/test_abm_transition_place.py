"""最小ABM遷移モデルの純ロジック（位置正規化・ロールアウト・上位ROI）テスト。"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

_MOD = Path(__file__).resolve().parents[2] / "scripts" / "abm_transition_place.py"
_spec = importlib.util.spec_from_file_location("abm_transition_place", _MOD)
a = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(a)


def test_norm_pos_relative():
    corner = pd.Series([1, 5, 9])          # 9頭立て → 0.0, 0.5, 1.0
    fs = pd.Series([9, 9, 9])
    q = a.norm_pos(corner, fs)
    assert abs(q.iloc[0] - 0.0) < 1e-9 and abs(q.iloc[1] - 0.5) < 1e-9 and abs(q.iloc[2] - 1.0) < 1e-9
    # 頭数1・0順位は NaN
    assert a.norm_pos(pd.Series([0]), pd.Series([9])).isna().iloc[0]
    assert a.norm_pos(pd.Series([1]), pd.Series([1])).isna().iloc[0]


def test_rollout_applies_transition_n_times_and_clips():
    # trans_fn: 毎回 +0.1（位置後退）。start 0.2 → 3回 → 0.5。
    def trans(pos, feats):
        return pos + 0.1
    out = a.rollout_positions(np.array([0.2, 0.9]), trans, np.zeros((2, 1)), n_steps=3)
    assert abs(out[0] - 0.5) < 1e-9
    assert out[1] == 1.0                    # 0.9→1.2 clip 1.0


def test_place_roi_by_score_picks_top():
    score = np.array([0.1, 0.9, 0.5, 0.8])   # 上位50%=idx 1,3
    won = np.array([0.0, 1.0, 0.0, 1.0])
    pay = np.array([2.0, 3.0, 2.0, 4.0])     # 精算: idx1=3, idx3=4
    roi, k = a.place_roi_by_score(score, won, pay, top_pct=50.0)
    assert k == 2 and abs(roi - (3.0 + 4.0) / 2) < 1e-9
