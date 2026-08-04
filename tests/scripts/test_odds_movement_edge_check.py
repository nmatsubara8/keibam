"""オッズの動き×ML 残差エッジ検証ハーネスの純ロジック単体テスト。"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd

_MOD = Path(__file__).resolve().parents[2] / "scripts" / "odds_movement_edge_check.py"
_spec = importlib.util.spec_from_file_location("odds_movement_edge_check", _MOD)
m = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(m)


def _frame():
    # R1: 馬1 が OZ→TYB で短縮(被支持)・ML も上位。馬2 は延伸・ML 下位。
    return pd.DataFrame([
        {"rid": "2025", "uma": 1, "o_oz": 4.0, "o_tyb": 3.0, "o_final": 3.0, "p_ml": 0.5, "won": 1.0},
        {"rid": "2025", "uma": 2, "o_oz": 4.0, "o_tyb": 6.0, "o_final": 6.0, "p_ml": 0.2, "won": 0.0},
    ])


def test_add_normalized_sums_to_one_and_signs():
    d = m.add_normalized(_frame())
    g = d.groupby("rid")
    assert abs(g["q_oz"].sum().iloc[0] - 1.0) < 1e-9
    assert abs(g["q_tyb"].sum().iloc[0] - 1.0) < 1e-9
    # 馬1 は短縮＝q 上昇 → dq>0。馬2 は延伸 → dq<0。
    assert d.loc[d.uma == 1, "dq"].iloc[0] > 0
    assert d.loc[d.uma == 2, "dq"].iloc[0] < 0


def test_segment_label_quadrants_and_dead():
    assert m.segment_label(0.1, 0.1, 0.01) == "市場↑×ML↑(一致)"
    assert m.segment_label(0.1, -0.1, 0.01) == "市場↑×ML↓(逆行)"   # Case2
    assert m.segment_label(-0.1, 0.1, 0.01) == "市場↓×ML↑(逆行)"
    assert m.segment_label(0.001, 0.1, 0.01) == "中立"              # |dq|<dead
    assert m.segment_label(0.1, 0.001, 0.01) == "中立"


def test_segment_roi_settles_at_final():
    d = m.add_normalized(_frame())
    roi = m.segment_roi(d, dead=0.0)
    # 馬1 のセグメントは的中(最終3.0払戻)、n=1、return_rate=3.0
    row = roi[roi["seg"].str.contains("一致")].iloc[0]
    assert row["n"] == 1 and abs(row["return_rate"] - 3.0) < 1e-9


def test_races_for_blend_requires_single_winner():
    d = m.add_normalized(_frame())
    races = m.races_for_blend(d, "q_oz", "q_tyb")
    assert len(races) == 1
    pf, pp, w = races[0]
    assert w == 1 and set(pf) == {1, 2}
