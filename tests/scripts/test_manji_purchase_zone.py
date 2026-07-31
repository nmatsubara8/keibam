"""卍式購入ゾーン v2（OOF・形状制約）の純ロジックテスト。"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

_MOD = Path(__file__).resolve().parents[2] / "scripts" / "manji_purchase_zone.py"
_spec = importlib.util.spec_from_file_location("manji_purchase_zone", _MOD)
m = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(m)


def test_odds_band_fixed_edges():
    assert m.odds_band(0.9) == -1                 # 帯外(1.0未満)
    assert m.odds_band(1.0) == 0 and m.odds_band(1.5) == 1
    assert m.odds_band(2.9) == 2 and m.odds_band(9.9) == 4 and m.odds_band(50.0) == 5


def test_assign_edge_band_uses_fixed_boundaries():
    be = np.array([-np.inf, 0.0, 0.5, np.inf])    # 3帯
    eb = m.assign_edge_band(np.array([-0.1, 0.2, 0.9]), be)
    assert list(eb) == [0, 1, 2]


def test_select_rectangle_prefers_connected_region():
    # セル(1,1)(1,2)(2,1)(2,2) が高ROI、他0 → 矩形[1-2]x[1-2]が選ばれる
    rows = []
    for eb in range(4):
        for ob in range(4):
            hi = eb in (1, 2) and ob in (1, 2)
            for _ in range(400):
                rows.append({"eb": eb, "ob": ob, "pay": 3.0, "won": 1.0 if hi else 0.0})
    df = pd.DataFrame(rows)
    zone = m.select_rectangle(df, n_eb=4, n_ob=4, min_roi=1.0, min_n=300)
    assert zone == {(1, 1), (1, 2), (2, 1), (2, 2)}


def test_select_free_cells_and_apply():
    tbl = pd.DataFrame({"eb": [1, 0], "ob": [1, 1], "n": [400, 400], "roi": [3.0, 0.5]})
    zone = m.select_free_cells(tbl, min_roi=1.0, min_n=300)
    assert zone == {(1, 1)}
    te = pd.DataFrame({"eb": [1, 0], "ob": [1, 1], "pay": [3.0, 3.0], "won": [1.0, 1.0]})
    roi, n, psum = m.apply_zone(te, zone)
    assert n == 1 and abs(roi - 3.0) < 1e-9 and abs(psum - 3.0) < 1e-9


def test_roi_excl_top_removes_top_payouts():
    pay = np.array([0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 100.0])  # 1件の大当たり
    # 上位5件除外 → 分子は残り5件(全0)=0、分母10 → ROI 0（単一高配当依存を露呈）
    assert m.roi_excl_top(pay, k=5) == 0.0
    assert np.isnan(m.roi_excl_top(np.array([1.0, 2.0]), k=5))       # 件数不足→NaN
