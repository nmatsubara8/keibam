"""卍式購入ゾーン（2D ROIセル・両側打ち切り・walk-forward適用）の純ロジックテスト。"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

_MOD = Path(__file__).resolve().parents[2] / "scripts" / "manji_purchase_zone.py"
_spec = importlib.util.spec_from_file_location("manji_purchase_zone", _MOD)
m = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(m)


def test_odds_band_boundaries():
    assert m.odds_band(1.5) == 0 and m.odds_band(2.0) == 1
    assert m.odds_band(6.9) == 2 and m.odds_band(7.0) == 3
    assert m.odds_band(100.0) == 4


def test_edge_band_partitions_evenly():
    eb = m.edge_band(np.linspace(0, 1, 100), n_bands=10)
    assert set(eb) == set(range(10))
    assert (eb[:10] == 0).all() and (eb[-10:] == 9).all()   # 昇順→下位帯0・上位帯9


def test_cell_roi_and_zone_selection_two_sided():
    # セル(1,1)だけ ROI 高(払戻3.0で全的中)、他は0。min_roi でそこだけ購入ゾーンに。
    df = pd.DataFrame({
        "eb": [1, 1, 1, 0, 0, 2], "ob": [1, 1, 1, 1, 1, 1],
        "pay": [3.0, 3.0, 3.0, 3.0, 3.0, 3.0], "won": [1.0, 1.0, 1.0, 0.0, 0.0, 0.0],
    })
    roi = m.cell_roi_table(df)
    zone = m.select_zone(roi, min_roi=1.0, min_n=3)
    assert zone == {(1, 1)}                       # ROI3.0・n3 のセルのみ（両側打ち切り）


def test_apply_zone_settles_only_in_zone():
    te = pd.DataFrame({
        "eb": [1, 0], "ob": [1, 1], "pay": [3.0, 3.0], "won": [1.0, 1.0],
    })
    roi, n, pay_sum = m.apply_zone(te, {(1, 1)})
    assert n == 1 and abs(roi - 3.0) < 1e-9 and abs(pay_sum - 3.0) < 1e-9
    # ゾーン外なら 0 件
    r2, n2, _ = m.apply_zone(te, {(5, 5)})
    assert n2 == 0 and np.isnan(r2)
