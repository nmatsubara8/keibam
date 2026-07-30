"""層別 ΔR² ハーネスの純ロジック（races 変換・帯分け・in/OOS ΔR²）の単体テスト。"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd

_MOD = Path(__file__).resolve().parents[2] / "scripts" / "jrdb_stratified_dr2.py"
_spec = importlib.util.spec_from_file_location("jrdb_stratified_dr2", _MOD)
s = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(s)


def _edge(rid, rows):
    """rows=[(umaban, r_hat, p_mkt, won)] の 1 レース分 edge_df 片。"""
    return pd.DataFrame(rows, columns=["umaban", "r_hat", "p_mkt", "won"],
                        index=[rid] * len(rows))


def test_field_size_and_dist_band():
    assert s.field_size_band(6) == "≤8" and s.field_size_band(10) == "9-12"
    assert s.field_size_band(15) == "13-16" and s.field_size_band(18) == "17-18"
    assert s.dist_band(1200) == "sprint≤1400" and s.dist_band(1600) == "mile1401-1800"
    assert s.dist_band(2000) == "mid1801-2200" and s.dist_band(2500) == "long>2200"
    assert s.dist_band(None) is None


def test_edge_to_races_shapes_and_filters():
    df = pd.concat([
        _edge("R1", [(1, 0.5, 0.4, 1), (2, 0.3, 0.35, 0), (3, 0.2, 0.25, 0)]),
        _edge("R2", [(1, 0.6, 0.5, 0), (2, 0.4, 0.5, 1)]),
        # 勝ち馬なしのレースは除外される
        _edge("R3", [(1, 0.5, 0.5, 0), (2, 0.5, 0.5, 0)]),
    ])
    races = s.edge_to_races(df)
    assert len(races) == 2                      # R3 は除外
    pf, pp, w = races[0]
    assert w == 1 and set(pf) == {1, 2, 3}


def test_dr2_respects_min_races():
    df = _edge("R1", [(1, 0.5, 0.4, 1), (2, 0.5, 0.6, 0)])
    races = s.edge_to_races(df)
    assert s.dr2_in_sample(races, min_races=5) is None      # レース不足→None
    # 十分な数なら float を返す（値の符号は問わない）
    many = races * 10
    assert isinstance(s.dr2_in_sample(many, min_races=5), float)


def test_dr2_oos_needs_both_sides():
    df = _edge("R1", [(1, 0.5, 0.4, 1), (2, 0.5, 0.6, 0)])
    races = s.edge_to_races(df) * 10
    assert s.dr2_oos(races, [], min_races=5) is None        # test 側不足→None
    assert isinstance(s.dr2_oos(races, races, min_races=5), float)
