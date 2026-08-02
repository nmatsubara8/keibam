"""生DB完全性監査の純関数テスト（PK重複・NULL率・孤児率・年欠落・着順整合）。"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd

_MOD = Path(__file__).resolve().parents[2] / "scripts" / "audit_db_completeness.py"
_spec = importlib.util.spec_from_file_location("audit_db_completeness", _MOD)
ad = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ad)


def test_pk_duplicate_count():
    df = pd.DataFrame({"race_id": ["R1", "R1", "R1"], "umaban": [1, 2, 1]})  # (R1,1) 重複
    assert ad.pk_duplicate_count(df, ["race_id", "umaban"]) == 1
    assert ad.pk_duplicate_count(df, ["race_id"]) == 2                       # R1 が3行→2重複
    assert ad.pk_duplicate_count(df, ["missing"]) == -1                      # 列なし=判定不能


def test_null_rate():
    df = pd.DataFrame({"a": [1, None, 3, None], "s": ["x", None, "y", "z"]})
    assert ad.null_rate(df, "a") == 0.5
    assert ad.null_rate(df, "s") == 0.25
    assert ad.null_rate(df, "missing") == 1.0


def test_orphan_rate():
    o = ad.orphan_rate(["A", "B", "C", "C"], ["A", "B"])
    assert o["n_child"] == 3 and o["n_orphan"] == 1                          # C が孤児
    assert abs(o["rate"] - 1 / 3) < 1e-9
    assert ad.orphan_rate([], ["A"])["rate"] == 0.0


def test_year_span_missing():
    y = ad.year_span(["2019", "2019", "2021", "2022"])
    assert y["min"] == 2019 and y["max"] == 2022
    assert y["missing"] == [2020]
    assert ad.year_span([])["min"] is None


def test_rank_consistency():
    # R1: 3頭・1着1頭・範囲内。R2: 1着が2頭（同着扱いでなく異常）
    df = pd.DataFrame({
        "race_id": ["R1", "R1", "R1", "R2", "R2"],
        "着順": [1, 2, 3, 1, 1],
        "頭数": [3, 3, 3, 2, 2],
    })
    rc = ad.rank_consistency(df)
    assert rc["n_races"] == 2
    assert rc["one_winner_rate"] == 0.5          # R1 のみ1着1頭
    assert rc["rank_in_range_rate"] == 1.0       # 全 rank<=頭数
