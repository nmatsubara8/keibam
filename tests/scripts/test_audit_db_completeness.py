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


def test_norm_rank_series_tolerant():
    import numpy as np
    s = ad.norm_rank_series(["1", "01", "１", "1着", "10", "取消", "", None, np.nan])
    got = [None if (v != v) else int(v) for v in s]   # NaN→None
    assert got == [1, 1, 1, 1, 10, None, None, None, None]


def test_race_stats_by_year_splits_representation():
    import pandas as pd
    # 2014: 着順が '1着' 表現でも 1着1頭を正しく判定（頑健正規化）。2015: 素の int。
    df = pd.DataFrame({
        "race_id": ["201401010101"] * 3 + ["201501010101"] * 3,
        "着順": ["1着", "2着", "3着", 1, 2, 3],
    })
    ys = ad.race_stats_by_year(df)
    assert ys["2014"]["one_winner_rate"] == 1.0    # '1着' も 1着として拾える
    assert ys["2015"]["one_winner_rate"] == 1.0
    assert ys["2014"]["rows_per_race"] == 3.0


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


def test_race_id_structure_by_year():
    import pandas as pd
    # 2023: 12桁 正常。2025: 一部が 14桁（分裂/形式変化の想定）
    df = pd.DataFrame({"race_id": [
        "202305010101", "202305010102",
        "20250501010199", "2025050101019A", "202505010101",
    ]})
    st = ad.race_id_structure_by_year(df, years=["2023", "2025"])
    assert st["2023"]["len_dist"] == {12: 2}
    assert st["2025"]["len_dist"].get(14) == 2 and st["2025"]["len_dist"].get(12) == 1
    assert st["2023"]["place_top"] == {"05": 2}
    assert len(st["2025"]["samples"]) == 3
