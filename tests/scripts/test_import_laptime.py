"""laptime CSV → race_pace 変換の単体テスト。"""
from __future__ import annotations

import pandas as pd

from scripts.import_archive_laptime import csv_to_race_pace


def _sample():
    return pd.DataFrame({
        "レースID": ["198601010101", "198601010101", "198601010102"],
        "前半3ハロン": ["34.0", "34.0", "36.5"],
        "上がり3ハロン": ["36.5", "36.5", "35.0"],   # R1: 上-前=+2.5→前傾 / R2: -1.5→後傾
        "ラップタイム1": ["12.0", "12.0", "12.5"],
        "ラップタイム2": ["11.5", "11.5", None],
        "ラップタイム3": [None, None, None],
    })


def test_race_pace_dedup_and_diff():
    p = csv_to_race_pace(_sample())
    assert len(p) == 2                                # race 単位
    d = dict(zip(p["race_id"], p["pace_diff"]))
    assert d["198601010101"] == 2.5                   # 36.5 - 34.0
    assert d["198601010102"] == -1.5


def test_pace_type_labels():
    p = csv_to_race_pace(_sample())
    t = dict(zip(p["race_id"], p["pace_type"]))
    assert t["198601010101"] == "前傾"                # +2.5 > 0.8
    assert t["198601010102"] == "後傾"                # -1.5 < -0.8


def test_n_laps_counts_present():
    p = csv_to_race_pace(_sample())
    nl = dict(zip(p["race_id"], p["n_laps"]))
    assert nl["198601010101"] == 2                    # ラップ1,2 有効
    assert nl["198601010102"] == 1                    # ラップ1のみ


def test_schema():
    p = csv_to_race_pace(_sample())
    assert list(p.columns) == ["race_id", "zenhan_3f", "agari_3f", "pace_diff", "pace_type", "n_laps"]
