"""dedup_raw_pickle.dedup_frame の単体テスト。

keep-last の正しさ（後着が残る）と、元の index 構造（named index / RangeIndex）の
保存を、raw_results（index_col=race_id）と raw_horse_results（index_col=None）で検証する。
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd

_MOD_PATH = Path(__file__).resolve().parents[2] / "scripts" / "dedup_raw_pickle.py"
_spec = importlib.util.spec_from_file_location("dedup_raw_pickle", _MOD_PATH)
dedup = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(dedup)


def test_dedup_results_keeps_last_and_named_index():
    # raw_results: index=race_id, 馬番 は列。(race_id,馬番)=(R1,1) が重複→後着 val=99 が残る。
    df = pd.DataFrame(
        {"馬番": [1, 2, 1], "val": [10, 20, 99]},
        index=pd.Index(["R1", "R1", "R1"], name="race_id"),
    )
    out, removed, pk = dedup.dedup_frame(df, "raw_results")
    assert removed == 1
    assert tuple(pk) == ("race_id", "馬番")
    assert out.index.name == "race_id"           # 構造保存: race_id は index のまま
    # (R1,1) は keep-last で val=99
    got = out.reset_index()
    v = got[(got["race_id"] == "R1") & (got["馬番"] == 1)]["val"].tolist()
    assert v == [99]
    assert len(out) == 2


def test_dedup_results_race_id_as_column_rangeindex():
    # race_id が通常列・RangeIndex のケース（fill が to_raw_shape で作る構造）でも動く。
    df = pd.DataFrame({"race_id": ["R1", "R1", "R1"], "馬番": [1, 2, 1],
                       "val": [10, 20, 99]})
    out, removed, _ = dedup.dedup_frame(df, "raw_results")
    assert removed == 1
    # 元が RangeIndex（index 名なし）なので RangeIndex を維持
    assert out.index.name is None
    v = out[(out["race_id"] == "R1") & (out["馬番"] == 1)]["val"].tolist()
    assert v == [99]


def test_dedup_horse_results_rangeindex_keep_last():
    # raw_horse_results: index_col=None, PK=(horse_id,日付)。
    df = pd.DataFrame({
        "horse_id": ["H1", "H1", "H2"], "日付": ["2022-01-01", "2022-01-01", "2022-01-02"],
        "着順": [3, 1, 5],
    })
    out, removed, pk = dedup.dedup_frame(df, "raw_horse_results")
    assert removed == 1 and tuple(pk) == ("horse_id", "日付")
    assert out.index.name is None
    v = out[(out["horse_id"] == "H1")]["着順"].tolist()
    assert v == [1]      # keep-last


def test_dedup_no_duplicates_is_noop():
    df = pd.DataFrame({"馬番": [1, 2], "val": [10, 20]},
                      index=pd.Index(["R1", "R1"], name="race_id"))
    out, removed, _ = dedup.dedup_frame(df, "raw_results")
    assert removed == 0 and len(out) == 2
