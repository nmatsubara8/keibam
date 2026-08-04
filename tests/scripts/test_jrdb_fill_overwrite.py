"""jrdb_fill_netkeiba の overwrite 合成（drop→concat→keep-last dedup）の統合テスト。

DB 非依存で、上書きモードの中核（対象 JRA race_id を JRDB 行で置換し NAR を保護、
連結後に主キー重複を掃除、netkeiba raw の RangeIndex+列 構造を保つ）を検証する。
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd

from src.jrdb._fill import drop_race_ids, to_raw_shape

_MOD_PATH = Path(__file__).resolve().parents[2] / "scripts" / "jrdb_fill_netkeiba.py"
_spec = importlib.util.spec_from_file_location("jrdb_fill_netkeiba", _MOD_PATH)
fill = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fill)


def _overwrite_merge(existing, jrdb_new, name):
    """main() の overwrite apply 部と同じ合成を再現する。"""
    drop_ids = set(jrdb_new.index.astype(str))          # JRDB が持つ race_id
    ex = drop_race_ids(existing, drop_ids)
    ex = to_raw_shape(ex)
    new_df = to_raw_shape(jrdb_new)
    merged = pd.concat([ex, new_df], ignore_index=True)
    merged, n_dup = fill._dedup_keep_last(merged, name)
    return merged, n_dup


def test_overwrite_replaces_broken_jra_and_protects_nar():
    # 既存 netkeiba(results): JRA1 は壊れて1頭だけ、NAR は別 race_id。index=race_id。
    existing = pd.DataFrame(
        {"馬番": [1, 1], "単勝": [3.0, 9.9]},
        index=pd.Index(["JRA1", "NAR9"], name="race_id"),
    )
    # JRDB: JRA1 を全頭（3頭）で持つ。index=race_id。
    jrdb = pd.DataFrame(
        {"馬番": [1, 2, 3], "単勝": [2.0, 4.0, 6.0]},
        index=pd.Index(["JRA1", "JRA1", "JRA1"], name="race_id"),
    )
    merged, n_dup = _overwrite_merge(existing, jrdb, "results")
    assert n_dup == 0
    # JRA1 は JRDB の3頭に置換（壊れた netkeiba 1頭は消える）
    jra1 = merged[merged["race_id"] == "JRA1"].sort_values("馬番")
    assert jra1["馬番"].tolist() == [1, 2, 3]
    assert jra1["単勝"].tolist() == [2.0, 4.0, 6.0]     # netkeiba の 3.0 は置換された
    # NAR は保護（残る）
    assert (merged["race_id"] == "NAR9").sum() == 1
    # 構造: race_id は列・RangeIndex
    assert merged.index.name is None and "race_id" in merged.columns


def test_overwrite_dedup_cleans_preexisting_duplicates():
    # 既存に (RACE,馬番) 重複が紛れていても keep-last で掃除される（対象外 race）。
    existing = pd.DataFrame(
        {"race_id": ["OTHER", "OTHER", "JRA1"], "馬番": [1, 1, 1], "単勝": [5.0, 7.0, 9.0]},
    )
    jrdb = pd.DataFrame({"馬番": [1, 2]}, index=pd.Index(["JRA1", "JRA1"], name="race_id"))
    merged, n_dup = _overwrite_merge(existing, jrdb, "results")
    assert n_dup == 1                                   # OTHER の重複1件を除去
    other = merged[merged["race_id"] == "OTHER"]
    assert len(other) == 1 and other.iloc[0]["単勝"] == 7.0   # keep-last
