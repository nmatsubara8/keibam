"""src/pipeline/_duplicate_audit.py: raw 主キー重複点検のテスト。"""

from __future__ import annotations

import pandas as pd

from src.pipeline._duplicate_audit import audit_all_raw
from src.pipeline._duplicate_audit import audit_dataframe
from src.pipeline._duplicate_audit import audit_results_horse_id
from src.pipeline._duplicate_audit import count_pk_extras
from src.pipeline._doctor import ERROR
from src.pipeline._doctor import OK
from src.pipeline._doctor import check_raw_duplicates


def test_count_pk_extras_clean():
    df = pd.DataFrame({"race_id": ["a", "b"], "馬番": [1, 1]})
    n, samples = count_pk_extras(df, ("race_id", "馬番"))
    assert n == 0
    assert samples == ()


def test_count_pk_extras_finds_dupes():
    df = pd.DataFrame({
        "race_id": ["a", "a", "b"],
        "馬番": [1, 1, 2],
        "着順": [1, 2, 1],
    })
    n, samples = count_pk_extras(df, ("race_id", "馬番"))
    assert n == 1
    assert any("a|1" == s for s in samples)


def test_audit_dataframe_race_info_dup():
    df = pd.DataFrame({"race_id": ["r1", "r1", "r2"], "天候": ["晴", "雨", "曇"]})
    df = df.set_index("race_id")
    # index が race_id のとき列にも残す形式と、index のみ形式の両方を許容
    rep = audit_dataframe("raw_race_info", df)
    assert rep.n_extra == 1
    assert rep.has_duplicates


def test_audit_dataframe_horse_info_ok():
    df = pd.DataFrame({"horse_id": ["h1", "h2"], "性齢": ["牡3", "牝4"]})
    rep = audit_dataframe("raw_horse_info", df)
    assert not rep.skipped
    assert rep.n_extra == 0


def test_audit_dataframe_person_yearly_composite_pk():
    df = pd.DataFrame({
        "entity_type": ["jockey", "jockey", "trainer"],
        "entity_id": ["00100", "00100", "00200"],
        "year": [2024, 2024, 2024],
        "wins": [10, 11, 5],
    })
    rep = audit_dataframe("raw_person_yearly", df)
    assert rep.n_extra == 1


def test_audit_return_tables_full_row_when_no_row_idx():
    """row_idx 無し pickle は全列一致の完全重複のみ検出する。"""
    df = pd.DataFrame({
        "race_id": ["r1", "r1", "r1"],
        "券種": ["単勝", "単勝", "複勝"],
        "払戻": [100, 100, 200],
    })
    rep = audit_dataframe("raw_return_tables", df)
    assert not rep.skipped
    assert rep.n_extra == 1  # 最初の2行が完全一致


def test_audit_results_horse_id():
    df = pd.DataFrame({
        "race_id": ["r1", "r1", "r1"],
        "馬番": [1, 2, 3],
        "horse_id": ["h1", "h1", "h2"],
    })
    # 馬番 PK では重複なし、horse_id 軸では重複
    assert audit_dataframe("raw_results", df).n_extra == 0
    assert audit_results_horse_id(df).n_extra == 1


def test_check_raw_duplicates_reports_error(tmp_path):
    path = tmp_path / "race_info.pkl"
    pd.DataFrame({"race_id": ["r1", "r1"], "x": [1, 2]}).to_pickle(path)
    # 他 alias はファイルなし → OK スキップ
    overrides = {"raw_race_info": str(path)}
    # audit_all_raw は全 alias を見るので、overrides 以外は LocalPaths（大抵なし→OK）
    results = check_raw_duplicates(path_overrides=overrides)
    by_name = {r.name: r for r in results}
    assert by_name["dup.raw_race_info"].level == ERROR
    assert "重複" in by_name["dup.raw_race_info"].detail


def test_audit_all_raw_missing_is_skipped(tmp_path):
    aliases = ("raw_race_info", "raw_horse_info")
    reports = audit_all_raw(
        aliases,
        include_results_horse_id=False,
        path_overrides={a: str(tmp_path / f"{a}.pkl") for a in aliases},
    )
    assert all(r.skipped for r in reports)
    assert all(r.skip_reason == "ファイルなし" for r in reports)


def test_check_raw_duplicates_ok_clean(tmp_path):
    path = tmp_path / "horse_info.pkl"
    pd.DataFrame({"horse_id": ["h1", "h2"], "x": [1, 2]}).to_pickle(path)
    results = check_raw_duplicates(path_overrides={"raw_horse_info": str(path)})
    hit = next(r for r in results if r.name == "dup.raw_horse_info")
    assert hit.level == OK
    assert "重複なし" in hit.detail
