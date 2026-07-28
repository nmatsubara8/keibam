"""JRDB↔netkeiba クロスウォーク（同一性橋渡し）の単体テスト。

重複年の (race_id, 馬番) 突合で 血統登録番号↔horse_id 等が対応づき、多対一の揺れは
多数決で解決され、confidence が付くことを合成データで検証する。
"""
from __future__ import annotations

import pandas as pd

from src.jrdb._crosswalk import build_crosswalk, coverage, read_crosswalk, save_crosswalk


def _nk(rows):
    """netkeiba raw_results 風（race_id を index に持たせる）。"""
    df = pd.DataFrame(rows).set_index("race_id")
    return df


def _jr(rows):
    return pd.DataFrame(rows)


def test_basic_mapping_by_race_and_umaban():
    nk = _nk([
        {"race_id": "202001010101", "馬番": 1, "horse_id": "7000001",
         "jockey_id": "j01", "trainer_id": "t01"},
        {"race_id": "202001010101", "馬番": 2, "horse_id": "7000002",
         "jockey_id": "j02", "trainer_id": "t01"},
    ])
    jr = _jr([
        {"race_id": "202001010101", "umaban": 1, "ketto": "13103588",
         "kishu_code": "01001", "chokyo_code": "05001"},
        {"race_id": "202001010101", "umaban": 2, "ketto": "13103599",
         "kishu_code": "01002", "chokyo_code": "05001"},
    ])
    out = build_crosswalk(nk, jr)
    horse = out["horse"].set_index("ketto")
    assert horse.loc["13103588", "horse_id"] == "7000001"
    assert horse.loc["13103599", "horse_id"] == "7000002"
    assert horse.loc["13103588", "confidence"] == 1.0
    # 騎手・調教師も対応
    assert out["jockey"].set_index("kishu_code").loc["01001", "jockey_id"] == "j01"
    tr = out["trainer"].set_index("chokyo_code")
    assert tr.loc["05001", "trainer_id"] == "t01" and tr.loc["05001", "support"] == 2


def test_majority_resolves_conflict():
    """同じ ketto が2レースで別 horse_id に当たっても、多数決で1つへ収束する。"""
    nk = _nk([
        {"race_id": "R1", "馬番": 1, "horse_id": "HORSE_A"},
        {"race_id": "R2", "馬番": 1, "horse_id": "HORSE_A"},
        {"race_id": "R3", "馬番": 1, "horse_id": "HORSE_B"},  # 少数派（誤突合の想定）
    ])
    jr = _jr([
        {"race_id": "R1", "umaban": 1, "ketto": "K"},
        {"race_id": "R2", "umaban": 1, "ketto": "K"},
        {"race_id": "R3", "umaban": 1, "ketto": "K"},
    ])
    out = build_crosswalk(nk, jr)
    row = out["horse"].iloc[0]
    assert row["ketto"] == "K" and row["horse_id"] == "HORSE_A"  # 2票 > 1票
    assert row["support"] == 2 and row["total"] == 3
    assert abs(row["confidence"] - 2 / 3) < 1e-9
    rep = coverage(out)                       # 2/3=0.667 < 0.8 → low_conf
    assert rep["horse"]["mapped"] == 1 and rep["horse"]["low_conf"] == 1
    assert abs(rep["horse"]["mean_confidence"] - 2 / 3) < 1e-3


def test_no_overlap_returns_empty():
    nk = _nk([{"race_id": "R1", "馬番": 1, "horse_id": "H"}])
    jr = _jr([{"race_id": "R2", "umaban": 1, "ketto": "K"}])  # race_id 不一致
    out = build_crosswalk(nk, jr)
    assert out["horse"].empty


def test_blank_codes_ignored():
    nk = _nk([{"race_id": "R1", "馬番": 1, "horse_id": ""}])   # 空 ID は対応にしない
    jr = _jr([{"race_id": "R1", "umaban": 1, "ketto": "K"}])
    out = build_crosswalk(nk, jr)
    assert out["horse"].empty


def test_save_and_read_roundtrip(tmp_path):
    nk = _nk([{"race_id": "R1", "馬番": 1, "horse_id": "H1", "jockey_id": "J1",
               "trainer_id": "T1"}])
    jr = _jr([{"race_id": "R1", "umaban": 1, "ketto": "K1", "kishu_code": "C1",
               "chokyo_code": "D1"}])
    out = build_crosswalk(nk, jr)
    db = str(tmp_path / "xwalk.db")
    save_crosswalk(out, db_path=db)
    got = read_crosswalk("horse", db_path=db).set_index("ketto")
    assert got.loc["K1", "horse_id"] == "H1"
    # 主キーは JRDB コード → 再保存しても重複しない（keep-last）
    save_crosswalk(out, db_path=db)
    assert len(read_crosswalk("horse", db_path=db)) == 1
