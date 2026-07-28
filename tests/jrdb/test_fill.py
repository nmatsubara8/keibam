"""JRDB→netkeiba fill 組み立ての単体テスト（fill 方針・新規抽出）。"""
from __future__ import annotations

import pandas as pd

from src.jrdb._fill import (
    FILL_RACE_INFO_KEEP,
    build_fill_tables,
    filter_years,
    new_by_race_id,
    new_horse_results,
)


def _sed(rid, umaban="1", ketto="18103588", ymd="20220614"):
    return {"race_id": rid, "umaban": umaban, "ketto": ketto, "ymd": ymd,
            "kyori": "1800", "shiba_dirt": "1", "tenko_code": "1", "baba_state": "10",
            "chakujun": "1", "ijo_kubun": "0", "shubetsu": "13", "joken": "A3",
            "kishu_name": "騎手", "kakutei_tansho": " 3.5", "kakutei_ninki": "1"}


def test_fill_policy_nulls_recent_only_race_info():
    sed = pd.DataFrame([_sed("202205020201")])
    t = build_fill_tables(sed)
    ri = t["race_info"]
    # 全年充填の列は値が入る
    assert ri.iloc[0]["race_type"] == "芝" and ri.iloc[0]["weather"] == "晴"
    assert ri.iloc[0]["ground_state1"] == "良"
    # recent-only 列は NaN 化（fill 方針）
    for c in ["place", "around", "time", "age", "race_class"]:
        if c in ri.columns:
            assert pd.isna(ri.iloc[0][c]), c
    # keep 集合の妥当性
    assert set(FILL_RACE_INFO_KEEP) <= set(ri.columns)


def test_filter_years_and_new_by_race_id():
    sed = pd.DataFrame([_sed("202205020201"), _sed("202105020202"),
                        _sed("201805020203")])
    res = build_fill_tables(sed)["results"]
    y = filter_years(res, ["2021", "2022"])
    assert set(y.index.map(lambda r: r[:4])) == {"2021", "2022"}   # 2018 除外
    # 既存に 2022 の race_id があれば除外される
    n = new_by_race_id(y, existing_race_ids=["202205020201"])
    assert "202205020201" not in set(n.index) and "202105020202" in set(n.index)


def test_new_horse_results_dedup_by_key():
    sed = pd.DataFrame([_sed("202205020201", ketto="18103588", ymd="20220614"),
                        _sed("202205020301", ketto="18103599", ymd="20220615")])
    hr = build_fill_tables(sed)["horse_results"]
    assert len(hr) == 2
    # 18103588→2018103588 の 2022/06/14 が既存なら除外
    existing = {("2018103588", "2022/06/14")}
    n = new_horse_results(hr, existing)
    assert len(n) == 1 and n.iloc[0]["horse_id"] == "2018103599"


def test_minimal_false_keeps_all():
    sed = pd.DataFrame([_sed("202205020201")])
    ri = build_fill_tables(sed, minimal_race_info=False)["race_info"]
    # 方針を切ると age/race_class も残る
    assert ri.iloc[0]["age"] == "3" and ri.iloc[0]["race_class"] == "未勝利"
