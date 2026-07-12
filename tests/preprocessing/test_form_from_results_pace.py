"""build_horse_results_from_results の脚質配線テスト。

アーカイブ由来 results（通過あり・n_horses なし）から horse_results を再構成すると、
first_corner と頭数(=race_id 出走数)が付与され、add_pace_stats が leg_type_binary を
出せることを固定する（歴史馬でも展開×脚質が効かせられる根拠）。
"""
from __future__ import annotations

import pandas as pd

from src.constants._horse_results_cols import HorseResultsCols as HRCols
from src.preprocessing._horse_features import (
    add_pace_stats,
    build_horse_results_from_results,
)


def _results():
    # 2レース×同一馬H1の過去2走。通過あり、n_horses 列なし（アーカイブ想定）。
    df = pd.DataFrame(
        {
            "horse_id": ["H1", "H2", "H3", "H1", "H2", "H3"],
            "date": ["2000-01-01"] * 3 + ["2000-02-01"] * 3,
            "着順": [1, 2, 3, 4, 1, 2],
            "通過": ["1-1", "5-5", "8-8", "1-1", "6-6", "9-9"],  # H1=前(先行), H3=後(追込)
            "course_len": [1600] * 6,
        },
        index=["R1", "R1", "R1", "R2", "R2", "R2"],  # index=race_id
    )
    df.index.name = "race_id"
    return df


def test_reconstruct_adds_first_corner_and_nhorses():
    hr = build_horse_results_from_results(_results())
    assert "first_corner" in hr.columns
    assert HRCols.N_HORSES in hr.columns
    # 頭数は race_id ごとの出走数=3
    assert (pd.to_numeric(hr[HRCols.N_HORSES]) == 3).all()
    # H1 の first_corner は 1（"1-1" と "2-1" の先頭）
    h1 = hr[hr.index == "H1"]
    assert sorted(pd.to_numeric(h1["first_corner"]).tolist()) == [1, 1]


def test_add_pace_stats_yields_leg_type():
    hr = build_horse_results_from_results(_results())
    # 現行レース（3頭）に leg_type を付与
    cur = pd.DataFrame(
        {"horse_id": ["H1", "H3"], "course_len": [1600, 1600]},
        index=["R9", "R9"],
    )
    out = add_pace_stats(cur, hr)
    assert "leg_type_binary" in out.columns
    lt = dict(zip(out["horse_id"], pd.to_numeric(out["leg_type_binary"], errors="coerce")))
    # H1: first_corner≈1-2 / 3頭 → _pace_num<0.5 → 前(0.0)
    assert lt["H1"] == 0.0
    # H3: first_corner≈8-9 / 3頭 → _pace_num>=0.5 → 後(1.0)
    assert lt["H3"] == 1.0


def test_no_corner_column_is_noop():
    df = _results().drop(columns=["通過"])
    hr = build_horse_results_from_results(df)
    assert "first_corner" not in hr.columns   # 通過なしなら付けない（従来動作）


def test_reconstruct_handles_japanese_date_format():
    # アーカイブ取込 results は date が "YYYY年MM月DD日" の文字列。書式無しパースだと
    # 全 NaT→dropna で 0 行になり脚質が全馬同値に潰れる回帰を固定する。
    df = _results().copy()
    df["date"] = ["2000年01月01日"] * 3 + ["2000年02月01日"] * 3
    hr = build_horse_results_from_results(df)
    assert not hr.empty, "日本語形式 date で reconstruction が空になってはいけない"
    assert hr["date"].notna().all()          # date が datetime として救済されている
    assert pd.to_numeric(hr["first_corner"], errors="coerce").notna().any()
    # 脚質が変動する（H1=先行 0, H3=追込 1 の両方が出る＝定数化しない）
    hr2 = add_pace_stats(df.copy(), hr)
    lt = pd.to_numeric(hr2["leg_type_binary"], errors="coerce").dropna()
    assert lt.nunique() >= 2, f"leg_type_binary が定数化: uniq={lt.nunique()}"
