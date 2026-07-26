"""アーカイブ CSV→results/race_info 変換ロジックの単体テスト。

実データ無しでも変換の正しさ（列名・性齢合成・馬体重形式・race_id 同型・通過結合・
race_info の date 形式）を固定する。ユーザ提供の実カラム名に準拠した合成 CSV を使う。
"""
from __future__ import annotations

import pandas as pd
import pytest

from scripts.import_archive_race_result import csv_to_race_info, csv_to_results


def _sample_csv() -> pd.DataFrame:
    # ユーザの実ヘッダの部分集合（変換に使う列のみ）
    return pd.DataFrame(
        {
            "レースID": ["198601010101", "198601010101", "202001010101"],
            "レース日付": ["1986-06-07", "1986-06-07", "2020-01-05"],
            "競馬場コード": ["1", "1", "6"],
            "距離(m)": ["1500", "1500", "2000"],
            "芝・ダート区分": ["ダート", "ダート", "芝"],
            "障害区分": [None, None, None],
            "馬場状態1": ["良", "良", "稍重"],
            "馬場状態2": [None, None, None],
            "天候": ["晴", "晴", "曇"],
            "着順": ["1", "2", "1"],
            "枠番": ["2", "3", "5"],
            "馬番": ["2", "3", "8"],
            "馬名": ["ワクセイ", "マツタカラオー", "ワクセイ"],
            "性別": ["牡", "牡", "牝"],
            "馬齢": ["4", "4", "3"],
            "斤量": ["55", "55", "54"],
            "騎手": ["柏崎正次", "国兼正浩", "武豊"],
            "タイム": ["1:34.3", "1:36.0", "2:01.1"],
            "着差": [None, "大", "クビ"],
            "1コーナー": [None, None, "5"],
            "2コーナー": ["7", "4", "5"],
            "3コーナー": ["3", "3", "3"],
            "4コーナー": ["1", "4", "1"],
            "上り": ["38.6", "40.3", "34.6"],
            "単勝": ["2.1", "7.0", "3.2"],
            "人気": ["1", "4", "2"],
            "馬体重": ["468", "430", "476"],
            "場体重増減": ["0", "4", "-4"],
            "調教師": ["宮沢今朝", "斎藤籌敬", "友道康夫"],
            "馬主": ["A", "B", "C"],
            "賞金(万円)": ["290", "120", "580"],
        }
    )


def test_results_core_columns_and_composites():
    r = csv_to_results(_sample_csv())
    # race_id は現行と同型（12桁文字列）
    assert list(r["race_id"]) == ["198601010101", "198601010101", "202001010101"]
    # 性齢は 性別+馬齢 の合成
    assert list(r["性齢"]) == ["牡4", "牡4", "牝3"]
    # 馬体重は "468(0)" 形式（増減の符号を保持）
    assert list(r["馬体重"]) == ["468(0)", "430(4)", "476(-4)"]
    # 通過は 1–4 コーナーを "-" 結合（NA 除外）
    assert list(r["通過"]) == ["7-3-1", "4-3-4", "5-5-3-1"]
    # manji 決済に必須の列がそのまま入る
    assert list(r["単勝"]) == ["2.1", "7.0", "3.2"]
    assert list(r["着順"]) == ["1", "2", "1"]
    assert list(r["馬番"]) == ["2", "3", "8"]


def test_results_schema_matches_pipeline():
    r = csv_to_results(_sample_csv())
    expected = ["race_id", "着順", "枠番", "馬番", "馬名", "性齢", "斤量", "騎手", "タイム",
                "着差", "ﾀｲﾑ指数", "通過", "上り", "単勝", "人気", "馬体重", "調教ﾀｲﾑ",
                "厩舎ｺﾒﾝﾄ", "備考", "調教師", "馬主", "賞金(万円)", "horse_id",
                "jockey_id", "trainer_id", "owner_id"]
    assert list(r.columns) == expected


def test_surrogate_horse_id_stable_and_nonclashing():
    r = csv_to_results(_sample_csv())
    hid = r["horse_id"]
    # 同名（ワクセイ）は同一 ID
    assert hid.iloc[0] == hid.iloc[2]
    # 実 netkeiba ID 帯（<3e9）と衝突しない高位帯
    assert (hid.dropna() >= 9_000_000_000).all()


def test_race_info_dedup_and_date_format():
    ri = csv_to_race_info(_sample_csv())
    # レース単位に重複排除（2レース）
    assert len(ri) == 2
    assert set(ri["race_id"]) == {"198601010101", "202001010101"}
    # RaceInfoProcessor がパースできる "%Y年%m月%d日" 形式
    d = dict(zip(ri["race_id"], ri["date"]))
    assert d["198601010101"] == "1986年06月07日"
    assert pd.to_datetime(ri["date"], format="%Y年%m月%d日").notna().all()
    # course_len は生メートル（processor 側で //10 する）
    assert list(pd.to_numeric(ri["course_len"])) == [1500, 2000]


def test_race_info_required_columns_present():
    ri = csv_to_race_info(_sample_csv())
    for col in ("race_id", "course_len", "date", "place_id", "age", "sex",
                "race_type", "ground_state1", "ground_state2"):
        assert col in ri.columns


def test_race_type_hurdle_override():
    df = _sample_csv()
    df.loc[0, "障害区分"] = "障害"
    ri = csv_to_race_info(df)
    rt = dict(zip(ri["race_id"], ri["race_type"]))
    assert rt["198601010101"] == "障害"     # 障害区分ありは race_type=障害
    assert rt["202001010101"] == "芝"
