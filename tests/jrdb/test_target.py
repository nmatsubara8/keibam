"""JRDB TARGET 全8種パーサの単体テスト（STEP5）。実ファイル解析で確定した形式に準拠。"""
from __future__ import annotations

import pandas as pd

from src.jrdb._target import (
    classify,
    parse_gaikyu_comment,
    parse_mark_file,
    parse_rank,
    parse_seiseki_idm,
    parse_target_bytes,
)


def test_classify_distinguishes_prefixes():
    assert classify("gaikyu_20260726.zip") == "gaikyu"
    assert classify("gaikyucomment_20260726.zip") == "gaikyucomment"  # gaikyu より優先
    assert classify("idm_20260726.zip") == "idm"
    assert classify("idmse_2026.zip") == "idmse"                       # idm と区別
    assert classify("itidori_20260726.zip") == "itidori"
    assert classify("bante_20260726.zip") == "bante"
    assert classify("tnrank_20260726.zip") == "tnrank"
    assert classify("jocrank_20260726.zip") == "jocrank"
    assert classify("unknown_x.zip") is None


def test_parse_mark_gaikyu_ketto_and_code():
    # 内包名 012612026.DAT: 場01 年26 回1 日2 R02 種別6 → race_id 復元。2byte コード(2半角/1全角)。
    data = "23106073CH\r\n24105886鈴\r\n".encode("cp932")
    df = parse_mark_file("gaikyu_20260726/012612026.DAT", data, "gaikyu")
    assert list(df.columns) == ["race_id", "ketto", "horse_id", "gaikyu_code"]
    assert df.loc[0, "race_id"] == "202601010202"          # 2026+場01+回01+日02+R02
    assert df.loc[0, "ketto"] == "23106073"
    assert df.loc[0, "horse_id"] == "2023106073"           # ketto→horse_id(生年4桁)
    assert df.loc[0, "gaikyu_code"] == "CH"
    assert df.loc[1, "gaikyu_code"] == "鈴"                 # 全角1文字も2byteコードとして取得


def test_parse_mark_bante_is_9bytes_1digit():
    data = b"241022904\r\n231058032\r\n"                    # 9B = ketto8 + 番手1桁
    df = parse_mark_file("bante_20260726/012612013.DAT", data, "bante")
    assert df.loc[0, "ketto"] == "24102290" and df.loc[0, "bante_code"] == "4"
    assert df.loc[1, "bante_code"] == "2"


def test_parse_gaikyu_comment_key_and_fields():
    data = ("0426220104,ノーザンＦしがらき　07/07　中4週\r\n"
            "0126120103,加藤ステーブル　07/15　連闘\r\n").encode("cp932")
    df = parse_gaikyu_comment(data)
    r0 = df.iloc[0]
    assert r0["race_id"] == "202604020201"                  # 場04 回02 日02 R01
    assert r0["umaban"] == 4
    assert r0["gaikyu_name"] == "ノーザンＦしがらき"
    assert r0["kikyu_date"] == "07/07"
    assert r0["interval_weeks"] == 4
    assert df.iloc[1]["interval_weeks"] == 0                 # 連闘=0週


def test_parse_seiseki_idm_signed_and_key():
    data = ("202601040601010101,9\r\n"
            "202601250601090102,-19\r\n"
            "202605020401010101,\r\n").encode("cp932")       # 空値=NA
    df = parse_seiseki_idm(data)
    r0 = df.iloc[0]
    assert r0["race_id"] == "202606010101"                  # 年2026+場06+回01+日01+R01
    assert r0["umaban"] == 1 and r0["seiseki_idm"] == 9
    assert df.iloc[1]["seiseki_idm"] == -19                  # 符号付き
    assert pd.isna(df.iloc[2]["seiseki_idm"])               # 空値は NA


def test_parse_rank_utf8():
    df = parse_rank("2,01048,05,畠山吉宏\n1,1181,12,秋山稔樹\n".encode("utf-8"), "tnrank")
    assert df.loc[0, "area"] == 2 and df.loc[0, "person_code"] == "01048"
    assert df.loc[0, "rank"] == 5 and df.loc[0, "name"] == "畠山吉宏"
    assert df.loc[0, "kind"] == "tnrank"


def test_parse_target_bytes_concats_multiple_race_files():
    e1 = ("gaikyu_x/012612026.DAT", "23106073CH\r\n".encode("cp932"))
    e2 = ("gaikyu_x/012612036.DAT", "23107042NF\r\n".encode("cp932"))
    df = parse_target_bytes("gaikyu", [e1, e2])
    assert len(df) == 2
    assert set(df["race_id"]) == {"202601010202", "202601010203"}  # R02/R03
