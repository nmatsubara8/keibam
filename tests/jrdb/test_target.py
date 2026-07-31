"""JRDB TARGET 全8種パーサの単体テスト（STEP5）。実ファイル解析で確定した形式に準拠。"""
from __future__ import annotations

import pandas as pd

import datetime as dt

from src.jrdb._target import (
    NATURAL_KEYS,
    classify,
    complete_return_date,
    dedup_by_keys,
    kikyu_month_day,
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
    assert r0["gaikyu_name"] == "ノーザンFしがらき"          # NFKC 正規化（Ｆ→F）
    assert r0["gaikyu_name_raw"] == "ノーザンＦしがらき"      # 生値は保持
    assert r0["kikyu_date"] == "07/07"
    assert r0["interval_weeks"] == 4
    assert df.iloc[1]["interval_weeks"] == 0                 # 連闘=0週


def test_gaikyu_comment_empty_weeks_is_na():
    # 中週（週数空欄・年次で 12.9%）→ interval_weeks NA。前走なし=初出走等。
    df = parse_gaikyu_comment("0126120303,チャンピオンヒルズ　05/30　中週\r\n".encode("cp932"))
    assert pd.isna(df.iloc[0]["interval_weeks"])
    assert df.iloc[0]["gaikyu_name"] == "チャンピオンヒルズ"


def test_gaikyu_comment_missing_date_slash():
    # 帰厩日欠損 `外厩名␣/␣中N週`（年次6件）→ kikyu_date 空。
    df = parse_gaikyu_comment("0826140302,ノルマンディ小野町　/　中3週\r\n".encode("cp932"))
    assert df.iloc[0]["kikyu_date"] == ""
    assert df.iloc[0]["interval_weeks"] == 3


def test_gaikyu_comment_sentinel_names_to_na():
    # 情報無し(年次4件) / 全角数値 ９９．９(年次3件) → gaikyu_name NA だが生値は保持。
    data = ("0826110301,情報無し　/　中1週\r\n"
            "0226120506,９９．９　05/29　中週\r\n").encode("cp932")
    df = parse_gaikyu_comment(data)
    assert pd.isna(df.iloc[0]["gaikyu_name"]) and df.iloc[0]["gaikyu_name_raw"] == "情報無し"
    assert pd.isna(df.iloc[1]["gaikyu_name"]) and df.iloc[1]["gaikyu_name_raw"] == "９９．９"


def test_complete_return_date_year_crossing():
    # 出走1月・帰厩12月 → 前年（年跨ぎ）。通常は当年。
    assert complete_return_date("12/20", dt.date(2026, 1, 10)) == dt.date(2025, 12, 20)
    assert complete_return_date("07/01", dt.date(2026, 7, 26)) == dt.date(2026, 7, 1)
    assert complete_return_date("", dt.date(2026, 7, 26)) is None       # 欠損
    assert complete_return_date("02/30", dt.date(2026, 3, 1)) is None   # 異常日付
    assert kikyu_month_day("07/15") == (7, 15)
    assert kikyu_month_day("/") == (None, None)


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


def test_parse_rank_utf8_with_source_date():
    df = parse_rank("2,01048,05,畠山吉宏\n1,1181,12,秋山稔樹\n".encode("utf-8"),
                    "tnrank", source_date="20260726")
    assert df.loc[0, "area"] == 2 and df.loc[0, "person_code"] == "01048"
    assert df.loc[0, "rank"] == 5 and df.loc[0, "name"] == "畠山吉宏"
    assert df.loc[0, "kind"] == "tnrank" and df.loc[0, "source_date"] == "20260726"


def test_rank_timeseries_preserved_across_dates():
    # 同一人物が2日分 → (source_date, person_code) キーで両日残る（最新1日に collapse しない）。
    e1 = ("tnrank_20260725.zip", "tnrank.csv", b"2,01048,06,X")
    e2 = ("tnrank_20260726.zip", "tnrank.csv", b"2,01048,05,X")
    df = parse_target_bytes("tnrank", [e1, e2])
    out, dropped = dedup_by_keys(df, NATURAL_KEYS["tnrank"])
    assert dropped == 0 and len(out) == 2                      # 2日分とも残る
    assert set(out["source_date"]) == {"20260725", "20260726"}
    assert dict(zip(out.source_date, out["rank"], strict=True)) == {"20260725": 6, "20260726": 5}


def test_rank_same_date_redownload_dedups():
    # 同一日を2回（再DL・内容同一）→ (source_date, person_code) で1つに。
    e = ("tnrank_20260726.zip", "tnrank.csv", b"2,01048,05,X")
    out, dropped = dedup_by_keys(parse_target_bytes("tnrank", [e, e]), NATURAL_KEYS["tnrank"])
    assert dropped == 1 and len(out) == 1


def test_parse_target_bytes_concats_multiple_race_files():
    e1 = ("gaikyu_x/012612026.DAT", "23106073CH\r\n".encode("cp932"))
    e2 = ("gaikyu_x/012612036.DAT", "23107042NF\r\n".encode("cp932"))
    df = parse_target_bytes("gaikyu", [e1, e2])
    assert len(df) == 2
    assert set(df["race_id"]) == {"202601010202", "202601010203"}  # R02/R03


def test_dedup_by_keys_removes_dup_race_ketto_keep_last():
    # 同一 (race_id, ketto) が2回（再DL/年次×日次の重なり）→ 1つに、keep=last。
    df = pd.DataFrame({
        "race_id": ["r1", "r1", "r2"], "ketto": ["k1", "k1", "k2"],
        "gaikyu_code": ["CH", "NF", "天"],
    })
    out, dropped = dedup_by_keys(df, ["race_id", "ketto"])
    assert dropped == 1 and len(out) == 2
    assert out[(out.race_id == "r1")].iloc[0]["gaikyu_code"] == "NF"  # 後勝ち


def test_dedup_by_keys_noop_when_keys_missing_or_empty():
    df = pd.DataFrame({"a": [1, 1]})
    out, dropped = dedup_by_keys(df, ["race_id"])   # キー列が無い→素通し
    assert dropped == 0 and len(out) == 2
    out2, d2 = dedup_by_keys(pd.DataFrame(), ["race_id"])  # 空df→素通し
    assert d2 == 0 and out2.empty


def test_classify_ignores_redownload_copy_suffix():
    # 「gaikyu_20260726 (1).zip」も接頭辞 gaikyu に分類される（内容重複は CLI の sha1 で排除）。
    assert classify("gaikyu_20260726 (1).zip") == "gaikyu"


def _sed_record_with_idm(race_key: str, umaban: str, ketto: str, ymd: str, idm: str) -> bytes:
    """idm@183 を設定した合成 SED レコード（成績IDM×SED 照合テスト用）。"""
    r = bytearray(b" " * 376)

    def put(start1, s):
        b = s.encode("cp932")
        r[start1 - 1: start1 - 1 + len(b)] = b

    put(1, race_key); put(9, umaban); put(11, ketto); put(19, ymd); put(183, idm)
    return bytes(r) + b"\r\n"


def test_compare_seiseki_vs_sed_joins_and_matches(tmp_path):
    from src.jrdb._parser import parse
    from src.jrdb._target import compare_seiseki_vs_sed, parse_seiseki_idm

    # SED: race_key 01082101(→race_id 200801020101) 馬番07 idm=45 / 馬番08 idm=30
    sed_bytes = (
        _sed_record_with_idm("01082101", "07", "06102843", "20080913", " 45")
        + _sed_record_with_idm("01082101", "08", "06102844", "20080913", " 30")
    )
    sed_path = tmp_path / "SED080913.txt"
    sed_path.write_bytes(sed_bytes)
    sed = parse(str(sed_path), "SED")

    # idmse: 同 (race_id, umaban) を作る鍵 = YYYYMMDD+場01+回02+日01+R01+馬番
    idmse = parse_seiseki_idm(
        ("200809130102010107,45\r\n"   # 馬番07: SED と一致(45)
         "200809130102010108,31\r\n").encode("cp932")  # 馬番08: 30 vs 31 → 差1
    )
    rep = compare_seiseki_vs_sed(sed, idmse)
    assert rep["n_overlap_keys"] == 2
    assert rep["n_both_present"] == 2
    assert rep["max_abs_diff"] == 1.0            # 馬番08 の 1 差
    assert rep["exact_match_rate"] == 0.5        # 2件中1件一致
    assert rep["sed_range"] == [30.0, 45.0]
