"""JRDB パーサの単体テスト。合成した固定長レコードで offset/変換/特記展開を検証。"""
from __future__ import annotations

import pandas as pd

from src.jrdb._parser import parse


def _put(buf: bytearray, start1: int, s: str):
    b = s.encode("cp932")
    buf[start1 - 1: start1 - 1 + len(b)] = b


def _kyi_record() -> bytes:
    r = bytearray(b" " * 1024)
    _put(r, 1, "02152201")     # race_key: 場02 年15 回2 日2 R01
    _put(r, 9, "01")           # 馬番
    _put(r, 11, "13103588")    # 血統登録番号
    _put(r, 19, "テスト馬")     # 馬名
    _put(r, 55, " 45.0")       # IDM
    _put(r, 96, " 12.3")       # 基準オッズ
    _put(r, 101, " 3")         # 基準人気
    # 第11版で追加した指数群（展開予想・休養明け等）— spec 位置での照合用
    _put(r, 359, "120.5")      # テン指数
    _put(r, 364, "118.0")      # ペース指数
    _put(r, 379, "H")          # ペース予想 H/M/S
    _put(r, 570, " 21")        # 入厩何日前
    return bytes(r) + b"\r\n"


def _sed_record() -> bytes:
    r = bytearray(b" " * 376)
    _put(r, 1, "01082101")     # 場01 年08 回2 日1 R01
    _put(r, 9, "07")           # 馬番
    _put(r, 11, "06102843")
    _put(r, 19, "20080913")    # 年月日
    _put(r, 141, " 3")         # 着順
    _put(r, 195, "  5")        # 出遅
    _put(r, 201, "  8")        # 不利
    return bytes(r) + b"\r\n"


def _skb_record() -> bytes:
    r = bytearray(b" " * 304)
    _put(r, 1, "01022201")
    _put(r, 9, "01")
    _put(r, 11, "99101712")
    _put(r, 19, "20020908")
    _put(r, 27, "387332")      # 特記1=387(不利), 特記2=332
    return bytes(r) + b"\r\n"


def _cyb_record() -> bytes:
    r = bytearray(b" " * 94)   # データ 94 + CRLF 2 = レコード長 96
    _put(r, 1, "02152201")     # race_key -> 201502020201
    _put(r, 9, "03")           # 馬番
    _put(r, 14, "01")          # 坂路 有
    _put(r, 20, "01")          # 芝 有
    _put(r, 30, "58 ")         # 追切指数（ZZ9・左詰め既知バグを模擬）
    _put(r, 33, " 62")         # 仕上指数
    _put(r, 36, "A")           # 調教量評価
    _put(r, 86, "1")           # 調教評価 ◎
    _put(r, 87, "55 ")         # 一週前追切指数
    return bytes(r) + b"\r\n"


def _cha_record() -> bytes:
    r = bytearray(b" " * 62)   # データ 62 + CRLF 2 = レコード長 64
    _put(r, 1, "02152201")     # race_key -> 201502020201
    _put(r, 9, "05")           # 馬番
    _put(r, 11, "水")          # 曜日
    _put(r, 13, "20150710")    # 調教年月日
    _put(r, 24, "1")           # 追切種類 一杯
    _put(r, 29, "135")         # テンＦ
    _put(r, 35, "118")         # 終いＦ
    _put(r, 47, " 62")         # 追切指数
    _put(r, 50, "1")           # 併せ結果 先着
    return bytes(r) + b"\r\n"


def test_parse_cha(tmp_path):
    p = tmp_path / "CHA080913.txt"
    p.write_bytes(_cha_record())
    df = parse(str(p), "CHA")
    assert df.loc[0, "race_id"] == "201502020201"
    assert df.loc[0, "umaban"] == 5
    assert df.loc[0, "youbi"] == "水"
    assert df.loc[0, "chokyo_ymd"] == "20150710"
    # 部分別ハロンタイム・指数（数値化）
    assert df.loc[0, "ten_f"] == 135
    assert df.loc[0, "shimai_f"] == 118
    assert df.loc[0, "oikiri_idx"] == 62
    assert df.loc[0, "oikiri_shurui"] == 1
    assert df.loc[0, "awase_kekka"] == "1"


def test_parse_cyb(tmp_path):
    p = tmp_path / "CYB080913.txt"
    p.write_bytes(_cyb_record())
    df = parse(str(p), "CYB")
    assert df.loc[0, "race_id"] == "201502020201"
    assert df.loc[0, "umaban"] == 3
    # 主要な調教シグナル（数値化・左詰めバグは strip で吸収）
    assert df.loc[0, "oikiri_idx"] == 58
    assert df.loc[0, "shiage_idx"] == 62
    assert df.loc[0, "isshumae_oikiri_idx"] == 55
    # コード系（文字列のまま）
    assert df.loc[0, "chokyo_ryo_hyoka"] == "A"
    assert df.loc[0, "chokyo_hyoka"] == "1"
    assert df.loc[0, "course_saka"] == 1 and df.loc[0, "course_shiba"] == 1


def test_parse_kyi(tmp_path):
    p = tmp_path / "KYI150712.txt"
    p.write_bytes(_kyi_record())
    df = parse(str(p), "KYI")
    assert df.loc[0, "race_id"] == "201502020201"
    assert df.loc[0, "umaban"] == 1
    assert df.loc[0, "ketto"] == "13103588"
    assert df.loc[0, "bamei"] == "テスト馬"
    assert df.loc[0, "idm"] == 45.0
    assert df.loc[0, "kijun_odds"] == 12.3
    # 展開予想（P(z) の外部教師）と休養明けの新規フィールド
    assert df.loc[0, "ten_idx"] == 120.5
    assert df.loc[0, "pace_idx"] == 118.0
    assert df.loc[0, "pace_yosou"] == "H"
    assert df.loc[0, "nyukyu_days"] == 21


def test_build_kyi_feature_columns(tmp_path):
    """build_kyi が jrdb_ 指数群と pace_hms 数値化を出すこと。"""
    from src.jrdb._augment import JRDB_COLS, build_kyi

    p = tmp_path / "KYI150712.txt"
    p.write_bytes(_kyi_record())
    k = build_kyi([str(p)])
    assert k.loc[0, "jrdb_idm"] == 45.0
    assert k.loc[0, "jrdb_ten_idx"] == 120.5
    assert k.loc[0, "jrdb_pace_hms"] == 1.0     # H → +1
    assert k.loc[0, "jrdb_nyukyu_days"] == 21
    # JRDB_COLS の指数が実際に列として存在する
    assert "jrdb_pace_idx" in k.columns and "jrdb_pace_idx" in JRDB_COLS


def test_parse_sed_trouble_fields(tmp_path):
    p = tmp_path / "SED080913.txt"
    p.write_bytes(_sed_record())
    df = parse(str(p), "SED")
    assert df.loc[0, "race_id"] == "200801020101"
    assert df.loc[0, "umaban"] == 7
    assert df.loc[0, "ymd"] == "20080913"
    assert df.loc[0, "chakujun"] == 3
    assert df.loc[0, "deokure"] == 5     # 出遅
    assert df.loc[0, "furi"] == 8        # 不利


def test_parse_skb_tokki(tmp_path):
    p = tmp_path / "SKB020908.txt"
    p.write_bytes(_skb_record())
    df = parse(str(p), "SKB")
    assert df.loc[0, "tokki1"] == "387"   # 不利
    assert df.loc[0, "tokki2"] == "332"
    assert df.loc[0, "tokki3"] == ""      # 空スロット
    assert df.loc[0, "race_id"] == "200201020201"


def _tyb_record() -> bytes:
    r = bytearray(b" " * 128)
    _put(r, 1, "06251101")     # race_key: 場06 年25 回1 日1 R01
    _put(r, 9, "03")           # 馬番
    _put(r, 11, " 38.0")       # IDM
    _put(r, 26, "  2.0")       # オッズ指数
    _put(r, 31, "  3.4")       # パドック指数
    _put(r, 73, "   9.2")      # 単勝オッズ（直前）
    _put(r, 89, "444")         # 馬体重
    return bytes(r) + b"\r\n"


def test_parse_tyb(tmp_path):
    """TYB（直前情報・血統登録番号なし）のパースと直前指数。"""
    p = tmp_path / "TYB250105.txt"
    p.write_bytes(_tyb_record())
    df = parse(str(p), "TYB")
    assert df.loc[0, "race_id"] == "202506010101"
    assert df.loc[0, "umaban"] == 3
    assert df.loc[0, "idm"] == 38.0
    assert df.loc[0, "paddock_idx"] == 3.4
    assert df.loc[0, "tansho_odds"] == 9.2
    assert df.loc[0, "bataijuu"] == 444
    assert "ketto" not in df.columns  # TYB は血統登録番号を持たない


def test_multiple_records(tmp_path):
    p = tmp_path / "SKB020908.txt"
    p.write_bytes(_skb_record() + _skb_record())
    df = parse(str(p), "SKB")
    assert len(df) == 2
    assert isinstance(df, pd.DataFrame)
