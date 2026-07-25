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


def test_multiple_records(tmp_path):
    p = tmp_path / "SKB020908.txt"
    p.write_bytes(_skb_record() + _skb_record())
    df = parse(str(p), "SKB")
    assert len(df) == 2
    assert isinstance(df, pd.DataFrame)
