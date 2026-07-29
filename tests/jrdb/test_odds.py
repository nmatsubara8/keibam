"""JRDB 基準オッズ（OZ/OW/OU/OT/OV）パーサの単体テスト。

組合せ列挙の件数・順序、byte オフセット、3連単の 0.1倍単位、取消/頭数フィルタを検証。
"""
from __future__ import annotations

import pandas as pd

from src.jrdb._odds import (
    _kumi_sanrentan,
    _kumi_umatan,
    _selftest_combos,
    favorites,
    normalize_combo,
    parse_odds,
)


def _put(buf: bytearray, start1: int, s: str):
    b = s.encode("cp932")
    buf[start1 - 1: start1 - 1 + len(b)] = b


def test_combo_enumeration_counts_and_order():
    assert _selftest_combos()
    assert _kumi_umatan()[0] == (1, 2) and _kumi_umatan()[-1] == (18, 17)
    assert _kumi_sanrentan()[0] == (1, 2, 3) and _kumi_sanrentan()[-1] == (18, 17, 16)


def test_parse_oz_tansho_fukusho_umaren(tmp_path):
    r = bytearray(b" " * 955)   # 957 - CRLF
    _put(r, 1, "02152201")      # race_key -> 201502020201
    _put(r, 9, " 3")            # 登録頭数=3（4番以降の組合せは除外される）
    # 単勝@11（5byte×18）: 馬番1=" 2.1", 馬番2=" 5.0", 馬番3="12.3"
    _put(r, 11, "  2.1")
    _put(r, 16, "  5.0")
    _put(r, 21, " 12.3")
    # 複勝@101: 馬番1=" 1.5"
    _put(r, 101, "  1.5")
    # 馬連@191（i<j 順、153組）: 先頭が 1-2
    _put(r, 191, " 45.6")       # 1-2
    _put(r, 196, " 78.9")       # 1-3
    p = tmp_path / "OZ150712.txt"
    p.write_bytes(bytes(r) + b"\r\n")
    df = parse_odds(str(p), "OZ")
    assert set(df["bet"].unique()) == {"tansho", "fukusho", "umaren"}
    tan = df[df.bet == "tansho"].set_index("combo")["odds"]
    assert tan["01"] == 2.1 and tan["02"] == 5.0 and tan["03"] == 12.3
    assert df[df.bet == "fukusho"].set_index("combo")["odds"]["01"] == 1.5
    um = df[df.bet == "umaren"].set_index("combo")["odds"]
    assert um["01-02"] == 45.6 and um["01-03"] == 78.9
    # 頭数=3 なので 04 を含む組合せは無い
    assert not df["combo"].str.contains("04").any()


def test_parse_ov_tenths_and_cancel(tmp_path):
    r = bytearray(b" " * 34286)   # 34288 - CRLF
    _put(r, 1, "02152201")
    _put(r, 9, "18")
    # 3連単@11（7byte×4896・0.1倍単位）: 先頭 01-02-03 = "0001234" → 123.4
    _put(r, 11, "0001234")
    _put(r, 18, "9999999")        # 01-02-04 は取消 → 除外
    p = tmp_path / "OV150712.txt"
    p.write_bytes(bytes(r) + b"\r\n")
    df = parse_odds(str(p), "OV")
    s = df.set_index("combo")["odds"]
    assert s["01-02-03"] == 123.4          # 0.1倍単位
    assert "01-02-04" not in s.index       # 取消は除外
    assert df["bet"].unique().tolist() == ["sanrentan"]


def test_parse_ou_umatan_order(tmp_path):
    r = bytearray(b" " * 1854)   # 1856 - CRLF
    _put(r, 1, "02152201")
    _put(r, 9, "18")
    _put(r, 11, "  12.3")        # 1-2（6byte ZZZ9.9）
    _put(r, 17, "  45.6")        # 1-3
    p = tmp_path / "OU150712.txt"
    p.write_bytes(bytes(r) + b"\r\n")
    df = parse_odds(str(p), "OU").set_index("combo")["odds"]
    assert df["01-02"] == 12.3 and df["01-03"] == 45.6


def test_normalize_combo_ordered_vs_unordered():
    # 順不同（3連複）は昇順ソートで HJC 連結と一致
    assert normalize_combo("070311", ordered=False) == "03-07-11"
    assert normalize_combo("03-07-11", ordered=False) == "03-07-11"
    # 着順あり（3連単）は順序保持
    assert normalize_combo("070311", ordered=True) == "07-03-11"
    assert normalize_combo("07-03-11", ordered=True) == "07-03-11"


def test_favorites_picks_min_odds():
    long = pd.DataFrame({
        "race_id": ["R1", "R1", "R1"], "bet": ["umaren"] * 3,
        "combo": ["01-02", "03-04", "05-06"], "odds": [12.3, 4.5, 20.0],
    })
    fav = favorites(long, top=1)
    assert len(fav) == 1 and fav.iloc[0]["combo"] == "03-04"   # 最小 4.5
