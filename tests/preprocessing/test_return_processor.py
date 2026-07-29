"""ReturnProcessor の単体テスト（ファイル I/O なし）。

ヘルパー関数（count_br / convert_to_int / split_bar_to_int / split_arrow_to_int）と
preprocessed_data の各馬券種 DataFrame を、pd.read_pickle をモックして検証する。
"""

from __future__ import annotations

import unittest.mock

import pandas as pd

from src.constants._bet_types import BetType
from src.preprocessing._return_processor import (
    ReturnProcessor,
    convert_to_int,
    count_br,
    split_arrow_to_int,
    split_bar_to_int,
)

# 8 馬券種のラベルとダミーデータ（全馬券種を含む raw_data を生成するための定数）
_ALL_BET_LABELS = [
    ("単勝", "1", "100"),
    ("複勝", "1", "110"),
    ("枠連", "1-2", "200"),
    ("馬連", "1-2", "300"),
    ("馬単", "1→2", "400"),
    ("ワイド", "1-2", "150"),
    ("三連複", "1-2-3", "500"),
    ("三連単", "1→2→3", "600"),
]


# ──────────────────────────────────────────────────────
# ヘルパー: フィクスチャ生成
# ──────────────────────────────────────────────────────


def _make_raw_full(overrides: dict | None = None, race_id: str = "R001") -> pd.DataFrame:
    """全 8 馬券種を含む raw_data を生成する。overrides で特定馬券種を上書きできる。

    Parameters
    ----------
    overrides: {ラベル: (win_str, return_str)} で上書き値を指定する。
    race_id: レース ID。
    """
    overrides = overrides or {}
    records = []
    for label, default_win, default_ret in _ALL_BET_LABELS:
        win, ret = overrides.get(label, (default_win, default_ret))
        records.append({0: label, 1: win, 2: str(ret), "race_id": race_id})
    return pd.DataFrame(records)


def _rp_from_raw(raw_df: pd.DataFrame) -> ReturnProcessor:
    """pd.read_pickle をモックして ReturnProcessor を生成する（ファイル I/O なし）。"""
    with unittest.mock.patch("pandas.read_pickle", return_value=raw_df):
        return ReturnProcessor("dummy.pkl")


# ──────────────────────────────────────────────────────
# ヘルパー関数テスト
# ──────────────────────────────────────────────────────


def test_convert_to_int_plain():
    assert convert_to_int("1200") == 1200


def test_convert_to_int_removes_comma():
    assert convert_to_int("1,200") == 1200


def test_convert_to_int_large_number():
    assert convert_to_int("1,234,567") == 1234567


def test_split_bar_to_int_two_horses():
    assert split_bar_to_int("2-1") == [2, 1]


def test_split_bar_to_int_three_horses():
    assert split_bar_to_int("3-1-5") == [3, 1, 5]


def test_split_bar_to_int_passthrough_non_string():
    # 非文字列はそのまま返す
    assert split_bar_to_int([1, 2]) == [1, 2]


def test_split_arrow_to_int_two_horses():
    assert split_arrow_to_int("1→3") == [1, 3]


def test_split_arrow_to_int_three_horses():
    assert split_arrow_to_int("5→2→8") == [5, 2, 8]


def test_split_arrow_to_int_tokubarai_is_empty():
    # 特払い（レース不成立）の '特' は馬番でない → 組合せ全体を該当なし([])にして落とさない
    assert split_arrow_to_int("特") == []
    assert split_arrow_to_int("1→特→3") == []


def test_split_bar_to_int_tokubarai_is_empty():
    assert split_bar_to_int("特") == []
    assert split_bar_to_int("3 - 特") == []


def test_convert_to_int_tokubarai_is_zero():
    # 特払い等の非数値は 0（どの馬番にも一致しない＝該当なし）で落とさない
    assert convert_to_int("特") == 0
    assert convert_to_int("") == 0


def test_count_br_zero_no_separator():
    df = pd.DataFrame({1: ["5"]})
    assert count_br(df, 1) == 0


def test_count_br_one_separator():
    df = pd.DataFrame({1: ["3br5"]})
    assert count_br(df, 1) == 1


def test_count_br_two_separators():
    df = pd.DataFrame({1: ["3br5br7"]})
    assert count_br(df, 1) == 2


def test_count_br_max_across_rows():
    df = pd.DataFrame({1: ["3br5", "7"]})
    assert count_br(df, 1) == 1


def test_count_br_empty_dataframe_returns_zero():
    """空 DataFrame では 0 を返す（ValueError を出さない）。"""
    df = pd.DataFrame({1: []})
    assert count_br(df, 1) == 0


# ──────────────────────────────────────────────────────
# preprocessed_data — 各馬券種のテスト
# ──────────────────────────────────────────────────────


def test_tansho_win_is_int_and_return_correct():
    raw = _make_raw_full({"単勝": ("5", "1,200")})
    rp = _rp_from_raw(raw)
    df = rp.preprocessed_data[BetType.TANSHO]
    assert df.loc["R001", "win_0"] == 5
    assert df.loc["R001", "return_0"] == 1200


def test_fukusho_three_hits_returns():
    raw = _make_raw_full({"複勝": ("3br5br7", "320br290br270")})
    rp = _rp_from_raw(raw)
    df = rp.preprocessed_data[BetType.FUKUSHO]
    assert df.loc["R001", "return_0"] == 320
    assert df.loc["R001", "return_1"] == 290
    assert df.loc["R001", "return_2"] == 270


def test_wakuren_list_key_parsed():
    raw = _make_raw_full({"枠連": ("2-4", "800")})
    rp = _rp_from_raw(raw)
    df = rp.preprocessed_data[BetType.WAKUREN]
    assert df.loc["R001", "win_0"] == [2, 4]
    assert df.loc["R001", "return_0"] == 800


def test_umaren_win_preserves_parse_order():
    raw = _make_raw_full({"馬連": ("3-1", "1500")})
    rp = _rp_from_raw(raw)
    df = rp.preprocessed_data[BetType.UMAREN]
    # split_bar_to_int は順序を保持（ソートしない）
    assert df.loc["R001", "win_0"] == [3, 1]
    assert df.loc["R001", "return_0"] == 1500


def test_umatan_ordered_key():
    raw = _make_raw_full({"馬単": ("1→3", "2000")})
    rp = _rp_from_raw(raw)
    df = rp.preprocessed_data[BetType.UMATAN]
    assert df.loc["R001", "win_0"] == [1, 3]
    assert df.loc["R001", "return_0"] == 2000


def test_wide_multi_hit():
    raw = _make_raw_full({"ワイド": ("1-3br2-4", "500br300")})
    rp = _rp_from_raw(raw)
    df = rp.preprocessed_data[BetType.WIDE]
    assert df.loc["R001", "win_0"] == [1, 3]
    assert df.loc["R001", "win_1"] == [2, 4]
    assert df.loc["R001", "return_0"] == 500


def test_sanrenpuku_three_horses():
    raw = _make_raw_full({"三連複": ("1-3-5", "5000")})
    rp = _rp_from_raw(raw)
    df = rp.preprocessed_data[BetType.SANRENPUKU]
    assert df.loc["R001", "win_0"] == [1, 3, 5]
    assert df.loc["R001", "return_0"] == 5000


def test_sanrentan_ordered_three_horses():
    raw = _make_raw_full({"三連単": ("2→5→1", "35000")})
    rp = _rp_from_raw(raw)
    df = rp.preprocessed_data[BetType.SANRENTAN]
    assert df.loc["R001", "win_0"] == [2, 5, 1]
    assert df.loc["R001", "return_0"] == 35000


def test_multiple_races_independent():
    records = []
    for race_id in ("R001", "R002"):
        for label, default_win, default_ret in _ALL_BET_LABELS:
            records.append({0: label, 1: default_win, 2: default_ret, "race_id": race_id})
    raw = pd.DataFrame(records)
    # 単勝だけ上書き
    raw.loc[(raw[0] == "単勝") & (raw["race_id"] == "R001"), 1] = "3"
    raw.loc[(raw[0] == "単勝") & (raw["race_id"] == "R001"), 2] = "1000"
    raw.loc[(raw[0] == "単勝") & (raw["race_id"] == "R002"), 1] = "7"
    raw.loc[(raw[0] == "単勝") & (raw["race_id"] == "R002"), 2] = "2500"
    rp = _rp_from_raw(raw)
    df = rp.preprocessed_data[BetType.TANSHO]
    assert df.loc["R001", "win_0"] == 3
    assert df.loc["R002", "win_0"] == 7


def test_preprocessed_data_has_all_bet_types():
    raw = _make_raw_full()
    rp = _rp_from_raw(raw)
    data = rp.preprocessed_data
    expected_keys = {
        BetType.TANSHO, BetType.FUKUSHO, BetType.WAKUREN,
        BetType.UMAREN, BetType.UMATAN, BetType.WIDE,
        BetType.SANRENPUKU, BetType.SANRENTAN,
    }
    assert expected_keys == set(data.keys())
