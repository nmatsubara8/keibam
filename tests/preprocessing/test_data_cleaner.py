"""_data_cleaner モジュールの単体テスト。

convert_to_datetime / dict_selector / convert_column_types の
全変換パス（float→int / padding あり・なし / str/int→str）を検証する。
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.preprocessing._data_cleaner import convert_column_types, convert_to_datetime, dict_selector


# ──────────────────────────────────────────────────────
# convert_to_datetime
# ──────────────────────────────────────────────────────


def test_convert_to_datetime_basic():
    result = convert_to_datetime("2023-01-15 00:00:00", "10:30")
    assert result == "2023-01-15 10:30"


def test_convert_to_datetime_strips_time_part_of_date():
    result = convert_to_datetime("2024-06-01 extra", "09:00")
    assert result == "2024-06-01 09:00"


# ──────────────────────────────────────────────────────
# dict_selector
# ──────────────────────────────────────────────────────


def test_dict_selector_results_has_trainer_id():
    d = dict_selector("_results")
    assert "trainer_id" in d


def test_dict_selector_horse_results_has_rank():
    d = dict_selector("_horse_results")
    assert "R" in d


def test_dict_selector_else_returns_horse_id():
    d = dict_selector("other")
    assert "horse_id" in d


def test_dict_selector_results_trainer_id_uses_int_src():
    _, (from_type, to_type, width, padding) = ("trainer_id", dict_selector("_results")["trainer_id"])
    assert from_type == "int" and to_type == "str" and padding is True


# ──────────────────────────────────────────────────────
# convert_column_types — float → int
# ──────────────────────────────────────────────────────


def test_float_to_int_converts_cleanly():
    df = pd.DataFrame({"v": [1.0, 2.0, 3.0]})
    result = convert_column_types(df, {"v": ("float", "int", 0, False)})
    assert result["v"].dtype == int


def test_float_to_int_fills_nan_with_zero():
    df = pd.DataFrame({"v": [1.0, float("nan"), 3.0]})
    result = convert_column_types(df, {"v": ("float", "int", 0, False)})
    assert result["v"].iloc[1] == 0


# ──────────────────────────────────────────────────────
# convert_column_types — float → str with padding
# ──────────────────────────────────────────────────────


def test_float_to_str_with_padding_zero_fills():
    df = pd.DataFrame({"v": [1.0, 12.0]})
    result = convert_column_types(df, {"v": ("float", "str", 2, True)})
    assert result["v"].iloc[0] == "01"
    assert result["v"].iloc[1] == "12"


def test_float_to_str_with_padding_strips_decimal():
    df = pd.DataFrame({"v": [5.0]})
    result = convert_column_types(df, {"v": ("float", "str", 3, True)})
    assert result["v"].iloc[0] == "005"


# ──────────────────────────────────────────────────────
# convert_column_types — int → str with padding
# ──────────────────────────────────────────────────────


def test_int_to_str_with_padding_zero_fills():
    df = pd.DataFrame({"v": [7]})
    result = convert_column_types(df, {"v": ("int", "str", 5, True)})
    assert result["v"].iloc[0] == "00007"


def test_int_to_str_no_padding_converts():
    df = pd.DataFrame({"v": [42]})
    result = convert_column_types(df, {"v": ("int", "str", 0, False)})
    assert result["v"].iloc[0] == "42"


# ──────────────────────────────────────────────────────
# convert_column_types — 複数列の同時変換
# ──────────────────────────────────────────────────────


def test_multiple_columns_converted_independently():
    df = pd.DataFrame({"a": [1.0], "b": [3]})
    col_types = {
        "a": ("float", "int", 0, False),
        "b": ("int", "str", 2, True),
    }
    result = convert_column_types(df, col_types)
    assert result["a"].iloc[0] == 1
    assert result["b"].iloc[0] == "03"


def test_original_dataframe_not_mutated():
    df = pd.DataFrame({"v": [1.0]})
    _ = convert_column_types(df, {"v": ("float", "int", 0, False)})
    assert df["v"].dtype == float
