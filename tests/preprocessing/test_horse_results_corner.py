"""HorseResultsProcessor.parse_corner の堅牢性テスト（DB 復元時の空通過順対応）。"""

import numpy as np
import pandas as pd

from src.preprocessing._horse_results_processor import parse_corner


def test_parse_corner_normal():
    assert parse_corner("3-3-2-1", 1) == 3
    assert parse_corner("3-3-2-1", 4) == 1
    assert parse_corner("5-4", 1) == 5
    assert parse_corner("5-4", 4) == 4


def test_parse_corner_no_digits_returns_na():
    # 空文字・記号のみ（DB 復元時の空通過順）は IndexError で落ちず欠損になる
    assert parse_corner("", 1) is pd.NA
    assert parse_corner("-", 4) is pd.NA
    assert parse_corner("nan", 1) is pd.NA


def test_parse_corner_non_string_passthrough():
    # NaN（float）等の非文字列はそのまま返す
    assert np.isnan(parse_corner(np.nan, 1))
    assert parse_corner(None, 4) is None
