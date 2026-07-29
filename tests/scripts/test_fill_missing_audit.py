"""fill_missing_audit の純粋ヘルパー（年抽出・欠損率・乖離）の単体テスト。

ローカルの pickle には依存せず、合成 DataFrame で年抽出→欠損率→補完年乖離の
計算経路を検証する。
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# scripts/ はパッケージではないのでファイルパスから直接ロードする。
_MOD_PATH = Path(__file__).resolve().parents[2] / "scripts" / "fill_missing_audit.py"
_spec = importlib.util.spec_from_file_location("fill_missing_audit", _MOD_PATH)
audit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(audit)


def test_year_series_from_date_column():
    df = pd.DataFrame({"date": ["2021-04-01", "2022-11-30", "2023-01-05"]})
    assert audit.year_series(df).tolist() == ["2021", "2022", "2023"]


def test_year_series_from_race_id_column():
    df = pd.DataFrame({"race_id": ["202112345678", "202299990001"]})
    assert audit.year_series(df).tolist() == ["2021", "2022"]


def test_year_series_from_race_id_index():
    df = pd.DataFrame({"x": [1, 2]}, index=["202001010101", "202401010101"])
    assert audit.year_series(df).tolist() == ["2020", "2024"]


def test_year_series_raises_without_year():
    with pytest.raises(ValueError):
        audit.year_series(pd.DataFrame({"x": [1, 2]}))


def test_null_rate_by_year_counts_missing():
    # a列: 2022 だけ全欠損, b列: 全年充填
    df = pd.DataFrame({
        "a": [1.0, np.nan, np.nan, 4.0],
        "b": [1.0, 2.0, 3.0, 4.0],
    })
    years = pd.Series(["2020", "2022", "2022", "2023"])
    nby = audit.null_rate_by_year(df, years)
    assert nby.loc["a", "2022"] == 1.0      # 2022 の a は全欠損
    assert nby.loc["a", "2020"] == 0.0
    assert nby.loc["b", "2022"] == 0.0      # b は欠損なし


def test_divergence_ranks_fill_vs_neighbor():
    # a: 補完年(2022)で全欠損・近傍(2020/2023)で充填 → 差 +1.0 で先頭
    df = pd.DataFrame({
        "a": [1.0, np.nan, 3.0],
        "b": [1.0, 2.0, 3.0],
    })
    years = pd.Series(["2020", "2022", "2023"])
    nby = audit.null_rate_by_year(df, years)
    div = audit.divergence(nby, fill_years=["2022"], neighbor_years=["2020", "2023"])
    assert div.index[0] == "a"
    assert div.loc["a", "diff"] == pytest.approx(1.0)
    assert div.loc["b", "diff"] == pytest.approx(0.0)


def test_divergence_raises_when_years_absent():
    df = pd.DataFrame({"a": [1.0, 2.0]})
    years = pd.Series(["2020", "2023"])
    nby = audit.null_rate_by_year(df, years)
    with pytest.raises(ValueError):
        audit.divergence(nby, fill_years=["2099"], neighbor_years=["2020"])
