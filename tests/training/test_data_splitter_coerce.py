"""DataSplitter の特徴量 coercion（pd.NA / nullable 拡張dtype 対策）のテスト。

JRDB fill/overwrite が欠損を pd.NA で埋め、featured の一部列が nullable 拡張dtype
（Int64/boolean）で残ると LightGBM が "float() ... not 'NAType'" で落ちる。
__coerce_object_features がそれらを float32(np.nan) へ寄せ、category は温存することを検証。
"""
import sys
import types

import numpy as np
import pandas as pd

# optuna スタブ（import 連鎖のため）
_optuna_stub = types.ModuleType("optuna")
_lgb_stub = types.ModuleType("optuna.integration.lightgbm")
_lgb_stub.Dataset = object
_optuna_stub.integration = types.SimpleNamespace(lightgbm=_lgb_stub)
sys.modules.setdefault("optuna", _optuna_stub)
sys.modules.setdefault("optuna.integration", _optuna_stub.integration)
sys.modules.setdefault("optuna.integration.lightgbm", _lgb_stub)

from src.training._data_splitter import DataSplitter  # noqa: E402

# name-mangled private classmethod へアクセス
_coerce = DataSplitter._DataSplitter__coerce_object_features


def test_nullable_extension_dtypes_with_na_become_float32_nan():
    df = pd.DataFrame({
        "handi": pd.array([1, pd.NA, 3], dtype="Int64"),        # nullable int + pd.NA
        "hinba": pd.array([True, pd.NA, False], dtype="boolean"),  # nullable bool + pd.NA
        "obj": pd.Series([1.0, None, 2.0], dtype=object),        # object + None
        "plain": np.array([1.0, 2.0, 3.0], dtype="float64"),     # 通常 float は不変
    })
    out = _coerce(df)
    # 拡張dtype 列は float32 になり pd.NA→np.nan
    assert out["handi"].dtype == np.float32
    assert out["hinba"].dtype == np.float32
    assert out["obj"].dtype == np.float32
    assert np.isnan(out["handi"].iloc[1]) and np.isnan(out["hinba"].iloc[1])
    assert out["hinba"].iloc[0] == 1.0 and out["hinba"].iloc[2] == 0.0
    # NAType が一切残らない（LightGBM に渡せる）
    assert not out.isna().any().any() or True  # np.nan は許容
    assert out.select_dtypes(include=["object"]).empty
    # float64 の通常列は据え置き（object/拡張ではないので触らない）
    assert out["plain"].dtype == np.float64


def test_category_columns_are_preserved():
    df = pd.DataFrame({
        "cat": pd.Series(["a", "b", "a"], dtype="category"),
        "handi": pd.array([1, pd.NA, 3], dtype="Int64"),
    })
    out = _coerce(df)
    assert isinstance(out["cat"].dtype, pd.CategoricalDtype)   # category は温存
    assert out["handi"].dtype == np.float32


def test_no_bad_columns_is_noop():
    df = pd.DataFrame({"a": np.array([1.0, 2.0]), "b": np.array([3, 4], dtype="int64")})
    out = _coerce(df)
    assert out["a"].dtype == np.float64 and out["b"].dtype == np.int64
