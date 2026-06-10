"""DataSplitter の make_stacking_splits テスト。"""

import sys
import types
import unittest.mock as mock

import numpy as np
import pandas as pd
import pytest

from src.constants._results_cols import ResultsCols

# optuna が未インストール環境でも DataSplitter をテストできるようにスタブ化する。
_optuna_stub = types.ModuleType("optuna")
_lgb_stub = types.ModuleType("optuna.integration.lightgbm")


class _DatasetStub:
    def __init__(self, data, label):
        self.data = data
        self.label = label


_lgb_stub.Dataset = _DatasetStub
_optuna_stub.integration = types.SimpleNamespace(lightgbm=_lgb_stub)
sys.modules.setdefault("optuna", _optuna_stub)
sys.modules.setdefault("optuna.integration", _optuna_stub.integration)
sys.modules.setdefault("optuna.integration.lightgbm", _lgb_stub)

from src.training._data_splitter import DataSplitter  # noqa: E402


def _make_featured(n_races: int = 100, horses_per_race: int = 8, seed: int = 0) -> pd.DataFrame:
    """make_stacking_splits のテスト用ダミー featured_data。"""
    rng = np.random.default_rng(seed)
    rows = []
    base_date = pd.Timestamp("2020-01-01")
    for i in range(n_races):
        race_id = f"race_{i:04d}"
        date = base_date + pd.Timedelta(days=i)
        for h in range(horses_per_race):
            rows.append(
                {
                    "race_id": race_id,
                    "date": date,
                    "rank": int(rng.integers(0, 2)),
                    ResultsCols.TANSHO_ODDS: float(rng.uniform(1.5, 20.0)),
                    ResultsCols.UMABAN: h + 1,
                    "feat_a": float(rng.normal()),
                    "feat_b": float(rng.normal()),
                }
            )
    df = pd.DataFrame(rows).set_index("race_id")
    return df


def test_make_stacking_splits_sizes():
    df = _make_featured(n_races=100)
    ds = DataSplitter(df, test_size=0.2, valid_size=0.2)
    ds.make_stacking_splits(meta_ratio=0.3)

    # base_train + meta_train ≒ train_data_optuna (行数で確認)
    total_opt = len(ds.train_data_optuna)
    assert len(ds.base_train_data) + len(ds.meta_train_data) == total_opt


def test_make_stacking_splits_no_overlap_with_test():
    df = _make_featured(n_races=100)
    ds = DataSplitter(df, test_size=0.2, valid_size=0.2)
    ds.make_stacking_splits(meta_ratio=0.3)

    test_ids = set(ds.test_data.index)
    base_ids = set(ds.base_train_data.index)
    meta_ids = set(ds.meta_train_data.index)
    calib_ids = set(ds.calib_holdout_data.index)

    assert base_ids.isdisjoint(test_ids), "base_train と test が重複"
    assert meta_ids.isdisjoint(test_ids), "meta_train と test が重複"
    assert calib_ids.isdisjoint(test_ids), "calib_holdout と test が重複"


def test_x_y_splits_drop_correct_columns():
    df = _make_featured(n_races=100)
    ds = DataSplitter(df, test_size=0.2, valid_size=0.2)
    ds.make_stacking_splits(meta_ratio=0.3)

    for split_x in [ds.X_base_train, ds.X_meta_train, ds.X_calib]:
        assert "rank" not in split_x.columns
        assert "date" not in split_x.columns
        assert ResultsCols.TANSHO_ODDS not in split_x.columns

    for split_y in [ds.y_base_train, ds.y_meta_train, ds.y_calib]:
        assert isinstance(split_y, pd.Series)
        assert len(split_y) > 0


def test_before_make_stacking_splits_raises():
    df = _make_featured(n_races=50)
    ds = DataSplitter(df, test_size=0.2, valid_size=0.2)
    with pytest.raises(RuntimeError):
        _ = ds.base_train_data


def test_temporal_order_base_before_meta():
    """base_train の日付が meta_train よりも前であること（時系列リーク防止）。"""
    df = _make_featured(n_races=100)
    ds = DataSplitter(df, test_size=0.2, valid_size=0.2)
    ds.make_stacking_splits(meta_ratio=0.3)

    base_max_date = ds.base_train_data["date"].max()
    meta_min_date = ds.meta_train_data["date"].min()
    assert base_max_date <= meta_min_date
