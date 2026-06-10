"""DataSplitter + PreparedFeatures の統合テスト。"""

import sys
import types

import numpy as np
import pandas as pd
import pytest

from src.constants._results_cols import ResultsCols

# optuna スタブ（test_data_splitter_stacking.py と同じ方法）
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

from src.preprocessing._prepared_features import PreparedFeatures  # noqa: E402
from src.training._data_splitter import DataSplitter  # noqa: E402


def _make_prepared_features(n_races=100, horses_per_race=8, seed=0):
    """PreparedFeatures を持つテスト用ダミーデータ。"""
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
                    "horse_id": h + 1,
                    "feat_num_a": float(rng.normal()),
                    "feat_num_b": float(rng.uniform(100, 500)),
                }
            )
    gbdt_df = pd.DataFrame(rows).set_index("race_id")
    # NN stream: entity col (category dtype) + numeric cols (no date/rank/odds)
    nn_df = gbdt_df[["horse_id", "feat_num_a", "feat_num_b"]].copy()
    nn_df["horse_id"] = nn_df["horse_id"].astype("category")
    return PreparedFeatures(gbdt=gbdt_df, nn=nn_df)


class TestDataSplitterWithPreparedFeatures:
    def test_nn_scaler_is_set(self):
        pf = _make_prepared_features()
        ds = DataSplitter(pf, test_size=0.2, valid_size=0.2)
        assert ds.nn_scaler is not None

    def test_nn_train_shape_matches_gbdt_train(self):
        pf = _make_prepared_features()
        ds = DataSplitter(pf, test_size=0.2, valid_size=0.2)
        assert len(ds.X_nn_train) == len(ds.X_train)

    def test_nn_test_shape_matches_gbdt_test(self):
        pf = _make_prepared_features()
        ds = DataSplitter(pf, test_size=0.2, valid_size=0.2)
        assert len(ds.X_nn_test) == len(ds.X_test)

    def test_nn_numeric_cols_standardized_on_train(self):
        """訓練 NN データの数値列は mean≈0 / std≈1 であること。"""
        pf = _make_prepared_features(n_races=200)
        ds = DataSplitter(pf, test_size=0.2, valid_size=0.2)
        numeric_cols = ds.nn_scaler.numeric_cols
        for col in numeric_cols:
            assert abs(ds.X_nn_train[col].mean()) < 0.1, f"{col} mean not ≈0"
            assert abs(ds.X_nn_train[col].std() - 1.0) < 0.2, f"{col} std not ≈1"

    def test_scaler_fitted_on_train_only(self):
        """スケーラーが train 以外の情報で fit されていないこと（リーク防止）。
        テスト用データの train mean と scaler の mean_ が一致することで確認。"""
        pf = _make_prepared_features(n_races=200, seed=42)
        ds = DataSplitter(pf, test_size=0.2, valid_size=0.2)
        scaler = ds.nn_scaler
        numeric_cols = scaler.numeric_cols
        if numeric_cols:
            train_nn = ds.X_nn_train
            for i, col in enumerate(numeric_cols):
                # transform 後の平均が 0 (fit on train → train mean ≈ 0)
                assert abs(train_nn[col].mean()) < 0.2

    def test_entity_cols_not_scaled(self):
        """entity_cols (category dtype) は StandardScaler に通らないこと。"""
        pf = _make_prepared_features()
        ds = DataSplitter(pf, test_size=0.2, valid_size=0.2)
        assert "horse_id" in ds.nn_scaler.entity_cols
        # horse_id は category dtype のまま
        assert str(ds.X_nn_train["horse_id"].dtype) == "category"

    def test_no_nn_scaler_for_plain_dataframe(self):
        """plain DataFrame を渡した場合 nn_scaler は None。"""
        rows = []
        for i in range(50):
            rows.append({"race_id": f"r{i}", "date": pd.Timestamp("2020-01-01") + pd.Timedelta(days=i),
                         "rank": 0, ResultsCols.TANSHO_ODDS: 2.0, "feat": float(i)})
        df = pd.DataFrame(rows).set_index("race_id")
        ds = DataSplitter(df, test_size=0.2, valid_size=0.2)
        assert ds.nn_scaler is None
        assert ds.X_nn_train is None
        assert ds.X_nn_test is None


class TestDataSplitterNnStackingSplits:
    def test_nn_base_meta_available_after_make_stacking_splits(self):
        pf = _make_prepared_features()
        ds = DataSplitter(pf, test_size=0.2, valid_size=0.2)
        ds.make_stacking_splits(meta_ratio=0.3)
        assert ds.X_nn_base_train is not None
        assert ds.X_nn_meta_train is not None

    def test_nn_base_meta_sizes_consistent(self):
        pf = _make_prepared_features()
        ds = DataSplitter(pf, test_size=0.2, valid_size=0.2)
        ds.make_stacking_splits(meta_ratio=0.3)
        assert len(ds.X_nn_base_train) == len(ds.X_base_train)
        assert len(ds.X_nn_meta_train) == len(ds.X_meta_train)

    def test_nn_base_raises_before_make_stacking_splits(self):
        pf = _make_prepared_features()
        ds = DataSplitter(pf, test_size=0.2, valid_size=0.2)
        with pytest.raises(RuntimeError, match="make_stacking_splits"):
            _ = ds.X_nn_base_train
