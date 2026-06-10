"""NnFeatureScaler のテスト。"""

import numpy as np
import pandas as pd
import pytest

from src.preprocessing._nn_feature_scaler import NnFeatureScaler


def _make_df():
    return pd.DataFrame(
        {
            "horse_id": pd.Categorical([10, 20, 30, 40]),
            "jockey_id": pd.Categorical([1, 2, 1, 3]),
            "feat_a": [1.0, 2.0, 3.0, 4.0],
            "feat_b": [100.0, 200.0, 300.0, 400.0],
        }
    )


class TestNnFeatureScalerFitTransform:
    def test_returns_correct_columns(self):
        df = _make_df()
        scaler = NnFeatureScaler(entity_cols=["horse_id", "jockey_id"], numeric_cols=["feat_a", "feat_b"])
        result = scaler.fit_transform(df)
        assert set(result.columns) == {"horse_id", "jockey_id", "feat_a", "feat_b"}

    def test_numeric_cols_standardized(self):
        df = _make_df()
        scaler = NnFeatureScaler(entity_cols=["horse_id"], numeric_cols=["feat_a", "feat_b"])
        result = scaler.fit_transform(df)
        assert abs(result["feat_a"].mean()) < 1e-10
        assert abs(result["feat_b"].mean()) < 1e-10

    def test_entity_cols_not_modified(self):
        df = _make_df()
        scaler = NnFeatureScaler(entity_cols=["horse_id", "jockey_id"], numeric_cols=["feat_a", "feat_b"])
        result = scaler.fit_transform(df)
        pd.testing.assert_series_equal(result["horse_id"], df["horse_id"])
        pd.testing.assert_series_equal(result["jockey_id"], df["jockey_id"])

    def test_fitted_flag_set(self):
        scaler = NnFeatureScaler(entity_cols=[], numeric_cols=["feat_a"])
        assert not scaler._fitted
        scaler.fit_transform(_make_df())
        assert scaler._fitted

    def test_empty_numeric_cols(self):
        df = _make_df()
        scaler = NnFeatureScaler(entity_cols=["horse_id"], numeric_cols=[])
        result = scaler.fit_transform(df)
        assert list(result.columns) == ["horse_id"]


class TestNnFeatureScalerTransform:
    def test_transform_consistent_with_fit_transform(self):
        train_df = _make_df()
        scaler = NnFeatureScaler(entity_cols=["horse_id"], numeric_cols=["feat_a", "feat_b"])
        train_result = scaler.fit_transform(train_df)

        test_df = pd.DataFrame(
            {"horse_id": pd.Categorical([50]), "feat_a": [2.5], "feat_b": [250.0]}
        )
        test_result = scaler.transform(test_df)

        # Same mean/scale should be applied
        expected_a = (2.5 - train_df["feat_a"].mean()) / train_df["feat_a"].std(ddof=0)
        assert abs(test_result["feat_a"].iloc[0] - expected_a) < 1e-6

    def test_transform_before_fit_raises(self):
        scaler = NnFeatureScaler(entity_cols=[], numeric_cols=["feat_a"])
        df = pd.DataFrame({"feat_a": [1.0]})
        with pytest.raises(RuntimeError, match="fit_transform"):
            scaler.transform(df)

    def test_transform_handles_missing_optional_cols(self):
        """Columns missing from df are silently skipped by _select."""
        train_df = _make_df()
        scaler = NnFeatureScaler(entity_cols=["horse_id", "jockey_id"], numeric_cols=["feat_a", "feat_b"])
        scaler.fit_transform(train_df)

        # Only feat_a available, feat_b missing
        partial_df = pd.DataFrame({"horse_id": pd.Categorical([10]), "feat_a": [1.0]})
        result = scaler.transform(partial_df)
        assert "feat_a" in result.columns
        assert "feat_b" not in result.columns


class TestNnFeatureScalerRepr:
    def test_repr_contains_counts(self):
        scaler = NnFeatureScaler(entity_cols=["horse_id"], numeric_cols=["feat_a", "feat_b"])
        assert "n_entity=1" in repr(scaler)
        assert "n_numeric=2" in repr(scaler)
