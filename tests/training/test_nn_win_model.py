"""NnWinModel のテスト。

torch 非依存のロジック（ターゲット二値化・埋め込み次元・パラメータ保持）と、
torch がある場合のみ実行する学習スモークテストに分ける。
"""

import importlib.util

import numpy as np
import pytest

from src.training._nn_win_model import NnWinModel, _embedding_dim, in_dim_total

_HAS_TORCH = importlib.util.find_spec("torch") is not None
_torch_required = pytest.mark.skipif(not _HAS_TORCH, reason="torch 未インストール")


class TestEmbeddingDim:
    def test_small_cardinality(self):
        # min(50, (n+1)//2)
        assert _embedding_dim(3) == 2
        assert _embedding_dim(10) == 5

    def test_capped_at_50(self):
        assert _embedding_dim(1000) == 50


class TestInDimTotal:
    def test_counts_cat_and_numeric(self):
        assert in_dim_total({0: 5, 1: 10}, n_numeric=3) == 5


class TestTargetBinarization:
    def test_already_binary_unchanged(self):
        model = NnWinModel(rank_threshold=1)
        y = np.array([0, 1, 0, 1])
        result = model._binarize_targets(y)
        np.testing.assert_array_equal(result, [0.0, 1.0, 0.0, 1.0])

    def test_raw_rank_binarized_top1(self):
        model = NnWinModel(rank_threshold=1)
        y = np.array([1, 2, 3, 5])  # actual finishing positions
        result = model._binarize_targets(y)
        # rank <= 1 → only first is win
        np.testing.assert_array_equal(result, [1.0, 0.0, 0.0, 0.0])

    def test_raw_rank_binarized_top3(self):
        model = NnWinModel(rank_threshold=3)
        y = np.array([1, 2, 3, 5])
        result = model._binarize_targets(y)
        # rank <= 3 → first three are win
        np.testing.assert_array_equal(result, [1.0, 1.0, 1.0, 0.0])


class TestParamStorage:
    def test_pos_weight_stored(self):
        model = NnWinModel(pos_weight=15.0)
        assert model._pos_weight == 15.0

    def test_early_stopping_params_stored(self):
        model = NnWinModel(patience=5, min_delta=1e-3, val_ratio=0.25)
        assert model._patience == 5
        assert model._min_delta == 1e-3
        assert model._val_ratio == 0.25

    def test_predict_before_fit_raises(self):
        model = NnWinModel(n_numeric=2)
        with pytest.raises(RuntimeError, match="fit"):
            model.predict_proba(np.zeros((3, 2)))


@_torch_required
class TestNnWinModelTraining:
    def _make_data(self, n=200, n_numeric=4, seed=0):
        rng = np.random.default_rng(seed)
        x = rng.normal(size=(n, n_numeric)).astype(np.float32)
        y = (x[:, 0] + rng.normal(scale=0.5, size=n) > 0).astype(int)
        return x, y

    def test_fit_predict_shape_and_range(self):
        x, y = self._make_data()
        model = NnWinModel(n_numeric=4, epochs=3, hidden_dims=(16,))
        model.fit(x, y)
        proba = model.predict_proba(x)
        assert proba.shape == (len(x), 2)
        assert np.all(proba >= 0.0) and np.all(proba <= 1.0)
        assert np.allclose(proba.sum(axis=1), 1.0)

    def test_fit_with_pos_weight(self):
        x, y = self._make_data()
        model = NnWinModel(n_numeric=4, epochs=3, hidden_dims=(16,), pos_weight=10.0)
        model.fit(x, y)
        proba = model.predict_proba(x)
        assert np.all(np.isfinite(proba))

    def test_fit_with_sample_weight(self):
        x, y = self._make_data()
        w = np.ones(len(x), dtype=np.float32)
        model = NnWinModel(n_numeric=4, epochs=3, hidden_dims=(16,))
        model.fit(x, y, sample_weight=w)
        proba = model.predict_proba(x)
        assert np.all(np.isfinite(proba))

    def test_early_stopping_does_not_crash(self):
        x, y = self._make_data()
        model = NnWinModel(n_numeric=4, epochs=50, hidden_dims=(16,), patience=2, val_ratio=0.3)
        model.fit(x, y)
        proba = model.predict_proba(x)
        assert np.all(np.isfinite(proba))

    def test_fit_with_weight_decay(self):
        """weight_decay（Adam L2）を指定しても学習・推論が壊れない。"""
        x, y = self._make_data()
        model = NnWinModel(n_numeric=4, epochs=3, hidden_dims=(16,), weight_decay=1e-4)
        assert model._weight_decay == 1e-4
        model.fit(x, y)
        proba = model.predict_proba(x)
        assert np.all(np.isfinite(proba))


class TestWeightDecayDefault:
    def test_default_weight_decay_is_zero(self):
        # 既定は 0.0＝従来挙動（正則化なし）
        assert NnWinModel()._weight_decay == 0.0
