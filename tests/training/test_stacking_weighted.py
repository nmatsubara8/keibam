"""StackingModel の base_sample_weights / ModelWrapper の不均衡補正テスト。"""

import sys
import types

import numpy as np
import pytest
from sklearn.linear_model import LogisticRegression

from src.training._stacking_model import StackingModel


def _make_data(n=300, seed=0):
    rng = np.random.default_rng(seed)
    x = rng.normal(size=(n, 4))
    y = (x[:, 0] + 0.5 * x[:, 1] + rng.normal(scale=0.5, size=n) > 0).astype(int)
    return x, y


class _RecordingModel:
    """fit に渡された sample_weight を記録するスタブ base 学習器。"""

    def __init__(self):
        self.received_weight = "NOT_CALLED"

    def fit(self, x, y, sample_weight=None):
        self.received_weight = sample_weight
        return self

    def predict_proba(self, x):
        n = len(np.asarray(x))
        p = np.full(n, 0.5)
        return np.column_stack([1.0 - p, p])


class TestStackingBaseSampleWeights:
    def test_weights_passed_to_correct_base(self):
        x, y = _make_data()
        half = len(x) // 2
        m0, m1 = _RecordingModel(), _RecordingModel()
        stack = StackingModel([m0, m1], meta_model=LogisticRegression())
        w0 = np.ones(half)
        stack.fit(x[:half], y[:half], x[half:], y[half:], base_sample_weights=[w0, None])
        assert m0.received_weight is not None
        np.testing.assert_array_equal(m0.received_weight, w0)
        # m1 received None → fit called without sample_weight
        assert m1.received_weight is None

    def test_none_weights_backward_compatible(self):
        x, y = _make_data()
        half = len(x) // 2
        m0 = _RecordingModel()
        stack = StackingModel([m0], meta_model=LogisticRegression())
        stack.fit(x[:half], y[:half], x[half:], y[half:])
        # No base_sample_weights → fit called without sample_weight kwarg → None
        assert m0.received_weight is None

    def test_wrong_length_raises(self):
        x, y = _make_data()
        half = len(x) // 2
        stack = StackingModel(
            [LogisticRegression(), LogisticRegression()], meta_model=LogisticRegression()
        )
        with pytest.raises(ValueError):
            stack.fit(x[:half], y[:half], x[half:], y[half:], base_sample_weights=[np.ones(half)])

    def test_predict_proba_still_valid_with_weights(self):
        x, y = _make_data()
        half = len(x) // 2
        stack = StackingModel(
            [LogisticRegression(), LogisticRegression()], meta_model=LogisticRegression()
        )
        w = np.ones(half)
        stack.fit(x[:half], y[:half], x[half:], y[half:], base_sample_weights=[w, w])
        proba = stack.predict_proba(x)
        assert proba.shape == (len(x), 2)
        assert np.all(proba >= 0.0) and np.all(proba <= 1.0)


# ── ModelWrapper の scale_pos_weight 注入（optuna スタブ必要）──

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


class TestModelWrapperImbalance:
    def test_default_scale_pos_weight_injected(self):
        from src.constants._bet_thresholds import TrainingWeights
        from src.training._model_wrapper import ModelWrapper

        mw = ModelWrapper()
        assert mw.params["scale_pos_weight"] == pytest.approx(TrainingWeights.SCALE_POS_WEIGHT)

    def test_custom_scale_pos_weight(self):
        from src.training._model_wrapper import ModelWrapper

        mw = ModelWrapper(scale_pos_weight=8.0)
        assert mw.params["scale_pos_weight"] == pytest.approx(8.0)

    def test_is_unbalance_not_set(self):
        """is_unbalance は使わない（EV sigmoid との二重重み付け回避）。"""
        from src.training._model_wrapper import ModelWrapper

        mw = ModelWrapper()
        # is_unbalance は設定されていない（None or absent）
        assert not mw.params.get("is_unbalance", False)
