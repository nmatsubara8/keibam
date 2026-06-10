"""CalibratedModel のテスト。"""

import numpy as np
import pytest

from src.training._calibrated_model import CalibratedModel


class _BiasedModel:
    """正例確率を一定割合で過大申告するスタブ。"""

    def predict_proba(self, x):
        raw = np.asarray(x).ravel().astype(float)
        return np.column_stack([1.0 - raw, raw])


def _make_data(n=400, seed=0):
    rng = np.random.default_rng(seed)
    # 真の確率 = x、ただしモデルは raw=x をそのまま返す（ここでは単調性の確認が目的）
    x = rng.uniform(0, 1, size=n)
    y = (rng.uniform(0, 1, size=n) < x).astype(int)
    return x.reshape(-1, 1), y


def test_predict_proba_shape_and_range():
    x, y = _make_data()
    model = CalibratedModel.fit(_BiasedModel(), x, y)
    proba = model.predict_proba(x)
    assert proba.shape == (len(x), 2)
    assert np.all(proba >= 0.0) and np.all(proba <= 1.0)
    assert np.allclose(proba.sum(axis=1), 1.0)


def test_calibration_is_monotonic_in_raw():
    x, y = _make_data()
    model = CalibratedModel.fit(_BiasedModel(), x, y)
    grid = np.linspace(0, 1, 50).reshape(-1, 1)
    calibrated = model.predict_proba(grid)[:, 1]
    # Isotonic なので raw に対して単調非減少
    assert np.all(np.diff(calibrated) >= -1e-9)


def test_does_not_modify_base_model():
    x, y = _make_data()
    base = _BiasedModel()
    model = CalibratedModel.fit(base, x, y)
    assert model.base_model is base
