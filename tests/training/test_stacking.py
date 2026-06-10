"""StackingModel のテスト。"""

import numpy as np
import pytest
from sklearn.linear_model import LogisticRegression

from src.training._stacking_model import StackingModel


def _make_data(n=300, seed=0):
    rng = np.random.default_rng(seed)
    x = rng.normal(size=(n, 4))
    y = (x[:, 0] + 0.5 * x[:, 1] + rng.normal(scale=0.5, size=n) > 0).astype(int)
    return x, y


def test_predict_proba_shape_and_range():
    x, y = _make_data()
    n = len(x)
    half = n // 2
    stack = StackingModel(
        base_models=[LogisticRegression(), LogisticRegression()],
        meta_model=LogisticRegression(),
    )
    stack.fit(x[:half], y[:half], x[half:], y[half:])
    proba = stack.predict_proba(x)
    assert proba.shape == (n, 2)
    assert np.all(proba >= 0.0) and np.all(proba <= 1.0)


def test_base_predictions_count_matches_base_models():
    x, y = _make_data()
    half = len(x) // 2
    bases = [LogisticRegression(), LogisticRegression()]
    stack = StackingModel(base_models=bases, meta_model=LogisticRegression())
    stack.fit(x[:half], y[:half], x[half:], y[half:])
    preds = stack.base_predictions(x[:10])
    assert len(preds) == len(bases)
    assert all(p.shape == (10,) for p in preds)


def test_empty_base_models_raises():
    with pytest.raises(ValueError):
        StackingModel(base_models=[], meta_model=LogisticRegression())
