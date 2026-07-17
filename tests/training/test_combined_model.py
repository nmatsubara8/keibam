"""CombinedModel（遅延スタッキング融合）の単体テスト。"""
from __future__ import annotations

import numpy as np
import pytest

from src.training._combined_model import CombinedModel
from src.training._combined_model import NnDerivedPredictor


class _StubProba:
    """predict_proba が固定の陽性確率を返すスタブ base 予測器。"""

    def __init__(self, p1):
        self._p1 = np.asarray(p1, dtype=float)

    def predict_proba(self, x):
        p1 = self._p1
        return np.column_stack([1.0 - p1, p1])


def test_meta_features_stacks_positive_probs():
    a = _StubProba([0.1, 0.9, 0.5])
    b = _StubProba([0.8, 0.2, 0.5])
    cm = CombinedModel([a, b])
    feats = cm._meta_features(np.zeros((3, 2)))
    assert feats.shape == (3, 2)
    assert np.allclose(feats[:, 0], [0.1, 0.9, 0.5])
    assert np.allclose(feats[:, 1], [0.8, 0.2, 0.5])


def test_fit_predict_roundtrip_shape_and_range():
    rng = np.random.default_rng(0)
    n = 200
    # base0 が正解に強く相関、base1 はノイズ → meta は base0 を重視するはず
    y = (rng.random(n) > 0.5).astype(int)
    p_good = np.clip(y * 0.7 + rng.normal(0, 0.1, n) + 0.15, 0.01, 0.99)
    p_noise = rng.random(n)
    cm = CombinedModel([_StubProba(p_good), _StubProba(p_noise)])
    cm.fit(np.zeros((n, 1)), y)
    proba = cm.predict_proba(np.zeros((n, 1)))
    assert proba.shape == (n, 2)
    assert np.all(proba >= 0.0) and np.all(proba <= 1.0)
    assert np.allclose(proba.sum(axis=1), 1.0)
    # 情報のある base0 の meta 係数が、ノイズ base1 より大きい
    coef = cm.meta_model.coef_[0]
    assert coef[0] > coef[1]


def test_empty_base_predictors_raises():
    with pytest.raises(ValueError):
        CombinedModel([])


def test_custom_meta_model_used():
    from sklearn.tree import DecisionTreeClassifier

    meta = DecisionTreeClassifier(max_depth=2, random_state=0)
    cm = CombinedModel([_StubProba([0.2, 0.8]), _StubProba([0.3, 0.7])], meta_model=meta)
    cm.fit(np.zeros((2, 1)), [0, 1])
    assert cm.meta_model is meta
    assert cm.predict_proba(np.zeros((2, 1))).shape == (2, 2)


def test_nn_derived_predictor_uses_scaler_and_model(monkeypatch):
    """NnDerivedPredictor が derive_nn_input で NN 入力を作り、NnWinModel に渡す。"""
    import src.training._stacking_model as sm

    captured = {}

    def fake_derive(scaler, x):
        captured["scaler"] = scaler
        captured["x"] = x
        return np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32)

    class _NnStub:
        def predict_proba(self, arr):
            captured["arr"] = arr
            return np.column_stack([[0.4, 0.6], [0.6, 0.4]])

    monkeypatch.setattr(sm, "derive_nn_input", fake_derive)
    pred = NnDerivedPredictor(_NnStub(), nn_scaler="SCALER")
    out = pred.predict_proba("XDF")
    assert captured["scaler"] == "SCALER" and captured["x"] == "XDF"
    assert np.allclose(captured["arr"], [[1.0, 2.0], [3.0, 4.0]])
    assert out.shape == (2, 2)
