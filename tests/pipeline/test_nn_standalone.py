"""NN 単体の保存・読込ラウンドトリップ（torch 不要の I/O 部分）と構造探索の配線。"""
from __future__ import annotations

import numpy as np
import pytest

from src.pipeline._nn_standalone import load_nn_standalone
from src.pipeline._nn_standalone import save_nn_standalone
from src.pipeline._nn_standalone import search_nn_standalone


def test_save_load_roundtrip(tmp_path):
    obj = {"w": [1, 2, 3]}  # NnWinModel の代わりに任意の picklable
    path = save_nn_standalone(obj, {"scaler": "S"}, "v1", models_dir=str(tmp_path))
    assert path.endswith("v1__nn_standalone.pickle")
    model, scaler = load_nn_standalone(path)
    assert model == obj and scaler == {"scaler": "S"}


class _FakeScaler:
    numeric_cols = ["a", "b", "c"]
    entity_cols = []


class _FakeDatasets:
    has_nn_stream = True
    nn_scaler = _FakeScaler()
    nn_categorical_cardinalities = {"horse": 10}

    def __init__(self, n=100):
        self.X_train = np.zeros((n, 3))
        self.y_train = np.arange(n) % 2


def test_search_nn_standalone_requires_nn_stream():
    class _NoStream:
        has_nn_stream = False

    with pytest.raises(ValueError):
        search_nn_standalone(_NoStream(), {"arch": ["mlp"]})


def test_search_nn_standalone_wires_tune_nn(monkeypatch):
    """derive_nn_input と tune_nn をモックし、80/20 分割・引数受け渡し・戻り値を検証する。"""
    import src.training._multi_model_tuner as mmt
    import src.training._stacking_model as sm

    captured = {}

    def fake_derive(scaler, X):
        return np.asarray(X)

    def fake_tune_nn(x_tr, y_tr, x_val, y_val, search_space, **kw):
        captured["n_tr"] = len(x_tr)
        captured["n_val"] = len(x_val)
        captured["search_space"] = search_space
        captured["n_numeric"] = kw.get("n_numeric")
        captured["n_trials"] = kw.get("n_trials")
        return {"arch": "mlp", "lr": 0.001, "hidden_dims": [128]}

    monkeypatch.setattr(sm, "derive_nn_input", fake_derive)
    monkeypatch.setattr(mmt, "tune_nn", fake_tune_nn)

    ds = _FakeDatasets(n=100)
    best = search_nn_standalone(ds, {"arch": ["mlp"]}, n_trials=7)

    assert best == {"arch": "mlp", "lr": 0.001, "hidden_dims": [128]}
    assert captured["n_tr"] == 80 and captured["n_val"] == 20  # 80/20 時系列分割
    assert captured["n_numeric"] == 3  # len(scaler.numeric_cols)
    assert captured["n_trials"] == 7
