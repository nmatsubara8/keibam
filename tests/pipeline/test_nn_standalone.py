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


class _FakeTrial:
    params = {"arch": "mlp", "n_layers": 1, "layer_width": 128, "lr": 0.001}


class _FakeStudy:
    best_trial = _FakeTrial()
    best_value = 0.77


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
        captured["warm"] = kw.get("warm_start_params")
        assert kw.get("return_study") is True
        return {"arch": "mlp", "lr": 0.001, "hidden_dims": [128]}, _FakeStudy()

    monkeypatch.setattr(sm, "derive_nn_input", fake_derive)
    monkeypatch.setattr(mmt, "tune_nn", fake_tune_nn)

    ds = _FakeDatasets(n=100)
    warm = [{"arch": "mlp", "n_layers": 2, "layer_width": 256}]
    result = search_nn_standalone(ds, {"arch": ["mlp"]}, n_trials=7, warm_start_params=warm)

    assert result["nn_params"] == {"arch": "mlp", "lr": 0.001, "hidden_dims": [128]}
    assert result["optuna_params"] == _FakeTrial.params  # 生 suggest（ウォームスタート用）
    assert result["val_auc"] == 0.77
    assert captured["n_tr"] == 80 and captured["n_val"] == 20  # 80/20 時系列分割
    assert captured["n_numeric"] == 3  # len(scaler.numeric_cols)
    assert captured["n_trials"] == 7
    assert captured["warm"] == warm  # ウォームスタートが tune_nn へ渡る


def test_nn_leaderboard_topk_and_dedup(tmp_path):
    """台帳は auc_test 上位 top_k を保持し、同一構造は高い auc だけ残す（単純上書きしない）。"""
    from src.pipeline._nn_standalone import load_nn_leaderboard
    from src.pipeline._nn_standalone import nn_leaderboard_path
    from src.pipeline._nn_standalone import update_nn_leaderboard

    path = nn_leaderboard_path(str(tmp_path))

    def mk(auc, sig):
        return {"version": f"v{auc}", "auc_test": auc,
                "nn_params": {"hidden_dims": [sig]}, "optuna_params": {"layer_width": sig}}

    # 6 個投入 → top_k=5 に切られる。auc 降順で保持。
    for auc, sig in [(0.80, 64), (0.82, 128), (0.79, 256), (0.83, 512), (0.81, 32), (0.78, 16)]:
        board = update_nn_leaderboard(path, mk(auc, sig), top_k=5)
    aucs = [e["auc_test"] for e in board]
    assert aucs == sorted(aucs, reverse=True) and len(board) == 5
    assert 0.78 not in aucs  # 最下位は落ちる

    # 同一構造（layer_width=128）を高 auc で再投入 → 重複せず auc が更新される。
    board = update_nn_leaderboard(path, mk(0.90, 128), top_k=5)
    sig128 = [e for e in board if e["optuna_params"]["layer_width"] == 128]
    assert len(sig128) == 1 and sig128[0]["auc_test"] == 0.90

    # 永続化されている（読み直して一致）。
    assert load_nn_leaderboard(path) == board


def test_load_nn_leaderboard_missing_returns_empty(tmp_path):
    from src.pipeline._nn_standalone import load_nn_leaderboard

    assert load_nn_leaderboard(str(tmp_path / "nope.json")) == []
