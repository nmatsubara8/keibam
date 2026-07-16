"""_multi_model_tuner の探索空間サンプリング（NN suggest）の単体テスト。"""
from __future__ import annotations

from src.training._multi_model_tuner import _suggest_nn_params


class _FakeTrial:
    """suggest_* を記録するだけのスタブ trial（Optuna 不要で探索空間の配線を検証）。"""

    def __init__(self):
        self.calls: dict = {}

    def suggest_categorical(self, name, choices):
        self.calls[name] = ("cat", list(choices))
        return choices[0]

    def suggest_float(self, name, lo, hi, log=False):
        self.calls[name] = ("float", lo, hi, log)
        return lo

    def suggest_int(self, name, lo, hi):
        self.calls[name] = ("int", lo, hi)
        return lo


def test_suggest_nn_includes_weight_decay_when_in_space():
    ss = {"arch": ["mlp"], "weight_decay": [1e-7, 1e-3], "n_layers": [1, 3], "layer_width": [128]}
    t = _FakeTrial()
    params = _suggest_nn_params(t, ss)
    assert "weight_decay" in params
    # log スケールで探索される
    assert t.calls["weight_decay"] == ("float", 1e-7, 1e-3, True)


def test_suggest_nn_omits_weight_decay_when_absent():
    # 後方互換: 空間に weight_decay が無ければキー自体を出さない（＝モデル既定 0.0）
    ss = {"arch": ["mlp"], "n_layers": [1, 3], "layer_width": [128]}
    params = _suggest_nn_params(_FakeTrial(), ss)
    assert "weight_decay" not in params


def test_suggest_nn_mlp_builds_hidden_dims():
    ss = {"arch": ["mlp"], "n_layers": [2, 2], "layer_width": [256]}
    params = _suggest_nn_params(_FakeTrial(), ss)
    assert params["arch"] == "mlp"
    # width=256, n_layers=2 → [256, 128]（段階的に半減、最小32）
    assert params["hidden_dims"] == [256, 128]


def test_suggest_nn_cnn_branch():
    ss = {"arch": ["cnn"], "n_conv": [2, 2], "conv_width": [32], "kernel_size": [3]}
    params = _suggest_nn_params(_FakeTrial(), ss)
    assert params["arch"] == "cnn"
    assert params["conv_channels"] == [32, 64]  # width, 2*width
    assert params["kernel_size"] == 3
