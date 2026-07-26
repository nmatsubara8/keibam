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


def test_suggest_nn_mlp_independent_layer_widths():
    ss = {"arch": ["mlp"], "n_layers": [2, 2], "layer_width": [256, 128]}
    t = _FakeTrial()
    params = _suggest_nn_params(t, ss)
    assert params["arch"] == "mlp"
    # 各中間層のユニット数を独立に探索: layer_width_0 / layer_width_1 が個別に suggest される
    assert "layer_width_0" in t.calls and "layer_width_1" in t.calls
    # 各層とも同じ候補プールから独立に選ぶ
    assert t.calls["layer_width_0"] == ("cat", [256, 128])
    assert t.calls["layer_width_1"] == ("cat", [256, 128])
    # FakeTrial は choices[0] を返すので両層とも 256（＝半減ファンネルではない）
    assert params["hidden_dims"] == [256, 256]


def test_suggest_nn_mlp_layer_count_matches_n_layers():
    # n_layers ぶんだけ layer_width_i が生成される（層数と独立幅の連動）
    class _T(_FakeTrial):
        def suggest_int(self, name, lo, hi):
            self.calls[name] = ("int", lo, hi)
            return 3  # n_layers=3 を返す

    ss = {"arch": ["mlp"], "n_layers": [1, 4], "layer_width": [64, 128]}
    t = _T()
    params = _suggest_nn_params(t, ss)
    assert len(params["hidden_dims"]) == 3
    assert {"layer_width_0", "layer_width_1", "layer_width_2"} <= set(t.calls)
    assert "layer_width_3" not in t.calls


def test_suggest_nn_cnn_branch():
    ss = {"arch": ["cnn"], "n_conv": [2, 2], "conv_width": [32], "kernel_size": [3]}
    params = _suggest_nn_params(_FakeTrial(), ss)
    assert params["arch"] == "cnn"
    assert params["conv_channels"] == [32, 64]  # width, 2*width
    assert params["kernel_size"] == 3
