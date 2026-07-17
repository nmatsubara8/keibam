"""NN 単体の保存・読込ラウンドトリップ（torch 不要の I/O 部分）。"""
from __future__ import annotations

from src.pipeline._nn_standalone import load_nn_standalone
from src.pipeline._nn_standalone import save_nn_standalone


def test_save_load_roundtrip(tmp_path):
    obj = {"w": [1, 2, 3]}  # NnWinModel の代わりに任意の picklable
    path = save_nn_standalone(obj, {"scaler": "S"}, "v1", models_dir=str(tmp_path))
    assert path.endswith("v1__nn_standalone.pickle")
    model, scaler = load_nn_standalone(path)
    assert model == obj and scaler == {"scaler": "S"}
