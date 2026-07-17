"""NN 単体（GBDT スタックと分離）の学習・保存・読込。

分離NN + 遅延スタッキング（`src/training/_combined_model.py`）用。NN を GBDT スタックへ
同時投入すると 2系統 PreparedFeatures でメモリが倍化するため、NN だけを別ルートで学習して
保存する。NnWinModel は max_train_rows 上限＋ミニバッチで省メモリなので全データでも回せる。

学習: `train_nn_standalone(datasets, nn_params)` → (NnWinModel, metrics)。
保存: `save_nn_standalone(...)` → models/<date>/<version>__nn_standalone.pickle（nn_scaler 同梱）。
"""
from __future__ import annotations

import datetime
import os
from typing import Any

import dill

_NN_KWARG_KEYS = (
    "hidden_dims", "epochs", "lr", "batch_size", "max_train_rows",
    "arch", "dropout", "conv_channels", "kernel_size", "pre_norm", "weight_decay",
)


def _as_1d(y):
    return y.values if hasattr(y, "values") else y


def train_nn_standalone(datasets, nn_params: dict | None = None, pos_weight: float | None = None):
    """DataSplitter の NN ストリームで NnWinModel を単体学習し、(model, metrics) を返す。

    datasets は PreparedFeatures 由来（has_nn_stream=True）であること。X_train で学習し
    X_test で AUC を評価する。GBDT スタックは一切構築しない（NN だけ）。
    """
    from sklearn.metrics import roc_auc_score

    from src.constants._bet_thresholds import TrainingWeights
    from src.training._nn_win_model import NnWinModel
    from src.training._stacking_model import derive_nn_input

    if not getattr(datasets, "has_nn_stream", False):
        raise ValueError("NN ストリームがありません（PreparedFeatures を渡してください）。")

    scaler = datasets.nn_scaler
    cards = datasets.nn_categorical_cardinalities or {}
    kw = {k: v for k, v in dict(nn_params or {}).items() if k in _NN_KWARG_KEYS}
    pw = pos_weight if pos_weight is not None else TrainingWeights.SCALE_POS_WEIGHT

    model = NnWinModel(
        categorical_cardinalities=cards, n_numeric=len(scaler.numeric_cols), pos_weight=pw, **kw
    )
    model.fit(derive_nn_input(scaler, datasets.X_train), _as_1d(datasets.y_train))

    preds = model.predict_proba(derive_nn_input(scaler, datasets.X_test))[:, 1]
    auc = float(roc_auc_score(_as_1d(datasets.y_test), preds))
    return model, {"auc_test": auc}


def save_nn_standalone(
    nn_model: Any, nn_scaler: Any, version: str,
    suffix: str = "__nn_standalone", models_dir: str = "models",
) -> str:
    """NN 単体モデルと nn_scaler を dill で保存し、保存パスを返す。"""
    yyyymmdd = datetime.date.today().strftime("%Y%m%d")
    out_dir = os.path.join(models_dir, yyyymmdd)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{version}{suffix}.pickle")
    with open(path, "wb") as f:
        dill.dump({"nn_model": nn_model, "nn_scaler": nn_scaler}, f)
    return path


def load_nn_standalone(path: str):
    """save_nn_standalone で保存した (nn_model, nn_scaler) を復元する。"""
    with open(path, "rb") as f:
        obj = dill.load(f)
    return obj["nn_model"], obj["nn_scaler"]
