"""GBDT(xgboost/catboost) の GPU 利用の共通判定。

device は「モデルのハイパーパラメータ」ではなく実行環境の属性なので、多数の関数シグネチャに
通さず環境変数 ``KEIBA_USE_GPU`` で opt-in する（CLI ``retrain --gpu`` が 1 を立てる）。
tuner(_multi_model_tuner) と factory(_base_model_factory) の xgboost/catboost 生成時に参照する。
未設定なら CPU。lightgbm は pip 既定が CPU ビルドのため対象外（CPU 据え置き）。NN は torch 側で
``cuda`` を自動検出するため本モジュールとは独立。
"""
from __future__ import annotations

import os


def gpu_enabled() -> bool:
    """GBDT を GPU で動かすか（環境変数 KEIBA_USE_GPU で opt-in）。"""
    return os.environ.get("KEIBA_USE_GPU", "").lower() in ("1", "true", "yes", "on")


def xgb_gpu_params() -> dict:
    """xgboost の GPU パラメータ断片（有効時のみ ``device=cuda``・無効時は空）。"""
    return {"device": "cuda"} if gpu_enabled() else {}


def catboost_gpu_params() -> dict:
    """catboost の GPU パラメータ断片（有効時のみ ``task_type=GPU``・無効時は空）。"""
    return {"task_type": "GPU"} if gpu_enabled() else {}
