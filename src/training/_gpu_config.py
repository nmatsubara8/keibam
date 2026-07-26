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


def lgb_gpu_params() -> dict:
    """LightGBM の GPU パラメータ断片（要 GPU 有効ビルド）。

    LightGBM は pip 既定が CPU ビルドで device を渡すと落ちるため、xgboost/catboost の
    ``KEIBA_USE_GPU`` とは別に ``KEIBA_LGB_GPU`` で明示 opt-in する（GPU ビルドを入れた人だけ）:
      - ``"gpu"``  : OpenCL バックエンド（``-DUSE_GPU=ON`` ビルド。**nvcc 不要＝gcc15 でも可**）。
      - ``"cuda"`` : CUDA バックエンド（``-DUSE_CUDA=ON`` ビルド。nvcc が gcc<=13 を要求）。
    未設定・不正値は CPU（空 dict）。速度重視なら別途 ``max_bin<=63`` を設定するとよい（本関数は
    精度を勝手に変えないよう device_type のみ返す）。
    """
    mode = os.environ.get("KEIBA_LGB_GPU", "").strip().lower()
    if mode == "gpu":
        return {"device_type": "gpu"}
    if mode == "cuda":
        return {"device_type": "cuda"}
    return {}
