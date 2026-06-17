"""base 学習器選択設定 DTO。

SUPPORTED_MODELS のいずれかを models タプルで指定し、
各モデルのデフォルトパラメータ・チューニング探索空間を管理する。
"""

from __future__ import annotations

import dataclasses
import json
import logging
from dataclasses import field

logger = logging.getLogger(__name__)

SUPPORTED_MODELS = ("lightgbm", "xgboost", "catboost", "nn")

DEFAULT_XGB_PARAMS = {
    "objective": "binary:logistic",
    "eval_metric": "logloss",
    "n_estimators": 500,
    "learning_rate": 0.05,
    "max_depth": 6,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "seed": 100,
    "tree_method": "hist",
}
DEFAULT_CATBOOST_PARAMS = {
    "iterations": 500,
    "learning_rate": 0.05,
    "depth": 6,
    "eval_metric": "Logloss",
    "random_seed": 100,
    "verbose": 0,
}
DEFAULT_NN_PARAMS = {
    "hidden_dims": [128],
    "epochs": 20,
    "lr": 1e-3,
    "batch_size": 256,
}
DEFAULT_XGB_SEARCH_SPACE = {
    "n_estimators": [100, 2000],
    "learning_rate": [0.005, 0.3],
    "max_depth": [3, 10],
    "subsample": [0.5, 1.0],
    "colsample_bytree": [0.5, 1.0],
    "min_child_weight": [1, 50],
    "gamma": [0.0, 5.0],
    "reg_alpha": [1e-8, 10.0],
    "reg_lambda": [1e-8, 10.0],
}
DEFAULT_CATBOOST_SEARCH_SPACE = {
    "iterations": [100, 2000],
    "learning_rate": [0.005, 0.3],
    "depth": [4, 10],
    "l2_leaf_reg": [1.0, 10.0],
    "bagging_temperature": [0.0, 1.0],
    "random_strength": [1e-8, 10.0],
}


@dataclasses.dataclass(frozen=True)
class BaseModelsConfig:
    """base 学習器の選択・チューニング設定（frozen DTO）。

    models=("lightgbm",) がデフォルトで、既存の動作を完全に保持する。
    """

    models: tuple = ("lightgbm",)
    tune_per_model: bool = False
    n_trials: int = 50
    timeout: float | None = None
    xgboost_params: dict = field(default_factory=lambda: dict(DEFAULT_XGB_PARAMS))
    catboost_params: dict = field(default_factory=lambda: dict(DEFAULT_CATBOOST_PARAMS))
    nn_params: dict = field(default_factory=lambda: dict(DEFAULT_NN_PARAMS))
    xgboost_search_space: dict = field(default_factory=lambda: dict(DEFAULT_XGB_SEARCH_SPACE))
    catboost_search_space: dict = field(default_factory=lambda: dict(DEFAULT_CATBOOST_SEARCH_SPACE))

    def to_dict(self):
        return dataclasses.asdict(self)


def from_dict(raw: dict) -> BaseModelsConfig:
    """辞書から BaseModelsConfig を生成する（不明キーは無視、不足はデフォルト補完）。"""
    known_fields = {f.name for f in dataclasses.fields(BaseModelsConfig)}
    filtered = {k: v for k, v in raw.items() if k in known_fields}

    if "models" in filtered:
        filtered["models"] = tuple(filtered["models"])

    # dict フィールドはデフォルトとマージ（指定キーで上書き）
    for key, default in (
        ("xgboost_params", DEFAULT_XGB_PARAMS),
        ("catboost_params", DEFAULT_CATBOOST_PARAMS),
        ("nn_params", DEFAULT_NN_PARAMS),
        ("xgboost_search_space", DEFAULT_XGB_SEARCH_SPACE),
        ("catboost_search_space", DEFAULT_CATBOOST_SEARCH_SPACE),
    ):
        if key in filtered:
            merged = dict(default)
            merged.update(filtered[key])
            filtered[key] = merged

    return BaseModelsConfig(**filtered)


def load_base_models_config(path: str) -> BaseModelsConfig:
    """JSON ファイルから BaseModelsConfig を読み込む。"""
    with open(path) as f:
        raw = json.load(f)
    return from_dict(raw)
