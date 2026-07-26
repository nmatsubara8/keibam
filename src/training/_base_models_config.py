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

SUPPORTED_MODELS = ("lightgbm", "xgboost", "catboost", "nn", "kernel")

# kernel: Random Fourier Features(RBF近似) + ロジスティック回帰 = 線形時間の近似カーネルロジ回帰。
# 厳密カーネルは O(n²) で 163万行では不可能なため RFF で近似（数百万行でもスケール）。
DEFAULT_KERNEL_PARAMS = {
    "n_components": 500,   # RFF 次元（大=近似精度↑・メモリ n×n_components）
    "gamma": 0.1,          # RBF バンド幅（標準化後の特徴に対して）。要チューニング
    "C": 1.0,              # ロジ回帰の逆正則化強度
}

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
# meta 学習器（スタッキングの 2 段目）。meta 特徴量は base 予測確率の
# 3〜4 列のみと低次元なので、過学習を避けるため浅い GBDT を既定とする。
SUPPORTED_META_MODELS = ("logistic", "lightgbm")
DEFAULT_META_LGB_PARAMS = {
    "objective": "binary",
    "n_estimators": 100,
    "learning_rate": 0.05,
    "num_leaves": 3,  # 高相関・低次元な meta 特徴量では過学習を避け極浅に保つ
    "max_depth": 3,
    "min_child_samples": 50,
    "subsample": 0.8,
    "colsample_bytree": 1.0,
    "reg_lambda": 5.0,
    "random_state": 100,
    "verbose": -1,
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
DEFAULT_NN_SEARCH_SPACE = {
    "arch": ["mlp"],
    "lr": [1e-4, 5e-3],
    "dropout": [0.1, 0.5],
    "weight_decay": [1e-7, 1e-3],  # Adam L2（過学習抑制の主ノブ。log スケール探索）
    "batch_size": [256, 512, 1024],
    "pre_norm": ["layer_norm", "batch_norm", "none"],
    "n_layers": [1, 4],            # より深い MLP まで許容
    "layer_width": [64, 128, 256, 512],  # より広い層まで許容
    "n_conv": [1, 3],
    "conv_width": [16, 32, 64],
    "kernel_size": [3, 5],
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
    # 探索対象モデルの絞り込み（空=全モデル）。指定したモデルだけ探索し、他は stored/既定値で固定。
    # CLI --tune-models で設定する。値は models の部分集合（lightgbm/xgboost/catboost/nn）。
    tune_only: tuple = ()
    n_trials: int = 50
    timeout: float | None = None
    # LightGBM の明示パラメータ。既定は空＝チューナ/既定値に委ねる。探索済み config
    # （tuned_base_models.json）はここに best を載せ、--base-models-config で固定運用できる。
    lightgbm_params: dict = field(default_factory=dict)
    xgboost_params: dict = field(default_factory=lambda: dict(DEFAULT_XGB_PARAMS))
    catboost_params: dict = field(default_factory=lambda: dict(DEFAULT_CATBOOST_PARAMS))
    nn_params: dict = field(default_factory=lambda: dict(DEFAULT_NN_PARAMS))
    kernel_params: dict = field(default_factory=lambda: dict(DEFAULT_KERNEL_PARAMS))
    xgboost_search_space: dict = field(default_factory=lambda: dict(DEFAULT_XGB_SEARCH_SPACE))
    catboost_search_space: dict = field(default_factory=lambda: dict(DEFAULT_CATBOOST_SEARCH_SPACE))
    nn_search_space: dict = field(default_factory=lambda: dict(DEFAULT_NN_SEARCH_SPACE))
    # NN チューニング専用設定（1 trial の学習を軽くして探索回数を稼ぐ）
    nn_tune_trials: int = 12
    nn_tune_epochs: int = 15
    nn_tune_max_rows: int | None = 120000
    # meta 学習器（"logistic"=従来の LogisticRegression / "lightgbm"=浅い GBDT meta）。
    # meta_params は選択した meta_model の既定パラメータに上書きマージされる。
    meta_model: str = "logistic"
    meta_params: dict = field(default_factory=dict)

    def to_dict(self):
        return dataclasses.asdict(self)


def from_dict(raw: dict) -> BaseModelsConfig:
    """辞書から BaseModelsConfig を生成する（不明キーは無視、不足はデフォルト補完）。"""
    known_fields = {f.name for f in dataclasses.fields(BaseModelsConfig)}
    filtered = {k: v for k, v in raw.items() if k in known_fields}

    if "models" in filtered:
        filtered["models"] = tuple(filtered["models"])
    if "tune_only" in filtered:
        filtered["tune_only"] = tuple(filtered["tune_only"])

    if "meta_model" in filtered:
        meta = str(filtered["meta_model"]).lower()
        if meta not in SUPPORTED_META_MODELS:
            raise ValueError(
                f"未対応の meta_model: {filtered['meta_model']!r}（対応: {SUPPORTED_META_MODELS}）"
            )
        filtered["meta_model"] = meta

    # dict フィールドはデフォルトとマージ（指定キーで上書き）
    for key, default in (
        ("xgboost_params", DEFAULT_XGB_PARAMS),
        ("catboost_params", DEFAULT_CATBOOST_PARAMS),
        ("nn_params", DEFAULT_NN_PARAMS),
        ("kernel_params", DEFAULT_KERNEL_PARAMS),
        ("xgboost_search_space", DEFAULT_XGB_SEARCH_SPACE),
        ("catboost_search_space", DEFAULT_CATBOOST_SEARCH_SPACE),
        ("nn_search_space", DEFAULT_NN_SEARCH_SPACE),
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
