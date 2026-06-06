"""GBDT×DL スタッキング（Layer1 勝率モデル）。

base 学習器（LightGBM=数値分岐に強い / NN=血統・系列の非線形に強い）を時系列順の
base_train で学習し、meta_train 上の予測を特徴量として meta 学習器を学習する。
base 学習器・meta 学習器は DI で受け取り、本クラスは結合手順のみを担う。

predict_proba は 2 列（[負, 正]）を返し、CalibratedModel / ScorePolicy と互換。
重い依存（torch/optuna）は持たず、base 学習器側に隔離する。
"""

from __future__ import annotations

import numpy as np


class StackingModel:
    """base 学習器群 + meta 学習器によるスタッキング。

    Parameters
    ----------
    base_models : predict_proba(X)[:, 1] を返す学習器のリスト。
    meta_model : meta 特徴量を入力に predict_proba を返す学習器（例: LogisticRegression）。
    """

    def __init__(self, base_models: list, meta_model) -> None:
        if not base_models:
            raise ValueError("base_models が空です。")
        self._base_models = base_models
        self._meta_model = meta_model

    def fit(self, x_base, y_base, x_meta, y_meta) -> "StackingModel":
        for model in self._base_models:
            model.fit(x_base, y_base)
        meta_features = self._meta_features(x_meta)
        self._meta_model.fit(meta_features, np.asarray(y_meta))
        return self

    def _meta_features(self, x) -> np.ndarray:
        cols = [np.asarray(m.predict_proba(x))[:, 1] for m in self._base_models]
        return np.column_stack(cols)

    def predict_proba(self, x) -> np.ndarray:
        return np.asarray(self._meta_model.predict_proba(self._meta_features(x)))

    def base_predictions(self, x) -> list:
        """各 base 学習器の正例確率（確信度のモデル一致度算出に使用）。"""
        return [np.asarray(m.predict_proba(x))[:, 1] for m in self._base_models]
