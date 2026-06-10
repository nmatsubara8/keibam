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

    def fit(self, x_base, y_base, x_meta, y_meta, base_sample_weights=None) -> "StackingModel":
        """base 学習器を base_train で学習し、meta_train の OOF 予測で meta 学習器を学習。

        base_sample_weights: base_models と同じ長さのリスト。各要素は当該 base 学習器に
        渡す sample_weight（None なら等重み）。§2 EV境界重みを LightGBM base にのみ
        適用する用途を想定。None の場合は全 base 学習器を等重みで学習（後方互換）。
        """
        if base_sample_weights is not None and len(base_sample_weights) != len(self._base_models):
            raise ValueError("base_sample_weights の長さが base_models と一致しません。")
        for i, model in enumerate(self._base_models):
            sw = base_sample_weights[i] if base_sample_weights is not None else None
            if sw is not None:
                model.fit(x_base, y_base, sample_weight=sw)
            else:
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
