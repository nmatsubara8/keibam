"""GBDT×DL スタッキング（Layer1 勝率モデル）。

base 学習器（LightGBM=数値分岐に強い / NN=血統・系列の非線形に強い）を時系列順の
base_train で学習し、meta_train 上の予測を特徴量として meta 学習器を学習する。
base 学習器・meta 学習器は DI で受け取り、本クラスは結合手順のみを担う。

## stream-aware 化（Phase 2）

base 学習器ごとに消費する特徴量ストリームが異なる:
- "gbdt": gbdt DataFrame をそのまま（内部で `.values`）。LightGBM/XGBoost/CatBoost。
- "nn":   gbdt DataFrame から `nn_scaler` で entity+numeric 列を抽出・標準化し、
          entity 列を整数コード（未知/-1 → 0）へ変換した float 配列。NnWinModel。

NN の entity/numeric 列は gbdt DataFrame 内に共存するため、推論時も gbdt 1 枚から
NN ストリームを内部導出でき、predict_proba(X) の単一引数契約は不変。

predict_proba は 2 列（[負, 正]）を返し、CalibratedModel / ScorePolicy と互換。
重い依存（torch/optuna）は持たず、base 学習器側に隔離する。
"""

from __future__ import annotations

import numpy as np
import pandas as pd


class StackingModel:
    """base 学習器群 + meta 学習器によるスタッキング。

    Parameters
    ----------
    base_models : predict_proba(X)[:, 1] を返す学習器のリスト。
    meta_model : meta 特徴量を入力に predict_proba を返す学習器（例: LogisticRegression）。
    base_streams : base_models と同じ長さの "gbdt"/"nn" リスト。None なら全 "gbdt"
        （後方互換）。
    nn_scaler : NnFeatureScaler。"nn" ストリームの入力導出に使用。
    nn_cat_cardinalities : {nn入力列index: カーディナリティ}（参照用に保持）。
    """

    def __init__(
        self,
        base_models: list,
        meta_model,
        base_streams: list | None = None,
        nn_scaler=None,
        nn_cat_cardinalities: dict | None = None,
    ) -> None:
        if not base_models:
            raise ValueError("base_models が空です。")
        self._base_models = base_models
        self._meta_model = meta_model
        self._streams = base_streams if base_streams is not None else ["gbdt"] * len(base_models)
        if len(self._streams) != len(base_models):
            raise ValueError("base_streams の長さが base_models と一致しません。")
        self._nn_scaler = nn_scaler
        self._nn_cat_cardinalities = nn_cat_cardinalities

    def _stream_input(self, x, stream: str) -> np.ndarray:
        """base 学習器のストリーム種別に応じて入力配列を導出する。"""
        if stream == "gbdt":
            return x.values if isinstance(x, pd.DataFrame) else x
        # stream == "nn": gbdt DataFrame から entity+numeric 列を抽出して数値配列化する
        if not isinstance(x, pd.DataFrame):
            raise ValueError("nn ストリームには DataFrame 入力が必要です（列名で抽出するため）")
        if self._nn_scaler is None:
            raise ValueError("nn ストリームには nn_scaler が必要です。")
        nn_df = self._nn_scaler.transform(x).reindex(columns=self._nn_scaler.feature_names)
        cols = []
        entity = set(self._nn_scaler.entity_cols)
        for c in self._nn_scaler.feature_names:
            col = nn_df[c]
            if c in entity and isinstance(col.dtype, pd.CategoricalDtype):
                # 未知/欠損（-1）を 0（未知バケット）へシフト
                cols.append((col.cat.codes.to_numpy() + 1).astype(np.float32))
            else:
                cols.append(np.asarray(col, dtype=np.float32))
        # NN は NaN を扱えない（§2 由来の欠損や定数列の標準化で NaN が出る）ため、
        # 標準化後の平均に相当する 0 で補完する。entity コードは NaN を含まない。
        return np.nan_to_num(np.column_stack(cols), nan=0.0, posinf=0.0, neginf=0.0)

    def fit(self, x_base, y_base, x_meta, y_meta, base_sample_weights=None) -> "StackingModel":
        """base 学習器を base_train で学習し、meta_train の OOF 予測で meta 学習器を学習。

        base_sample_weights: base_models と同じ長さのリスト。各要素は当該 base 学習器に
        渡す sample_weight（None なら等重み）。§2 EV境界重みを GBDT base にのみ
        適用する用途を想定。None の場合は全 base 学習器を等重みで学習（後方互換）。
        """
        if base_sample_weights is not None and len(base_sample_weights) != len(self._base_models):
            raise ValueError("base_sample_weights の長さが base_models と一致しません。")
        for i, model in enumerate(self._base_models):
            xi = self._stream_input(x_base, self._streams[i])
            sw = base_sample_weights[i] if base_sample_weights is not None else None
            if sw is not None:
                model.fit(xi, y_base, sample_weight=sw)
            else:
                model.fit(xi, y_base)
        meta_features = self._meta_features(x_meta)
        self._meta_model.fit(meta_features, np.asarray(y_meta))
        return self

    def _meta_features(self, x) -> np.ndarray:
        cols = [
            np.asarray(m.predict_proba(self._stream_input(x, self._streams[i])))[:, 1]
            for i, m in enumerate(self._base_models)
        ]
        return np.column_stack(cols)

    def predict_proba(self, x) -> np.ndarray:
        return np.asarray(self._meta_model.predict_proba(self._meta_features(x)))

    def base_predictions(self, x) -> list:
        """各 base 学習器の正例確率（確信度のモデル一致度算出に使用）。"""
        return [
            np.asarray(m.predict_proba(self._stream_input(x, self._streams[i])))[:, 1]
            for i, m in enumerate(self._base_models)
        ]
