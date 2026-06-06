"""NN 入力の数値特徴量を標準化するユーティリティ。

entity_cols (ID 系) は StandardScaler 対象外（Embedding 層が処理）。
numeric_cols のみ fit / transform を適用する。

DataSplitter が訓練データで fit_transform()、検証・テストは transform() のみ。
KeibaAI.nn_scaler として dill 保存し、推論時は transform() のみ呼ぶ
（LabelEncoder の train/inference 分離パターンと同じ）。
"""

from __future__ import annotations

from typing import List

import pandas as pd
from sklearn.preprocessing import StandardScaler


class NnFeatureScaler:
    """Entity Embedding 列を除く数値列を StandardScaler で標準化する。

    Parameters
    ----------
    entity_cols : Entity Embedding 対象列（スケーリング対象外）。
    numeric_cols : 標準化する数値列。空リストの場合はスケーリングをスキップ。
    """

    def __init__(self, entity_cols: List[str], numeric_cols: List[str]) -> None:
        self.entity_cols = list(entity_cols)
        self.numeric_cols = list(numeric_cols)
        self._scaler = StandardScaler()
        self._fitted = False

    def fit_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """訓練データ用: fit + transform を実行し entity_cols + numeric_cols の DataFrame を返す。"""
        result = self._select(df)
        if self.numeric_cols:
            result[self.numeric_cols] = self._scaler.fit_transform(
                result[self.numeric_cols].astype(float)
            )
            self._fitted = True
        return result

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """検証・推論データ用: 学習済みスケーラーで変換のみ実行する。

        df に numeric_cols の一部しかない場合でも、存在する列のみを変換して返す。
        """
        if self.numeric_cols and not self._fitted:
            raise RuntimeError("fit_transform を先に呼んでください。")
        result = self._select(df)
        if self._fitted:
            for i, col in enumerate(self.numeric_cols):
                if col in result.columns:
                    result[col] = (result[col].astype(float) - self._scaler.mean_[i]) / (
                        self._scaler.scale_[i] + 1e-10
                    )
        return result

    def _select(self, df: pd.DataFrame) -> pd.DataFrame:
        cols = [c for c in self.entity_cols + self.numeric_cols if c in df.columns]
        return df[cols].copy()

    @property
    def feature_names(self) -> List[str]:
        """NN 入力列名（entity_cols + numeric_cols の順）。"""
        return self.entity_cols + self.numeric_cols

    def __repr__(self) -> str:
        return (
            f"NnFeatureScaler(n_entity={len(self.entity_cols)}, "
            f"n_numeric={len(self.numeric_cols)}, fitted={self._fitted})"
        )
