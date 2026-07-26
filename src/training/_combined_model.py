"""分離学習した base 予測器を meta 学習器で融合する遅延スタッキング（late fusion）。

背景: NN を GBDT スタックへ同時に組み込むと NN 2 系統 PreparedFeatures でメモリが倍化し、
全データ学習が OOM する。そこで **GBDT スタック（nn 抜き）** と **NN 単体** を別々に学習し、
予測時に各々の確率を個別計算して meta 学習器（[p_gbdt, p_nn] → logistic 等）で融合する。
これにより各モデルは自分のデータだけ抱えれば済み、全データでも学習できる。

CombinedModel は sklearn 互換（fit / predict_proba(X)->(n,2)）で、既存の effective_model
契約にそのまま差し込める。base_predictors は各々 predict_proba(X)->(n,2) を持てば何でもよく、
GBDT スタック（CalibratedModel）と `NnDerivedPredictor`（NnWinModel + nn_scaler）を渡す想定。
"""

from __future__ import annotations

from typing import Any
from typing import Sequence

import numpy as np


class NnDerivedPredictor:
    """NnWinModel を gbdt DataFrame から予測できるようにする薄いアダプタ。

    NnWinModel は NN ストリーム（float 配列）を入力に取るため、gbdt 特徴 DataFrame から
    `derive_nn_input(nn_scaler, X)` で内部導出してから予測する（StackingModel の NN base と
    同じ契約＝推論時も gbdt 1 枚から再構成できる）。
    """

    def __init__(self, nn_model: Any, nn_scaler: Any) -> None:
        self.nn_model = nn_model
        self.nn_scaler = nn_scaler

    def predict_proba(self, x) -> np.ndarray:
        from ._stacking_model import derive_nn_input

        return np.asarray(self.nn_model.predict_proba(derive_nn_input(self.nn_scaler, x)))


class CombinedModel:
    """複数 base 予測器を meta 学習器で融合する（遅延スタッキング）。

    Parameters
    ----------
    base_predictors : predict_proba(X)->(n,2) を持つ予測器の列（例: GBDTスタック, NN派生予測器）。
    meta_model : [p0, p1, ...]（各 base の陽性確率）を入力に fit / predict_proba する sklearn
        互換学習器。既定は LogisticRegression（2 特徴なので過学習しにくい浅い学習器）。

    fit(X_holdout, y_holdout) で meta を holdout の base 予測に合わせて学習する（base 自体は
    学習済みで再学習しない＝リーク回避のため base の学習に使っていない holdout を渡すこと）。
    """

    def __init__(self, base_predictors: Sequence[Any], meta_model: Any = None) -> None:
        if not base_predictors:
            raise ValueError("base_predictors が空です（最低 1 つ必要）。")
        self.base_predictors = list(base_predictors)
        if meta_model is None:
            from sklearn.linear_model import LogisticRegression

            meta_model = LogisticRegression(max_iter=1000, random_state=100)
        self.meta_model = meta_model

    def _meta_features(self, x) -> np.ndarray:
        """各 base の陽性確率 p[:,1] を列に並べた meta 入力（n, n_base）を作る。"""
        cols = [np.asarray(p.predict_proba(x))[:, 1] for p in self.base_predictors]
        return np.column_stack(cols)

    def fit(self, x, y, sample_weight=None) -> "CombinedModel":
        feats = self._meta_features(x)
        if sample_weight is not None:
            self.meta_model.fit(feats, y, sample_weight=sample_weight)
        else:
            self.meta_model.fit(feats, y)
        return self

    def predict_proba(self, x) -> np.ndarray:
        return np.asarray(self.meta_model.predict_proba(self._meta_features(x)))
