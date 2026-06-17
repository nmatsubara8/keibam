"""base 学習器スペックの生成ファクトリ。

BaseModelsConfig と チューニング済み LightGBM パラメータを受け取り、
StackingModel に渡す BaseModelSpec リストを構築する。
"""

from __future__ import annotations

import dataclasses
import logging
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class BaseModelSpec:
    """base 学習器の仕様。"""

    model: Any
    stream: str = "gbdt"
    weight: str | None = "ev"
    name: str = ""


class _NumericArrayAdapter:
    """pandas category 型を受け付けない学習器（XGBoost/CatBoost）向けの sklearn 互換ラッパー。

    StackingModel は学習時に ``DataFrame.values``（object 配列）、推論時に category 型を含む
    DataFrame を渡すため、両者を一貫して数値 float 配列へ正規化する必要がある。
    入力をまず ``to_numpy()``（DataFrame の場合）で object 配列へ落とし、列ごとに
    ``pd.to_numeric`` で数値化する。これにより学習・推論で同一エンコードになり、
    category 型による CatBoost/XGBoost のエラーも回避する（LightGBM は本ラッパー不要）。
    """

    def __init__(self, model: Any) -> None:
        self._model = model

    @staticmethod
    def _coerce(x) -> np.ndarray:
        arr = x.to_numpy() if isinstance(x, pd.DataFrame) else np.asarray(x)
        if arr.dtype != object and np.issubdtype(arr.dtype, np.number):
            return arr.astype(np.float32)
        # object 配列（category ラベル混在）は列ごとに数値化（非数値は NaN）
        return pd.DataFrame(arr).apply(pd.to_numeric, errors="coerce").to_numpy(dtype=np.float32)

    def fit(self, x, y, sample_weight=None):
        xn = self._coerce(x)
        if sample_weight is not None:
            self._model.fit(xn, y, sample_weight=sample_weight)
        else:
            self._model.fit(xn, y)
        return self

    def predict_proba(self, x) -> np.ndarray:
        return np.asarray(self._model.predict_proba(self._coerce(x)))

    @property
    def feature_importances_(self):
        return self._model.feature_importances_


def build_base_models(
    cfg,
    lgb_params: dict,
    scale_pos_weight: float,
) -> list[BaseModelSpec]:
    """BaseModelsConfig から BaseModelSpec リストを構築する。

    Parameters
    ----------
    cfg : BaseModelsConfig
    lgb_params : チューニング済み LightGBM パラメータ
    scale_pos_weight : クラス不均衡補正係数

    Returns
    -------
    list[BaseModelSpec] — 少なくとも 1 つの有効な spec を含む。
    有効なものが 1 つも無ければ RuntimeError を送出する。
    """
    specs = []
    for m in cfg.models:
        if m == "lightgbm":
            import lightgbm as lgb

            params = dict(lgb_params)
            params.setdefault("scale_pos_weight", scale_pos_weight)
            # LightGBM は category 型・.values を両方吸収するためラッパー不要
            specs.append(BaseModelSpec(lgb.LGBMClassifier(**params), name="LightGBM"))
        elif m == "xgboost":
            try:
                import xgboost as xgb

                params = dict(cfg.xgboost_params)
                params["scale_pos_weight"] = scale_pos_weight
                # _NumericArrayAdapter で category 型を float に正規化
                specs.append(BaseModelSpec(
                    _NumericArrayAdapter(xgb.XGBClassifier(**params)), name="XGBoost"
                ))
            except ImportError:
                logger.warning("xgboost 未導入のためスキップ")
        elif m == "catboost":
            try:
                from catboost import CatBoostClassifier

                params = dict(cfg.catboost_params)
                params["class_weights"] = [1.0, scale_pos_weight]
                # _NumericArrayAdapter で category 型を float に正規化
                specs.append(BaseModelSpec(
                    _NumericArrayAdapter(CatBoostClassifier(**params)), name="CatBoost"
                ))
            except ImportError:
                logger.warning("catboost 未導入のためスキップ")
        elif m == "nn":
            # NN base は専用ストリーム（nn_scaler/cardinalities）が必要なため
            # train_with_stacking 側で構築する（ここでは何もしない）。
            logger.debug("NN base は train_with_stacking で構築されます")

    if not specs:
        raise RuntimeError("有効な base 学習器が 1 つもありません。")
    return specs
