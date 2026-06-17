"""base 学習器スペックの生成ファクトリ。

BaseModelsConfig と チューニング済み LightGBM パラメータを受け取り、
StackingModel に渡す BaseModelSpec リストを構築する。
"""

from __future__ import annotations

import dataclasses
import logging
from typing import Any

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class BaseModelSpec:
    """base 学習器の仕様。"""

    model: Any
    stream: str = "gbdt"
    weight: str | None = "ev"
    name: str = ""


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
            specs.append(BaseModelSpec(lgb.LGBMClassifier(**params), name="LightGBM"))
        elif m == "xgboost":
            try:
                import xgboost as xgb

                params = dict(cfg.xgboost_params)
                params["scale_pos_weight"] = scale_pos_weight
                specs.append(BaseModelSpec(xgb.XGBClassifier(**params), name="XGBoost"))
            except ImportError:
                logger.warning("xgboost 未導入のためスキップ")
        elif m == "catboost":
            try:
                from catboost import CatBoostClassifier

                params = dict(cfg.catboost_params)
                params["class_weights"] = [1.0, scale_pos_weight]
                specs.append(BaseModelSpec(CatBoostClassifier(**params), name="CatBoost"))
            except ImportError:
                logger.warning("catboost 未導入のためスキップ")
        elif m == "nn":
            logger.info("NN base は Phase 2 で実装予定のためスキップ")

    if not specs:
        raise RuntimeError("有効な base 学習器が 1 つもありません。")
    return specs
