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


class RFFLogisticClassifier:
    """Random Fourier Features(RBF 近似) + ロジスティック回帰 = 線形時間の近似カーネルロジ回帰。

    厳密カーネル法は O(n²)〜O(n³) のグラム行列が要り 163万行では非現実的。RFF は RBF カーネルを
    ``n_components`` 次元のランダム特徴で線形時間近似するため、数百万行でもスケールする。
    sklearn 互換（fit / predict_proba）。前段で欠損補完・標準化（カーネルは NaN・スケール非対応）。
    sample_weight は最終段のロジ回帰へ委譲する。
    """

    def __init__(self, n_components=500, gamma=0.1, C=1.0, class_weight=None, random_state=100):
        self.n_components = int(n_components)
        self.gamma = float(gamma)
        self.C = float(C)
        self.class_weight = class_weight
        self.random_state = random_state

    def _make_imputer(self):
        from sklearn.impute import SimpleImputer
        try:  # 全 NaN 列（category 由来）でも列数を保つ
            return SimpleImputer(strategy="median", keep_empty_features=True)
        except TypeError:  # 古い sklearn
            return SimpleImputer(strategy="median")

    def fit(self, x, y, sample_weight=None):
        from sklearn.kernel_approximation import RBFSampler
        from sklearn.linear_model import LogisticRegression
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import StandardScaler

        self.pipe_ = Pipeline([
            ("impute", self._make_imputer()),
            ("scale", StandardScaler()),
            ("rff", RBFSampler(gamma=self.gamma, n_components=self.n_components,
                               random_state=self.random_state)),
            ("logit", LogisticRegression(C=self.C, max_iter=1000, class_weight=self.class_weight)),
        ])
        if sample_weight is not None:
            self.pipe_.fit(x, y, logit__sample_weight=sample_weight)
        else:
            self.pipe_.fit(x, y)
        return self

    def predict_proba(self, x):
        return self.pipe_.predict_proba(x)


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
            # cfg.lightgbm_params が非空なら上書き（探索済み config を --base-models-config で
            # 固定運用する経路。空なら従来どおりチューナ/既定の lgb_params を使う）。
            params.update(getattr(cfg, "lightgbm_params", None) or {})
            params.setdefault("scale_pos_weight", scale_pos_weight)
            # LightGBM は category 型・.values を両方吸収するためラッパー不要
            specs.append(BaseModelSpec(lgb.LGBMClassifier(**params), name="LightGBM"))
        elif m == "xgboost":
            try:
                import xgboost as xgb

                from src.training._gpu_config import xgb_gpu_params

                params = dict(cfg.xgboost_params)
                params["scale_pos_weight"] = scale_pos_weight
                params.update(xgb_gpu_params())     # --gpu 時のみ device=cuda
                # _NumericArrayAdapter で category 型を float に正規化
                specs.append(BaseModelSpec(
                    _NumericArrayAdapter(xgb.XGBClassifier(**params)), name="XGBoost"
                ))
            except ImportError:
                logger.warning("xgboost 未導入のためスキップ")
        elif m == "catboost":
            try:
                from catboost import CatBoostClassifier

                from src.training._gpu_config import catboost_gpu_params

                params = dict(cfg.catboost_params)
                params["class_weights"] = [1.0, scale_pos_weight]
                params.update(catboost_gpu_params())    # --gpu 時のみ task_type=GPU
                # _NumericArrayAdapter で category 型を float に正規化
                specs.append(BaseModelSpec(
                    _NumericArrayAdapter(CatBoostClassifier(**params)), name="CatBoost"
                ))
            except ImportError:
                logger.warning("catboost 未導入のためスキップ")
        elif m == "kernel":
            params = dict(getattr(cfg, "kernel_params", None) or {})
            kmodel = RFFLogisticClassifier(
                n_components=params.get("n_components", 500),
                gamma=params.get("gamma", 0.1),
                C=params.get("C", 1.0),
                class_weight={0: 1.0, 1: scale_pos_weight},
            )
            # _NumericArrayAdapter で category 型を float に正規化（RFF は数値のみ）
            specs.append(BaseModelSpec(_NumericArrayAdapter(kmodel), name="KernelRFF"))
        elif m == "nn":
            # NN base は専用ストリーム（nn_scaler/cardinalities）が必要なため
            # train_with_stacking 側で構築する（ここでは何もしない）。
            logger.debug("NN base は train_with_stacking で構築されます")

    if not specs:
        raise RuntimeError("有効な base 学習器が 1 つもありません。")
    return specs


def build_meta_model(cfg, scale_pos_weight: float | None = None):
    """BaseModelsConfig から meta 学習器（スタッキング 2 段目）を構築する。

    meta 特徴量は base 予測確率の数列のみと低次元なので、GBDT meta は浅い既定で
    過学習を抑える。cfg.meta_params は選択 meta_model の既定値に上書きマージされる。

    Parameters
    ----------
    cfg : BaseModelsConfig
    scale_pos_weight : クラス不均衡補正係数。指定時は meta にも適用する
        （ユーザーが meta_params で明示済みなら上書きしない）。

    Returns
    -------
    sklearn 互換の分類器（fit(X, y) / predict_proba(X)[:, 1]）。
    """
    from ._base_models_config import DEFAULT_META_LGB_PARAMS

    meta = getattr(cfg, "meta_model", "logistic")
    user_params = dict(getattr(cfg, "meta_params", None) or {})

    if meta == "logistic":
        from sklearn.linear_model import LogisticRegression

        params: dict[str, Any] = {"max_iter": 1000, "random_state": 100}
        params.update(user_params)
        return LogisticRegression(**params)

    if meta == "lightgbm":
        import lightgbm as lgb

        params = dict(DEFAULT_META_LGB_PARAMS)
        if scale_pos_weight is not None:
            params.setdefault("scale_pos_weight", scale_pos_weight)
        params.update(user_params)
        return lgb.LGBMClassifier(**params)

    raise ValueError(f"未対応の meta_model: {meta!r}")
