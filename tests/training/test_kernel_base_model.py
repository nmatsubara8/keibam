"""カーネル法(RFF+ロジスティック回帰) base learner の単体テスト。

厳密カーネルは O(n²) で大規模不可のため RFF で線形時間近似する。非線形構造を捉えること・
欠損/sample_weight を扱えること・スタッキングの base として組めることを固定する。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

from src.training._base_model_factory import RFFLogisticClassifier, build_base_models
from src.training._base_models_config import (
    SUPPORTED_MODELS,
    BaseModelsConfig,
    from_dict,
)


def _nonlinear_data(n=3000, seed=0):
    rng = np.random.default_rng(seed)
    X = pd.DataFrame({
        "a": rng.normal(size=n),
        "b": rng.normal(size=n),
        "c": rng.choice([np.nan, 1.0, 2.0], size=n),          # 欠損入り
        "cat": pd.Categorical(rng.choice(["x", "y", "z"], size=n)),  # category(→NaN化)
    })
    # 純粋な非線形（交互作用 a*b と 2次項 a^2）＝線形モデルには見えない構造
    logit = 1.5 * X["a"] * X["b"] - 0.8 * X["a"] ** 2 + rng.normal(scale=0.5, size=n)
    y = (logit > np.median(logit)).astype(int).values
    return X, y


def test_kernel_in_supported_models():
    assert "kernel" in SUPPORTED_MODELS


def test_build_base_models_includes_kernel():
    specs = build_base_models(BaseModelsConfig(models=("kernel",)), lgb_params={}, scale_pos_weight=3.0)
    assert [s.name for s in specs] == ["KernelRFF"]


def test_kernel_captures_nonlinear_structure():
    X, y = _nonlinear_data()
    m = RFFLogisticClassifier(n_components=500, gamma=0.1)
    m.fit(X.select_dtypes("number").fillna(0.0).to_numpy(), y)
    auc = roc_auc_score(y, m.predict_proba(X.select_dtypes("number").fillna(0.0).to_numpy())[:, 1])
    assert auc > 0.75  # 線形ロジ回帰なら a*b/a^2 を拾えず ~0.5。RFF は非線形を捉える


def test_kernel_handles_nan_category_and_sample_weight():
    X, y = _nonlinear_data()
    spec = build_base_models(BaseModelsConfig(models=("kernel",)), lgb_params={}, scale_pos_weight=3.0)[0]
    sw = np.random.default_rng(1).uniform(0.5, 2.0, size=len(y))
    spec.model.fit(X, y, sample_weight=sw)  # adapter が category→float 正規化、欠損は内部で補完
    p = spec.model.predict_proba(X)[:, 1]
    assert p.shape == (len(y),)
    assert (0.0 <= p).all() and (p <= 1.0).all()


def test_from_dict_merges_kernel_params():
    cfg = from_dict({"models": ["lightgbm", "kernel"], "kernel_params": {"n_components": 256, "gamma": 0.2}})
    assert cfg.models == ("lightgbm", "kernel")
    assert cfg.kernel_params["n_components"] == 256
    assert cfg.kernel_params["gamma"] == 0.2
    assert cfg.kernel_params["C"] == 1.0  # 未指定は既定で補完
