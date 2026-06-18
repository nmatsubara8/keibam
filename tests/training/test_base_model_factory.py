"""build_base_models の単体テスト。"""

import logging
from unittest.mock import patch

import pytest

from src.training._base_model_factory import build_base_models, build_meta_model
from src.training._base_models_config import BaseModelsConfig


_DUMMY_LGB_PARAMS = {"objective": "binary", "n_estimators": 10, "verbose": -1}
_SPW = 5.0


def test_lightgbm_only():
    cfg = BaseModelsConfig(models=("lightgbm",))
    specs = build_base_models(cfg, _DUMMY_LGB_PARAMS, _SPW)
    assert len(specs) == 1
    assert specs[0].name == "LightGBM"


def test_skip_missing_xgboost(caplog):
    cfg = BaseModelsConfig(models=("xgboost",))
    with patch("builtins.__import__", side_effect=ImportError("xgboost")):
        with caplog.at_level(logging.WARNING, logger="src.training._base_model_factory"):
            with pytest.raises(RuntimeError):
                build_base_models(cfg, _DUMMY_LGB_PARAMS, _SPW)
    assert any("xgboost" in r.message for r in caplog.records)


def test_skip_missing_catboost(caplog):
    cfg = BaseModelsConfig(models=("catboost",))
    with patch("builtins.__import__", side_effect=ImportError("catboost")):
        with caplog.at_level(logging.WARNING, logger="src.training._base_model_factory"):
            with pytest.raises(RuntimeError):
                build_base_models(cfg, _DUMMY_LGB_PARAMS, _SPW)
    assert any("catboost" in r.message for r in caplog.records)


def test_all_missing_raises():
    cfg = BaseModelsConfig(models=("nn",))
    with pytest.raises(RuntimeError, match="有効な base"):
        build_base_models(cfg, _DUMMY_LGB_PARAMS, _SPW)


# ---------------------------------------------------------------------------
# build_meta_model（meta 学習器ファクトリ）
# ---------------------------------------------------------------------------

def test_build_meta_model_default_logistic():
    from sklearn.linear_model import LogisticRegression

    meta = build_meta_model(BaseModelsConfig())
    assert isinstance(meta, LogisticRegression)


def test_build_meta_model_lightgbm():
    import lightgbm as lgb

    cfg = BaseModelsConfig(meta_model="lightgbm")
    meta = build_meta_model(cfg, scale_pos_weight=_SPW)
    assert isinstance(meta, lgb.LGBMClassifier)
    # 既定は極浅い構成（過学習抑制）
    assert meta.get_params()["num_leaves"] == 3
    assert meta.get_params()["scale_pos_weight"] == _SPW


def test_build_meta_model_lightgbm_params_override():
    cfg = BaseModelsConfig(meta_model="lightgbm", meta_params={"num_leaves": 31})
    meta = build_meta_model(cfg)
    assert meta.get_params()["num_leaves"] == 31


def test_build_meta_model_logistic_params_override():
    cfg = BaseModelsConfig(meta_model="logistic", meta_params={"C": 0.5})
    meta = build_meta_model(cfg)
    assert meta.get_params()["C"] == 0.5


def test_build_meta_model_fit_predict_roundtrip():
    """meta 特徴量（base 確率の数列）で fit→predict_proba が 2 列を返す。"""
    import numpy as np

    rng = np.random.default_rng(0)
    x = rng.uniform(0, 1, size=(200, 3))  # 3 base の予測確率を模す
    y = (x[:, 0] + rng.normal(scale=0.1, size=200) > 0.5).astype(int)
    for meta_kind in ("logistic", "lightgbm"):
        meta = build_meta_model(BaseModelsConfig(meta_model=meta_kind))
        meta.fit(x, y)
        proba = meta.predict_proba(x)
        assert proba.shape == (200, 2)
