"""build_base_models の単体テスト。"""

import logging
from unittest.mock import patch

import pytest

from src.training._base_model_factory import build_base_models
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
