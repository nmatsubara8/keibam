"""BaseModelsConfig の単体テスト。"""

import json
import tempfile

import pytest

from src.training._base_models_config import (
    BaseModelsConfig,
    from_dict,
    load_base_models_config,
)


def test_default_is_lightgbm_only():
    cfg = BaseModelsConfig()
    assert cfg.models == ("lightgbm",)
    assert cfg.tune_per_model is False
    assert cfg.n_trials == 50


def test_from_dict_three_gbdt():
    cfg = from_dict({"models": ["lightgbm", "xgboost", "catboost"]})
    assert cfg.models == ("lightgbm", "xgboost", "catboost")


def test_load_json_roundtrip():
    data = {"models": ["lightgbm", "xgboost"], "tune_per_model": True, "n_trials": 30}
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(data, f)
        path = f.name
    cfg = load_base_models_config(path)
    assert cfg.models == ("lightgbm", "xgboost")
    assert cfg.tune_per_model is True
    assert cfg.n_trials == 30


def test_unknown_key_ignored():
    cfg = from_dict({"models": ["lightgbm"], "unknown_future_key": "value"})
    assert cfg.models == ("lightgbm",)


def test_default_meta_model_is_logistic():
    cfg = BaseModelsConfig()
    assert cfg.meta_model == "logistic"
    assert cfg.meta_params == {}


def test_from_dict_meta_model_lightgbm():
    cfg = from_dict({"models": ["lightgbm", "xgboost"], "meta_model": "lightgbm"})
    assert cfg.meta_model == "lightgbm"


def test_from_dict_meta_model_case_insensitive():
    cfg = from_dict({"meta_model": "LightGBM"})
    assert cfg.meta_model == "lightgbm"


def test_from_dict_meta_params_passthrough():
    cfg = from_dict({"meta_model": "lightgbm", "meta_params": {"num_leaves": 15}})
    assert cfg.meta_params == {"num_leaves": 15}


def test_from_dict_invalid_meta_model_raises():
    with pytest.raises(ValueError, match="meta_model"):
        from_dict({"meta_model": "xgboost"})
