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


def test_lightgbm_params_default_empty_and_preserved():
    # 既定は空（チューナ/既定値に委ねる）
    assert BaseModelsConfig().lightgbm_params == {}
    # 明示指定はそのまま保持される（探索済み config の書き戻し経路）
    cfg = from_dict({"models": ["lightgbm", "nn"], "lightgbm_params": {"num_leaves": 15, "learning_rate": 0.03}})
    assert cfg.lightgbm_params == {"num_leaves": 15, "learning_rate": 0.03}


def test_tuned_config_ignores_score_metadata_keys():
    # tuned_base_models.json は _auc_test/_version を持つが from_dict は無視して読める
    cfg = from_dict({"models": ["lightgbm"], "lightgbm_params": {"num_leaves": 7}, "_auc_test": 0.83, "_version": "v1"})
    assert cfg.lightgbm_params == {"num_leaves": 7}


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
