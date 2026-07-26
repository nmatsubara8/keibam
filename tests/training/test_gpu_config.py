"""GBDT GPU 判定（_gpu_config）: 環境変数 opt-in と param 断片。"""
import importlib

import pytest


@pytest.fixture()
def gpu_cfg(monkeypatch):
    monkeypatch.delenv("KEIBA_USE_GPU", raising=False)
    return importlib.import_module("src.training._gpu_config")


def test_disabled_by_default(gpu_cfg):
    assert gpu_cfg.gpu_enabled() is False
    assert gpu_cfg.xgb_gpu_params() == {}
    assert gpu_cfg.catboost_gpu_params() == {}


@pytest.mark.parametrize("val", ["1", "true", "True", "yes", "on"])
def test_enabled_values(gpu_cfg, monkeypatch, val):
    monkeypatch.setenv("KEIBA_USE_GPU", val)
    assert gpu_cfg.gpu_enabled() is True
    assert gpu_cfg.xgb_gpu_params() == {"device": "cuda"}
    assert gpu_cfg.catboost_gpu_params() == {"task_type": "GPU"}


@pytest.mark.parametrize("val", ["0", "false", "", "off", "no"])
def test_disabled_values(gpu_cfg, monkeypatch, val):
    monkeypatch.setenv("KEIBA_USE_GPU", val)
    assert gpu_cfg.gpu_enabled() is False
    assert gpu_cfg.xgb_gpu_params() == {}


def test_lgb_gpu_params_off_by_default(gpu_cfg, monkeypatch):
    monkeypatch.delenv("KEIBA_LGB_GPU", raising=False)
    assert gpu_cfg.lgb_gpu_params() == {}


def test_lgb_gpu_params_opencl_and_cuda(gpu_cfg, monkeypatch):
    monkeypatch.setenv("KEIBA_LGB_GPU", "gpu")
    assert gpu_cfg.lgb_gpu_params() == {"device_type": "gpu"}
    monkeypatch.setenv("KEIBA_LGB_GPU", "cuda")
    assert gpu_cfg.lgb_gpu_params() == {"device_type": "cuda"}


def test_lgb_gpu_independent_of_use_gpu_and_invalid(gpu_cfg, monkeypatch):
    monkeypatch.setenv("KEIBA_USE_GPU", "1")   # xgb/cat 用。lightgbm には無関係
    monkeypatch.delenv("KEIBA_LGB_GPU", raising=False)
    assert gpu_cfg.lgb_gpu_params() == {}
    monkeypatch.setenv("KEIBA_LGB_GPU", "invalid")
    assert gpu_cfg.lgb_gpu_params() == {}
