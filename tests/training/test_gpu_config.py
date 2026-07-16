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
