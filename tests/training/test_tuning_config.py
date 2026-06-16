"""TuningConfig（探索範囲・回数の設定）と手書き Optuna 探索のテスト。"""

import json
import types

import pytest

from src.training._tuning_config import DEFAULT_SEARCH_SPACE
from src.training._tuning_config import METHOD_LIGHTGBM_TUNER
from src.training._tuning_config import METHOD_OPTUNA
from src.training._tuning_config import TuningConfig
from src.training._tuning_config import from_dict
from src.training._tuning_config import load_tuning_config


class _FakeTrial:
    """suggest_* を記録するダックタイプ。指定範囲の下限を返す。"""

    def __init__(self):
        self.calls = {}

    def suggest_int(self, name, low, high):
        self.calls[name] = ("int", low, high)
        return low

    def suggest_float(self, name, low, high, log=False):
        self.calls[name] = ("float", low, high, log)
        return low


class TestTuningConfigDefaults:
    def test_default_is_lightgbm_tuner(self):
        cfg = TuningConfig()
        assert cfg.method == METHOD_LIGHTGBM_TUNER
        assert cfg.is_custom is False

    def test_optuna_method_is_custom(self):
        cfg = TuningConfig(method=METHOD_OPTUNA)
        assert cfg.is_custom is True

    def test_default_search_space_copied_per_instance(self):
        a = TuningConfig()
        a.search_space["num_leaves"][0] = 999
        b = TuningConfig()
        assert b.search_space["num_leaves"][0] == DEFAULT_SEARCH_SPACE["num_leaves"][0]


class TestSuggestParams:
    def test_includes_objective_and_metric(self):
        cfg = TuningConfig(method=METHOD_OPTUNA)
        params = cfg.suggest_params(_FakeTrial())
        assert params["objective"] == "binary"
        assert params["metric"] == "binary_logloss"

    def test_int_and_log_scale_routing(self):
        cfg = TuningConfig(method=METHOD_OPTUNA)
        trial = _FakeTrial()
        cfg.suggest_params(trial)
        # num_leaves は整数探索
        assert trial.calls["num_leaves"][0] == "int"
        # learning_rate は log スケールの float
        assert trial.calls["learning_rate"][0] == "float"
        assert trial.calls["learning_rate"][3] is True
        # feature_fraction は通常 float（log=False）
        assert trial.calls["feature_fraction"][3] is False

    def test_custom_search_space_respected(self):
        cfg = TuningConfig(method=METHOD_OPTUNA, search_space={"num_leaves": [16, 32]})
        trial = _FakeTrial()
        params = cfg.suggest_params(trial)
        assert trial.calls["num_leaves"] == ("int", 16, 32)
        assert params["num_leaves"] == 16


class TestFromDict:
    def test_defaults_method_optuna(self):
        cfg = from_dict({})
        assert cfg.method == METHOD_OPTUNA
        assert cfg.n_trials == 50

    def test_partial_search_space_merged_with_defaults(self):
        cfg = from_dict({"search_space": {"num_leaves": [10, 20]}})
        assert cfg.search_space["num_leaves"] == [10, 20]
        # 未指定キーは既定範囲で補完される
        assert cfg.search_space["learning_rate"] == DEFAULT_SEARCH_SPACE["learning_rate"]

    def test_reads_scalar_fields(self):
        cfg = from_dict({"n_trials": 7, "timeout": 30, "seed": 1, "num_boost_round": 100})
        assert cfg.n_trials == 7
        assert cfg.timeout == 30
        assert cfg.seed == 1
        assert cfg.num_boost_round == 100


class TestLoadTuningConfig:
    def test_roundtrip(self, tmp_path):
        p = tmp_path / "tuning.json"
        p.write_text(json.dumps({"method": "optuna", "n_trials": 12}))
        cfg = load_tuning_config(str(p))
        assert cfg.n_trials == 12
        assert cfg.is_custom

    def test_missing_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_tuning_config(str(tmp_path / "nope.json"))


class TestTuneHyperParamsDispatch:
    """ModelWrapper.tune_hyper_params が config に応じて分岐することを確認する。"""

    def test_lightgbm_tuner_path_when_config_none(self, monkeypatch):
        from src.training import _model_wrapper

        mw = _model_wrapper.ModelWrapper()
        called = {}
        monkeypatch.setattr(
            mw, "_ModelWrapper__tune_lightgbm_tuner",
            lambda datasets, study=None: called.setdefault("tuner", True),
        )
        monkeypatch.setattr(
            mw, "_ModelWrapper__tune_custom",
            lambda datasets, cfg, study=None: called.setdefault("custom", True),
        )
        mw.tune_hyper_params(datasets=types.SimpleNamespace())
        assert called == {"tuner": True}

    def test_custom_path_when_method_optuna(self, monkeypatch):
        from src.training import _model_wrapper

        mw = _model_wrapper.ModelWrapper()
        called = {}
        monkeypatch.setattr(
            mw, "_ModelWrapper__tune_lightgbm_tuner",
            lambda datasets, study=None: called.setdefault("tuner", True),
        )
        monkeypatch.setattr(
            mw, "_ModelWrapper__tune_custom",
            lambda datasets, cfg, study=None: called.setdefault("custom", True),
        )
        mw.tune_hyper_params(
            datasets=types.SimpleNamespace(),
            tuning_config=TuningConfig(method=METHOD_OPTUNA),
        )
        assert called == {"custom": True}
