"""_retrain.py の純粋ロジックと RetrainJob のテスト（optuna/selenium 不要）。"""

import datetime as dt
import os

import numpy as np
import pandas as pd
import pytest
from sklearn.linear_model import LogisticRegression

from src.constants._results_cols import ResultsCols
from src.pipeline._retrain import RetrainConfig
from src.pipeline._retrain import RetrainJob
from src.pipeline._retrain import evaluate_test
from src.pipeline._retrain import load_metadata
from src.pipeline._retrain import save_metadata
from src.pipeline._retrain import version_name


# ---------------------------------------------------------------------------
# 純粋ロジック
# ---------------------------------------------------------------------------


def test_version_name_format():
    today = dt.date(2024, 1, 15)
    assert version_name("keibam", today) == "20240115_keibam"


def test_version_name_default_prefix():
    name = version_name(today=dt.date(2024, 6, 1))
    assert name.startswith("20240601_")


def test_evaluate_test_returns_auc_in_range():
    rng = np.random.default_rng(0)
    n = 200
    X = rng.normal(size=(n, 3))
    y = (X[:, 0] > 0).astype(int)
    model = LogisticRegression(max_iter=1000).fit(X, y)
    X_test = pd.DataFrame(X, columns=["a", "b", "c"])
    y_test = pd.Series(y)
    metrics = evaluate_test(model, X_test, y_test)
    assert "auc_test" in metrics
    assert 0.0 <= metrics["auc_test"] <= 1.0


def test_evaluate_test_drops_tansho_odds():
    """TANSHO_ODDS 列を持つ X_test でも例外にならないこと。"""
    rng = np.random.default_rng(1)
    n = 100
    X = rng.normal(size=(n, 2))
    y = (X[:, 0] > 0).astype(int)
    model = LogisticRegression(max_iter=200).fit(X, y)
    X_with_odds = pd.DataFrame(X, columns=["a", "b"])
    X_with_odds[ResultsCols.TANSHO_ODDS] = 3.0
    y_test = pd.Series(y)
    metrics = evaluate_test(model, X_with_odds, y_test)
    assert "auc_test" in metrics


# ---------------------------------------------------------------------------
# metadata save / load
# ---------------------------------------------------------------------------


def test_save_and_load_metadata_roundtrip(tmp_path):
    path = str(tmp_path / "history.json")
    save_metadata({"version": "v1", "auc_test": 0.72}, path)
    save_metadata({"version": "v2", "auc_test": 0.74}, path)
    history = load_metadata(path)
    assert len(history) == 2
    assert history[0]["version"] == "v1"
    assert history[1]["version"] == "v2"


def test_load_metadata_missing_returns_empty(tmp_path):
    assert load_metadata(str(tmp_path / "nope.json")) == []


# ---------------------------------------------------------------------------
# RetrainJob (スタブ DI)
# ---------------------------------------------------------------------------


def _make_featured(n=120, seed=0):
    """FeatureEngineering 出力を模したダミー DataFrame。"""
    rng = np.random.default_rng(seed)
    n_races, n_horses = n, 8
    rows = []
    base = pd.Timestamp("2022-01-01")
    for i in range(n_races):
        for h in range(n_horses):
            rows.append(
                {
                    "date": base + pd.Timedelta(days=i),
                    "rank": int(rng.integers(0, 2)),
                    ResultsCols.TANSHO_ODDS: float(rng.uniform(1.5, 20)),
                    "feat_a": float(rng.normal()),
                    "feat_b": float(rng.normal()),
                }
            )
    df = pd.DataFrame(rows)
    df.index = [f"r{i // n_horses:04d}" for i in range(len(df))]
    return df


class _StubAI:
    """KeibaAI のスタブ（重い学習を回避）。"""

    class _Datasets:
        def __init__(self, df):
            from src.training._data_splitter import DataSplitter
            import sys
            import types

            # optuna スタブ（テスト環境向け）
            for mod in ["optuna", "optuna.integration", "optuna.integration.lightgbm"]:
                if mod not in sys.modules:
                    stub = types.ModuleType(mod)
                    if mod == "optuna.integration.lightgbm":
                        class _DS:
                            def __init__(self, d, l): pass
                        stub.Dataset = _DS
                    sys.modules[mod] = stub

            self._ds = DataSplitter(df, test_size=0.3, valid_size=0.3)

        @property
        def X_test(self):
            return self._ds.X_test

        @property
        def y_test(self):
            return self._ds.y_test

    def __init__(self, df):
        rng = np.random.default_rng(0)
        X = rng.normal(size=(len(df), 2))
        y = (X[:, 0] > 0).astype(int)
        self.effective_model = LogisticRegression(max_iter=200, random_state=0).fit(X, y)
        self.datasets = self._Datasets(df)

    def train_with_stacking(self, **kwargs): pass
    def train_without_tuning(self): pass
    def train_with_tuning(self): pass


class _StubFactory:
    def __init__(self):
        self.saved = []

    def create(self, featured_data, test_size, valid_size):
        return _StubAI(featured_data)

    def save(self, ai, version_name):
        self.saved.append(version_name)


def test_retrain_job_returns_meta(tmp_path):
    df = _make_featured()
    cfg = RetrainConfig(models_dir=str(tmp_path), use_stacking=True)
    factory = _StubFactory()
    job = RetrainJob(factory, cfg)
    meta = job.run(df, vname="test_v1", with_tuning=False)
    assert meta["version"] == "test_v1"
    assert "auc_test" in meta
    assert meta["use_stacking"] is True


def test_retrain_job_saves_model(tmp_path):
    df = _make_featured()
    cfg = RetrainConfig(models_dir=str(tmp_path), use_stacking=False)
    factory = _StubFactory()
    job = RetrainJob(factory, cfg)
    job.run(df, vname="v1", with_tuning=False)
    assert "v1" in factory.saved


def test_retrain_job_appends_metadata(tmp_path):
    df = _make_featured()
    cfg = RetrainConfig(models_dir=str(tmp_path), use_stacking=False)
    factory = _StubFactory()
    job = RetrainJob(factory, cfg)
    job.run(df, vname="v1", with_tuning=False)
    job.run(df, vname="v2", with_tuning=False)
    history = load_metadata(os.path.join(str(tmp_path), "version_history.json"))
    assert len(history) == 2
    assert history[0]["version"] == "v1"
    assert history[1]["version"] == "v2"


def test_retrain_job_records_n_races(tmp_path):
    df = _make_featured(n=60)
    cfg = RetrainConfig(models_dir=str(tmp_path), use_stacking=False)
    job = RetrainJob(_StubFactory(), cfg)
    meta = job.run(df, vname="v1")
    assert meta["n_races"] == len(df.index.unique())
