"""_retrain.py の純粋ロジックと RetrainJob のテスト（optuna/selenium 不要）。"""

import datetime as dt
import os

import numpy as np
import pandas as pd
import pytest
from sklearn.linear_model import LogisticRegression

from src.constants._master import Master
from src.constants._results_cols import ResultsCols
from src.pipeline._retrain import RetrainConfig
from src.pipeline._retrain import RetrainJob
from src.pipeline._retrain import evaluate_test
from src.pipeline._retrain import load_metadata
from src.pipeline._retrain import save_metadata
from src.pipeline._retrain import version_name

Master_TURF = Master.RACE_TYPE_TURF
Master_DIRT = Master.RACE_TYPE_DIRT


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


def test_save_metadata_replaces_same_version(tmp_path):
    """同一 version の再保存は追記ではなく置き換え（同日再学習は pickle を上書きするため）。"""
    path = str(tmp_path / "history.json")
    save_metadata({"version": "v1", "auc_test": 0.72}, path)
    save_metadata({"version": "v1", "auc_test": 0.75}, path)
    history = load_metadata(path)
    assert len(history) == 1
    assert history[0]["auc_test"] == 0.75


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
        self.datasets = self._Datasets(df)
        # evaluate_test は X_test から TANSHO_ODDS を落として predict_proba するため、
        # モデルはその特徴量数（= X_test 列数 - 1）に合わせて学習する。
        xt = self.datasets.X_test.drop([ResultsCols.TANSHO_ODDS], axis=1, errors="ignore")
        n_features = max(1, xt.shape[1])
        rng = np.random.default_rng(0)
        X = rng.normal(size=(len(df), n_features))
        y = (X[:, 0] > 0).astype(int)
        self.effective_model = LogisticRegression(max_iter=200, random_state=0).fit(X, y)

    def train_with_stacking(self, **kwargs): pass
    def train_without_tuning(self): pass
    def train_with_tuning(self): pass


class _StubFactory:
    def __init__(self):
        self.saved = []
        self.saved_calls = []

    def create(self, featured_data, test_size, valid_size):
        return _StubAI(featured_data)

    def save(self, ai, version_name, category=None, models_dir="models"):
        self.saved.append(version_name)
        self.saved_calls.append((version_name, category))
        return f"{models_dir}/{version_name}.pickle"


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


def _make_categorized_featured():
    """race_type ワンホット + 全国/地方 race_id を持つ featured 風 DataFrame。

    central_turf 5 レース / central_dirt 4 レース / local_dirt 3 レースを含む。
    """
    n_horses = 6
    specs = [
        ("central", "05", Master_TURF, 5, 0),
        ("central", "05", Master_DIRT, 4, 100),
        ("local", "44", Master_DIRT, 3, 200),
    ]
    rows = []
    index = []
    base = pd.Timestamp("2024-01-01")
    day = 0
    for _org, place, race_type, n_races, id_off in specs:
        for i in range(n_races):
            race_id = f"2024{place}{id_off + i:06d}"
            day += 1
            for h in range(n_horses):
                rows.append(
                    {
                        "date": base + pd.Timedelta(days=day),
                        "rank": h % 2,  # 各レースに 0/1 両クラスを含める
                        ResultsCols.TANSHO_ODDS: 3.0,
                        "feat_a": float(h),
                        "race_type__芝": 1 if race_type == Master_TURF else 0,
                        "race_type__ダート": 1 if race_type == Master_DIRT else 0,
                        "race_type__障害": 0,
                    }
                )
                index.append(race_id)
    df = pd.DataFrame(rows)
    df.index = index
    return df


def test_retrain_job_trains_category_models(tmp_path):
    """全国/地方 × 芝/ダート の featured で統合 + カテゴリ別モデルが保存される。"""
    df = _make_categorized_featured()
    cfg = RetrainConfig(models_dir=str(tmp_path), use_stacking=False, min_category_races=3)
    factory = _StubFactory()
    job = RetrainJob(factory, cfg)
    meta = job.run(df, vname="vcat")

    saved_categories = {cat for _v, cat in factory.saved_calls}
    # 統合 + データが min 以上のカテゴリ 3 種
    assert None in saved_categories or "combined" in saved_categories
    assert "central_turf" in saved_categories
    assert "central_dirt" in saved_categories
    assert "local_dirt" in saved_categories
    # 統合モデルのメタに学習済みカテゴリが記録される
    assert set(meta["categories"]) == {"central_turf", "central_dirt", "local_dirt"}


def test_retrain_job_skips_small_categories(tmp_path):
    """min_category_races 未満のカテゴリは学習されない。"""
    df = _make_categorized_featured()
    cfg = RetrainConfig(models_dir=str(tmp_path), use_stacking=False, min_category_races=4)
    factory = _StubFactory()
    job = RetrainJob(factory, cfg)
    meta = job.run(df, vname="vcat2")
    # local_dirt は 3 レースなのでスキップされる
    assert "local_dirt" not in meta["categories"]
    assert "central_turf" in meta["categories"]


def test_retrain_job_no_category_split(tmp_path):
    """train_categories=False なら統合モデルのみ。"""
    df = _make_categorized_featured()
    cfg = RetrainConfig(models_dir=str(tmp_path), use_stacking=False, train_categories=False)
    factory = _StubFactory()
    job = RetrainJob(factory, cfg)
    meta = job.run(df, vname="vcat3")
    assert meta["categories"] == []
    saved_categories = {cat for _v, cat in factory.saved_calls}
    assert saved_categories == {"combined"}


def test_tune_categories_tunes_each_category(tmp_path):
    """tune_categories=True で、学習される各カテゴリが train_with_tuning を呼ぶ。"""
    calls = {"tuning": 0, "no_tuning": 0}

    class _RecAI(_StubAI):
        def train_with_tuning(self):
            calls["tuning"] += 1

        def train_without_tuning(self):
            calls["no_tuning"] += 1

    class _RecFactory(_StubFactory):
        def create(self, featured_data, test_size, valid_size):
            return _RecAI(featured_data)

    df = _make_categorized_featured()  # central_turf 5 / central_dirt 4 / local_dirt 3
    cfg = RetrainConfig(
        models_dir=str(tmp_path),
        use_stacking=False,
        min_category_races=3,
        tune_categories=True,
    )
    job = RetrainJob(_RecFactory(), cfg)
    meta = job.run(df, vname="vtune", with_tuning=False)

    # 統合は with_tuning=False（no_tuning）、カテゴリ 3 種は tuning
    assert calls["tuning"] == 3
    assert calls["no_tuning"] == 1
    assert set(meta["categories"]) == {"central_turf", "central_dirt", "local_dirt"}


def test_tune_categories_default_off(tmp_path):
    """既定（tune_categories=False）ではカテゴリは探索なしで学習される。"""
    calls = {"tuning": 0, "no_tuning": 0}

    class _RecAI(_StubAI):
        def train_with_tuning(self):
            calls["tuning"] += 1

        def train_without_tuning(self):
            calls["no_tuning"] += 1

    class _RecFactory(_StubFactory):
        def create(self, featured_data, test_size, valid_size):
            return _RecAI(featured_data)

    df = _make_categorized_featured()
    cfg = RetrainConfig(models_dir=str(tmp_path), use_stacking=False, min_category_races=3)
    job = RetrainJob(_RecFactory(), cfg)
    job.run(df, vname="vnotune", with_tuning=False)
    assert calls["tuning"] == 0
    assert calls["no_tuning"] == 4  # 統合 + カテゴリ 3


def test_retrain_job_injects_lgb_params(tmp_path):
    """lgb_params 指定時は set_lgb_params で注入され、メタに記録される。"""

    class _ParamAwareAI(_StubAI):
        def __init__(self, df):
            super().__init__(df)
            self.injected_params = None

        def set_lgb_params(self, params):
            self.injected_params = params

    class _ParamFactory(_StubFactory):
        def __init__(self):
            super().__init__()
            self.created = []

        def create(self, featured_data, test_size, valid_size):
            ai = _ParamAwareAI(featured_data)
            self.created.append(ai)
            return ai

    factory = _ParamFactory()
    cfg = RetrainConfig(models_dir=str(tmp_path), use_stacking=False)
    job = RetrainJob(factory, cfg)
    params = {"num_leaves": 31, "feature_fraction": 0.8}

    meta = job.run(_make_featured(), vname="v_params", lgb_params=params, params_rank=2)

    assert factory.created[0].injected_params == params
    assert meta["params_rank"] == 2
    assert meta["lgb_params"] == params
