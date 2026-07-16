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


def test_retrain_job_saves_tuned_base_models_config(tmp_path):
    """tune_per_model 探索後、完成 config が tuned_base_models.json に保存され読み戻せる。"""
    import json

    from src.training._base_models_config import from_dict, load_base_models_config

    class _TunedAI(_StubAI):
        def __init__(self, df):
            super().__init__(df)
            # tune_nn 後にマージされた完成 config を模す（arch/構造が反映済み）
            self.tuned_base_models_config = from_dict({
                "models": ["lightgbm", "nn"],
                "tune_per_model": True,
                "nn_params": {"arch": "mlp", "hidden_dims": [96], "dropout": 0.33, "lr": 0.0007},
            })

    class _TunedFactory(_StubFactory):
        def create(self, featured_data, test_size, valid_size):
            return _TunedAI(featured_data)

    cfg = RetrainConfig(models_dir=str(tmp_path), use_stacking=True)
    RetrainJob(_TunedFactory(), cfg).run(_make_featured(), vname="tv1", with_tuning=False)

    out = tmp_path / "tuned_base_models.json"
    assert out.exists()
    saved = json.loads(out.read_text(encoding="utf-8"))
    assert saved["tune_per_model"] is True
    assert saved["nn_params"]["hidden_dims"] == [96]
    # 保存 JSON はそのまま固定運用 config として読み戻せる（往復）
    cfg2 = load_base_models_config(str(out))
    assert cfg2.nn_params["dropout"] == 0.33


def test_retrain_job_skips_tuned_config_when_absent(tmp_path):
    """通常学習（tuned_base_models_config 無し）では JSON を作らない。"""
    cfg = RetrainConfig(models_dir=str(tmp_path), use_stacking=False)
    RetrainJob(_StubFactory(), cfg).run(_make_featured(), vname="v1", with_tuning=False)
    assert not (tmp_path / "tuned_base_models.json").exists()


def test_retrain_job_keeps_better_existing_tuned_config(tmp_path):
    """歴代 best 保持: 既存ファイルの auc_test が高ければ今回の学習で上書きしない。"""
    import json

    from src.training._base_models_config import from_dict

    out = tmp_path / "tuned_base_models.json"
    # 到達不能に高い _auc_test を持つ既存 best を置く（今回の auc_test ≤ 1.0 は必ず下回る）
    out.write_text(
        json.dumps({"models": ["lightgbm", "nn"], "nn_params": {"hidden_dims": [7]}, "_auc_test": 999.0}),
        encoding="utf-8",
    )

    class _TunedAI(_StubAI):
        def __init__(self, df):
            super().__init__(df)
            self.tuned_base_models_config = from_dict({
                "models": ["lightgbm", "nn"], "tune_per_model": True,
                "nn_params": {"hidden_dims": [96]},
            })

    class _TunedFactory(_StubFactory):
        def create(self, featured_data, test_size, valid_size):
            return _TunedAI(featured_data)

    cfg = RetrainConfig(models_dir=str(tmp_path), use_stacking=True)
    RetrainJob(_TunedFactory(), cfg).run(_make_featured(), vname="tv2", with_tuning=False)

    saved = json.loads(out.read_text(encoding="utf-8"))
    assert saved["_auc_test"] == 999.0                 # 据え置き（上書きされない）
    assert saved["nn_params"]["hidden_dims"] == [7]    # 既存 best を維持


# ---------------------------------------------------------------------------
# Stage B: Win ヘッド併行学習（lightgbm/DataSplitter 非依存の軽量スタブ）
# ---------------------------------------------------------------------------


class _LightDatasets:
    def __init__(self):
        self._X = pd.DataFrame({"f": [0.1, 0.2, 0.3, 0.4]})
        self._y = pd.Series([1, 0, 0, 1])

    @property
    def X_test(self):
        return self._X

    @property
    def y_test(self):
        return self._y


class _LightAI:
    def __init__(self, target_col):
        X = np.array([[0.1], [0.2], [0.3], [0.4]])
        y = np.array([1, 0, 0, 1])
        self.effective_model = LogisticRegression(max_iter=200).fit(X, y)
        self.datasets = _LightDatasets()
        self.target_col = target_col

    def train_with_stacking(self, **kwargs):
        pass

    def train_without_tuning(self):
        pass


class _TwoHeadFactory:
    """target_col / suffix を尊重する 2 ヘッド対応スタブ。"""

    def __init__(self):
        self.saved = []  # (version, suffix, target_col)
        self.created = []  # target_col

    def create(self, featured_data, test_size, valid_size, target_col="rank"):
        self.created.append(target_col)
        return _LightAI(target_col)

    def save(self, ai, version_name, suffix=""):
        self.saved.append((version_name, suffix, ai.target_col))


def test_retrain_trains_and_saves_win_head(tmp_path):
    factory = _TwoHeadFactory()
    cfg = RetrainConfig(models_dir=str(tmp_path), use_stacking=True, train_win_head=True)
    job = RetrainJob(factory, cfg)
    meta = job.run(_make_featured(), vname="v_2head", with_tuning=False)

    # Place(rank) と Win(rank_win) の 2 ヘッドが create された
    assert factory.created == ["rank", "rank_win"]
    # 保存は Place(suffix="") と Win(suffix="__win") の 2 回
    assert ("v_2head", "", "rank") in factory.saved
    assert ("v_2head", "__win", "rank_win") in factory.saved
    # メタに Win ヘッドの AUC が記録される
    assert "win_head" in meta and "auc_test" in meta["win_head"]


def test_retrain_win_head_reuses_tuned_params(tmp_path):
    """--with-tuning 時、Place 探索の best_params が Win ヘッドにも流用される（既定パラメータのままにしない）。"""
    tuned = {"num_leaves": 15, "learning_rate": 0.0076, "path_smooth": 18.0}

    class _FakeStudy:
        best_params = tuned
        trials: list = []

    class _ParamHeadAI(_LightAI):
        def __init__(self, target_col):
            super().__init__(target_col)
            self.injected_params = None
            if target_col == "rank":  # Place ヘッドだけが探索結果 study を持つ
                self.tuning_study_ = _FakeStudy()

        def set_lgb_params(self, params):
            self.injected_params = params

    class _ParamTwoHeadFactory(_TwoHeadFactory):
        def __init__(self):
            super().__init__()
            self.ais: dict = {}

        def create(self, featured_data, test_size, valid_size, target_col="rank"):
            self.created.append(target_col)
            ai = _ParamHeadAI(target_col)
            self.ais[target_col] = ai
            return ai

    factory = _ParamTwoHeadFactory()
    cfg = RetrainConfig(models_dir=str(tmp_path), use_stacking=True, train_win_head=True)
    job = RetrainJob(factory, cfg)
    job.run(_make_featured(), vname="v_tuned", with_tuning=True)

    # Win ヘッドが Place 探索の best_params を受け取っている（既定パラメータのままではない）
    assert factory.ais["rank_win"].injected_params == tuned
    # Place ヘッドは探索経由なので set_lgb_params は呼ばれない
    assert factory.ais["rank"].injected_params is None


def test_retrain_win_head_prefers_explicit_lgb_params(tmp_path):
    """明示 lgb_params 指定時は（tuning より）そちらを Win ヘッドへ注入する。"""
    explicit = {"num_leaves": 31}

    class _ParamHeadAI(_LightAI):
        def __init__(self, target_col):
            super().__init__(target_col)
            self.injected_params = None

        def set_lgb_params(self, params):
            self.injected_params = params

    class _ParamTwoHeadFactory(_TwoHeadFactory):
        def __init__(self):
            super().__init__()
            self.ais: dict = {}

        def create(self, featured_data, test_size, valid_size, target_col="rank"):
            self.created.append(target_col)
            ai = _ParamHeadAI(target_col)
            self.ais[target_col] = ai
            return ai

    factory = _ParamTwoHeadFactory()
    cfg = RetrainConfig(models_dir=str(tmp_path), use_stacking=True, train_win_head=True)
    job = RetrainJob(factory, cfg)
    job.run(_make_featured(), vname="v_expl", lgb_params=explicit, params_rank=1)

    assert factory.ais["rank_win"].injected_params == explicit


def test_retrain_skips_win_head_when_disabled(tmp_path):
    factory = _TwoHeadFactory()
    cfg = RetrainConfig(models_dir=str(tmp_path), use_stacking=True, train_win_head=False)
    job = RetrainJob(factory, cfg)
    meta = job.run(_make_featured(), vname="v_place_only", with_tuning=False)

    assert factory.created == ["rank"]  # Place のみ
    assert all(suffix == "" for _, suffix, _ in factory.saved)
    assert "win_head" not in meta


def test_retrain_win_head_skipped_if_factory_unsupported(tmp_path):
    """target_col 非対応の旧 factory では Win ヘッドを安全にスキップ（非致命）。"""
    factory = _StubFactory()  # create(featured_data, test_size, valid_size) のみ
    cfg = RetrainConfig(models_dir=str(tmp_path), use_stacking=False, train_win_head=True)
    job = RetrainJob(factory, cfg)
    # _StubAI は DataSplitter(lightgbm) を要するため、ここでは create が TypeError 以前に
    # 失敗し得る。Win ヘッドが本体 retrain を壊さないこと（例外を投げない）だけ確認する。
    try:
        meta = job.run(_make_featured(), vname="v_compat", with_tuning=False)
    except ModuleNotFoundError:
        pytest.skip("lightgbm 未導入環境（Place ヘッド学習に必要）")
    assert "win_head" not in meta
