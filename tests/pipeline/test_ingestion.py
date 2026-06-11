"""_ingestion.py の純粋ロジックと IngestJob のテスト（selenium/bs4 不要）。"""

import os

import numpy as np
import pandas as pd
import pytest

from src.pipeline._ingestion import IngestConfig
from src.pipeline._ingestion import IngestJob
from src.pipeline._ingestion import append_idempotent
from src.pipeline._ingestion import existing_race_ids
from src.pipeline._ingestion import find_new_race_ids
from src.pipeline._ingestion import load_raw
from src.pipeline._ingestion import save_raw


# ---------------------------------------------------------------------------
# 純粋ロジック
# ---------------------------------------------------------------------------


def _df(race_ids, n_cols=2, seed=0):
    rng = np.random.default_rng(seed)
    data = rng.normal(size=(len(race_ids), n_cols))
    return pd.DataFrame(data, index=race_ids, columns=[f"c{i}" for i in range(n_cols)])


def test_existing_race_ids_empty():
    assert existing_race_ids(pd.DataFrame()) == set()


def test_existing_race_ids_returns_index_set():
    df = _df(["r1", "r2", "r2"])
    ids = existing_race_ids(df)
    assert ids == {"r1", "r2"}


def test_find_new_race_ids_filters_known():
    new = find_new_race_ids({"r1", "r2"}, ["r1", "r3", "r4"])
    assert new == ["r3", "r4"]


def test_find_new_race_ids_all_new():
    assert find_new_race_ids(set(), ["r1", "r2"]) == ["r1", "r2"]


def test_find_new_race_ids_all_known():
    assert find_new_race_ids({"r1"}, ["r1"]) == []


def test_append_idempotent_no_overlap():
    existing = _df(["r1"])
    new = _df(["r2", "r3"], seed=1)
    result = append_idempotent(existing, new)
    assert set(result.index) == {"r1", "r2", "r3"}


def test_append_idempotent_skips_duplicates():
    existing = _df(["r1", "r2"])
    new = _df(["r2", "r3"], seed=1)  # r2 は重複
    result = append_idempotent(existing, new)
    assert list(result.index).count("r2") == 1


def test_append_idempotent_empty_existing():
    new = _df(["r1", "r2"])
    result = append_idempotent(pd.DataFrame(), new)
    assert set(result.index) == {"r1", "r2"}


def test_append_idempotent_empty_new():
    existing = _df(["r1"])
    result = append_idempotent(existing, pd.DataFrame())
    assert set(result.index) == {"r1"}


# ---------------------------------------------------------------------------
# load_raw / save_raw
# ---------------------------------------------------------------------------


def test_load_raw_missing_file_returns_empty(tmp_path):
    df = load_raw(str(tmp_path / "nope.pkl"))
    assert df.empty


def test_save_load_raw_roundtrip(tmp_path):
    path = str(tmp_path / "sub" / "test.pkl")
    df = _df(["r1", "r2"])
    save_raw(df, path)
    loaded = load_raw(path)
    assert list(loaded.index) == ["r1", "r2"]


# ---------------------------------------------------------------------------
# IngestJob (スタブ DI)
# ---------------------------------------------------------------------------


def _make_config(tmp_path, save_featured_to_db=False):
    base = str(tmp_path)
    return IngestConfig(
        raw_results_path=os.path.join(base, "results.pkl"),
        raw_race_info_path=os.path.join(base, "race_info.pkl"),
        raw_return_tables_path=os.path.join(base, "returns.pkl"),
        raw_horse_results_path=os.path.join(base, "horse_results.pkl"),
        raw_horse_info_path=os.path.join(base, "horse_info.pkl"),
        raw_peds_path=os.path.join(base, "peds.pkl"),
        featured_data_path=os.path.join(base, "featured.pkl"),
        # 既定では実 DB を汚さないよう DB 保存を無効化（DB 保存は専用テストで検証）
        save_featured_to_db=save_featured_to_db,
    )


class _StubFetcher:
    def fetch_results(self, race_ids):
        return _df(race_ids, seed=0)

    def fetch_race_info(self, race_ids):
        return _df(race_ids, seed=1)

    def fetch_return_tables(self, race_ids):
        return _df(race_ids, seed=2)


class _StubBuilder:
    def build(self, config):
        return _df(["r_feat"])


def test_ingest_run_stores_new_races(tmp_path):
    cfg = _make_config(tmp_path)
    job = IngestJob(_StubFetcher(), _StubBuilder(), cfg)
    result = job.run(["r1", "r2"])
    assert result["status"] == "ok"
    assert result["n_new"] == 2
    loaded = load_raw(cfg.raw_results_path)
    assert set(loaded.index) == {"r1", "r2"}


def test_ingest_run_idempotent_on_second_call(tmp_path):
    cfg = _make_config(tmp_path)
    job = IngestJob(_StubFetcher(), _StubBuilder(), cfg)
    job.run(["r1"])
    result = job.run(["r1"])  # 同じ race_id を再投入
    assert result["status"] == "no_new_races"
    assert result["n_new"] == 0


def test_ingest_run_appends_incrementally(tmp_path):
    cfg = _make_config(tmp_path)
    job = IngestJob(_StubFetcher(), _StubBuilder(), cfg)
    job.run(["r1"])
    job.run(["r2", "r3"])
    loaded = load_raw(cfg.raw_results_path)
    assert set(loaded.index) == {"r1", "r2", "r3"}


def test_ingest_run_saves_featured_data(tmp_path):
    cfg = _make_config(tmp_path)
    job = IngestJob(_StubFetcher(), _StubBuilder(), cfg)
    job.run(["r1"])
    assert os.path.exists(cfg.featured_data_path)


def test_ingest_saves_featured_snapshot_to_db(tmp_path, monkeypatch):
    """save_featured_to_db=True で featured スナップショットが DB に保存される（Phase 2）。

    IngestJob 内部の FeaturedDataRepo() は db_path 省略で LocalPaths.DB_PATH を引くため、
    実 DB を汚さないよう DB_PATH を tmp に差し替え、エンジンシングルトンをリセットする。
    """
    from src.constants._local_paths import LocalPaths
    from src.storage._db import _reset_engine_for_testing
    from src.storage._featured_repo import FeaturedDataRepo

    db_path = str(tmp_path / "test.db")
    monkeypatch.setattr(LocalPaths, "DB_PATH", db_path)
    _reset_engine_for_testing()

    cfg = _make_config(tmp_path, save_featured_to_db=True)
    job = IngestJob(_StubFetcher(), _StubBuilder(), cfg)
    job.run(["r1"])

    repo = FeaturedDataRepo()  # db_path 省略 → 差し替えた tmp DB を参照
    metas = repo.list_meta()
    assert len(metas) == 1
    loaded = repo.load()
    assert loaded is not None and len(loaded) == 1
    _reset_engine_for_testing()
