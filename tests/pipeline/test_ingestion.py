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


def _make_config(tmp_path):
    base = str(tmp_path)
    return IngestConfig(
        raw_results_path=os.path.join(base, "results.pkl"),
        raw_race_info_path=os.path.join(base, "race_info.pkl"),
        raw_return_tables_path=os.path.join(base, "returns.pkl"),
        raw_horse_results_path=os.path.join(base, "horse_results.pkl"),
        raw_horse_info_path=os.path.join(base, "horse_info.pkl"),
        raw_peds_path=os.path.join(base, "peds.pkl"),
        featured_data_path=os.path.join(base, "featured.pkl"),
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


# ---------------------------------------------------------------------------
# index 正規化（append_idempotent の前提を揃えるヘルパ）
# ---------------------------------------------------------------------------


def test_normalize_race_id_index_from_range_index():
    from src.pipeline._ingestion import normalize_race_id_index

    df = pd.DataFrame({"race_id": [101, 102], "v": [1, 2]})
    out = normalize_race_id_index(df)
    assert out.index.name == "race_id"
    assert list(out.index) == [101, 102]


def test_normalize_race_id_index_already_indexed_is_noop():
    from src.pipeline._ingestion import normalize_race_id_index

    df = pd.DataFrame({"v": [1]}, index=pd.Index([101], name="race_id"))
    out = normalize_race_id_index(df)
    assert out is df


def test_to_raw_format_restores_race_id_column():
    from src.pipeline._ingestion import to_raw_format

    df = pd.DataFrame({"v": [1, 2]}, index=pd.Index([101, 102], name="race_id"))
    out = to_raw_format(df)
    assert "race_id" in out.columns
    assert list(out.index) == [0, 1]


def test_append_idempotent_mixed_index_does_not_duplicate():
    """既存(RangeIndex+race_id列)と新規(race_id index)を正規化してから比較すると二重化しない。"""
    from src.pipeline._ingestion import normalize_race_id_index

    existing = pd.DataFrame({"race_id": [101, 102], "v": [1, 2]})
    new = pd.DataFrame({"v": [2, 3]}, index=pd.Index([102, 103], name="race_id"))
    result = append_idempotent(normalize_race_id_index(existing), new)
    assert sorted(result.index) == [101, 102, 103]
