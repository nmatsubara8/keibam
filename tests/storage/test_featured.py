"""src/storage/_featured.py: featured_data の Parquet 永続化 + メタ表のテスト。

PR #3 で導入された正準の featured_data 永続化（parquet ファイル + SQLite
`featured_data_meta` メタ表）の回帰ガード。各テストは `tmp_path` に DB / parquet を
切り、`_reset_engine_for_testing` でエンジンシングルトンをリセットする。
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.storage._db import _reset_engine_for_testing
from src.storage._featured import (
    load_featured_meta,
    load_parquet,
    save_featured_meta,
    save_parquet,
)


@pytest.fixture(autouse=True)
def _reset_engine():
    _reset_engine_for_testing()
    yield
    _reset_engine_for_testing()


def _featured_df() -> pd.DataFrame:
    """featured_data 相当: race_id インデックス + 数値/category/Int64/datetime/bool 混在。"""
    df = pd.DataFrame(
        {
            "馬番": pd.array([1, 2, 3, 1, 2], dtype="Int64"),
            "着順_mean_5R": [3.2, 1.1, 2.5, 4.0, 1.5],
            "place__東京": [True, False, True, False, True],
            "horse_id": pd.Categorical(["a", "b", "c", "a", "d"]),
            "date": pd.to_datetime(
                ["2024-01-06", "2024-01-06", "2024-01-06", "2024-02-10", "2024-02-10"]
            ),
            "race_id": [
                "202401010101",
                "202401010101",
                "202401010101",
                "202402010101",
                "202402010101",
            ],
        }
    )
    return df.set_index("race_id")


# ---------------------------------------------------------------------------
# Parquet roundtrip
# ---------------------------------------------------------------------------


class TestParquetRoundtrip:
    def test_roundtrip_preserves_shape_and_index(self, tmp_path):
        path = str(tmp_path / "f.parquet")
        df = _featured_df()
        save_parquet(df, path)

        out = load_parquet(path)
        assert out.index.name == "race_id"
        assert len(out) == len(df)
        assert list(out.columns) == list(df.columns)

    def test_roundtrip_preserves_dtypes(self, tmp_path):
        path = str(tmp_path / "f.parquet")
        df = _featured_df()
        save_parquet(df, path)
        out = load_parquet(path)

        # _restore_dtypes が category / Int64 を復元し、pyarrow が bool/datetime を保つ
        assert str(out["horse_id"].dtype) == "category"
        assert str(out["馬番"].dtype) == "Int64"
        assert pd.api.types.is_float_dtype(out["着順_mean_5R"])
        assert pd.api.types.is_bool_dtype(out["place__東京"])
        assert pd.api.types.is_datetime64_any_dtype(out["date"])

    def test_load_missing_returns_empty(self, tmp_path):
        out = load_parquet(str(tmp_path / "nope.parquet"))
        assert isinstance(out, pd.DataFrame)
        assert out.empty


# ---------------------------------------------------------------------------
# featured_data_meta テーブル
# ---------------------------------------------------------------------------


class TestFeaturedMeta:
    def test_save_then_load_latest(self, tmp_path):
        db = str(tmp_path / "test.db")
        parquet_path = str(tmp_path / "f.parquet")
        df = _featured_df()

        save_featured_meta(df, parquet_path=parquet_path, db_path=db)
        meta = load_featured_meta(db_path=db)

        assert meta is not None
        assert meta["n_rows"] == 5
        assert meta["n_cols"] == len(df.columns)
        assert meta["min_race_id"] == "202401010101"
        assert meta["max_race_id"] == "202402010101"
        assert meta["parquet_path"] == parquet_path
        # schema_json は列 dtype のスナップショット
        assert "horse_id" in meta["schema_json"]

    def test_load_returns_most_recent(self, tmp_path):
        db = str(tmp_path / "test.db")
        df = _featured_df()

        save_featured_meta(df.head(3), parquet_path="p1.parquet", db_path=db)
        save_featured_meta(df, parquet_path="p2.parquet", db_path=db)

        meta = load_featured_meta(db_path=db)
        assert meta is not None
        # id 降順で最新（2 回目: 全 5 行 / p2.parquet）が返る
        assert meta["n_rows"] == 5
        assert meta["parquet_path"] == "p2.parquet"

    def test_load_none_when_empty(self, tmp_path):
        db = str(tmp_path / "test.db")
        # get_engine がテーブルを作るが行は無い
        assert load_featured_meta(db_path=db) is None


# ---------------------------------------------------------------------------
# parquet + meta を組み合わせた典型運用（ingest 相当）
# ---------------------------------------------------------------------------


class TestSaveParquetAndMetaTogether:
    def test_parquet_path_recorded_in_meta(self, tmp_path):
        db = str(tmp_path / "test.db")
        parquet_path = str(tmp_path / "featured.parquet")
        df = _featured_df()

        save_parquet(df, parquet_path)
        save_featured_meta(df, parquet_path=parquet_path, db_path=db)

        meta = load_featured_meta(db_path=db)
        assert meta is not None
        # メタに記録された parquet から復元できる
        restored = load_parquet(meta["parquet_path"])
        assert len(restored) == meta["n_rows"]
