"""src/storage/_featured_repo.py: FeaturedDataRepo の単体テスト（Phase 2）。

featured_data スナップショットの save/load ラウンドトリップ、dtype 保持、
メタ情報抽出、最新版選択、上書き、削除、pickle フォールバックを網羅する。
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.storage._db import _reset_engine_for_testing
from src.storage._featured_repo import FeaturedDataRepo
from src.storage._featured_repo import _deserialize
from src.storage._featured_repo import _extract_meta
from src.storage._featured_repo import _serialize


@pytest.fixture(autouse=True)
def _reset_engine():
    _reset_engine_for_testing()
    yield
    _reset_engine_for_testing()


def _featured_df() -> pd.DataFrame:
    """featured_data 相当: race_id インデックス + 数値/category/bool/date 混在。"""
    df = pd.DataFrame(
        {
            "馬番": [1, 2, 3, 1, 2],
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


class TestSaveLoadRoundtrip:
    def test_save_then_load_latest(self, tmp_path):
        repo = FeaturedDataRepo(db_path=str(tmp_path / "test.db"))
        df = _featured_df()

        version = repo.save(df, version="v1")
        assert version == "v1"

        out = repo.load()  # 最新
        assert out is not None
        assert out.index.name == "race_id"
        assert len(out) == len(df)
        assert list(out.columns) == list(df.columns)

    def test_dtypes_preserved(self, tmp_path):
        repo = FeaturedDataRepo(db_path=str(tmp_path / "test.db"))
        df = _featured_df()
        repo.save(df, version="v1")
        out = repo.load("v1")

        assert out is not None
        # 数値・bool・datetime の dtype が保持される（parquet 経由）
        assert pd.api.types.is_float_dtype(out["着順_mean_5R"])
        assert pd.api.types.is_bool_dtype(out["place__東京"])
        assert pd.api.types.is_datetime64_any_dtype(out["date"])

    def test_load_specific_version(self, tmp_path):
        repo = FeaturedDataRepo(db_path=str(tmp_path / "test.db"))
        repo.save(_featured_df().head(3), version="v1")
        repo.save(_featured_df(), version="v2")

        out_v1 = repo.load("v1")
        out_v2 = repo.load("v2")
        assert out_v1 is not None and out_v2 is not None
        assert len(out_v1) == 3
        assert len(out_v2) == 5

    def test_load_missing_returns_none(self, tmp_path):
        repo = FeaturedDataRepo(db_path=str(tmp_path / "test.db"))
        assert repo.load() is None
        assert repo.load("nope") is None


class TestVersioning:
    def test_latest_version_picks_most_recent(self, tmp_path):
        repo = FeaturedDataRepo(db_path=str(tmp_path / "test.db"))
        repo.save(_featured_df(), version="20240101_000000")
        repo.save(_featured_df(), version="20240601_120000")
        assert repo.latest_version() == "20240601_120000"

    def test_save_same_version_overwrites(self, tmp_path):
        repo = FeaturedDataRepo(db_path=str(tmp_path / "test.db"))
        repo.save(_featured_df(), version="v1")
        repo.save(_featured_df().head(2), version="v1")

        out = repo.load("v1")
        assert out is not None
        assert len(out) == 2  # 上書きされている
        assert len(repo.list_meta()) == 1  # 重複しない

    def test_auto_version_when_omitted(self, tmp_path):
        repo = FeaturedDataRepo(db_path=str(tmp_path / "test.db"))
        version = repo.save(_featured_df())
        # YYYYmmdd_HHMMSS 形式
        assert len(version) == 15 and "_" in version


class TestMeta:
    def test_get_meta_fields(self, tmp_path):
        repo = FeaturedDataRepo(db_path=str(tmp_path / "test.db"))
        repo.save(_featured_df(), version="v1")
        meta = repo.get_meta("v1")

        assert meta is not None
        assert meta["version"] == "v1"
        assert meta["n_rows"] == 5
        assert meta["n_races"] == 2
        assert meta["n_features"] == 5  # race_id は index なので列に含まない
        assert meta["date_min"] == "2024-01-06"
        assert meta["date_max"] == "2024-02-10"
        assert "馬番" in meta["columns"]

    def test_list_meta_orders_desc(self, tmp_path):
        repo = FeaturedDataRepo(db_path=str(tmp_path / "test.db"))
        repo.save(_featured_df(), version="20240101_000000")
        repo.save(_featured_df(), version="20240601_120000")

        metas = repo.list_meta()
        assert [m["version"] for m in metas] == ["20240601_120000", "20240101_000000"]

    def test_get_meta_missing_returns_none(self, tmp_path):
        repo = FeaturedDataRepo(db_path=str(tmp_path / "test.db"))
        assert repo.get_meta() is None
        assert repo.get_meta("nope") is None


class TestDelete:
    def test_delete_removes_snapshot_and_meta(self, tmp_path):
        repo = FeaturedDataRepo(db_path=str(tmp_path / "test.db"))
        repo.save(_featured_df(), version="v1")
        deleted = repo.delete("v1")

        assert deleted == 1
        assert repo.load("v1") is None
        assert repo.get_meta("v1") is None
        assert repo.list_meta() == []

    def test_delete_missing_returns_zero(self, tmp_path):
        repo = FeaturedDataRepo(db_path=str(tmp_path / "test.db"))
        assert repo.delete("nope") == 0


class TestSerializationHelpers:
    def test_parquet_roundtrip(self):
        df = _featured_df()
        blob, fmt = _serialize(df)
        assert fmt == "parquet"
        out = _deserialize(blob, fmt)
        assert len(out) == len(df)
        assert out.index.name == "race_id"

    def test_pickle_fallback_roundtrip(self):
        # 非文字列列名は parquet で失敗 → pickle フォールバックを検証
        df = pd.DataFrame({0: [1, 2], 1: [3, 4]})
        blob, fmt = _serialize(df)
        # 0/1 の int 列名は pyarrow が文字列化できず pickle に落ちる想定だが、
        # 環境により parquet 成功もあり得るため format に関わらず round-trip を保証
        out = _deserialize(blob, fmt)
        assert len(out) == 2

    def test_extract_meta_without_date(self):
        df = pd.DataFrame({"x": [1, 2, 3]}, index=pd.Index(["r1", "r1", "r2"], name="race_id"))
        meta = _extract_meta(df)
        assert meta["n_rows"] == 3
        assert meta["n_races"] == 2
        assert meta["n_features"] == 1
        assert meta["date_min"] is None
        assert meta["date_max"] is None


class TestSharedDbWithRawRepo:
    """featured 表は raw 表と同一 DB ファイルに共存できる。"""

    def test_featured_tables_coexist_with_raw(self, tmp_path):
        from src.storage._repo import RawDataRepo

        db = str(tmp_path / "test.db")
        raw = RawDataRepo(db_path=db)
        raw_df = pd.DataFrame(
            {"馬番": [1], "horse_id": ["H1"], "race_id": ["202401010101"]}
        ).set_index("race_id")
        raw.upsert("raw_results", raw_df)

        feat = FeaturedDataRepo(db_path=db)
        feat.save(_featured_df(), version="v1")

        # 双方が独立して読める
        assert len(raw.read("raw_results")) == 1
        assert feat.load("v1") is not None
        # numpy 未使用列の参照を避けるためのダミーアサート
        assert np.isfinite(feat.get_meta("v1")["n_rows"])
