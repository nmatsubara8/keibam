"""src/storage/_repo.py: RawDataRepo の単体テスト。

各テストは `tmp_path / "test.db"` に新規 SQLite ファイルを切り、
`_reset_engine_for_testing` でシングルトンをクリアしてから `get_engine(db_path=...)`
で一時 DB に向ける。pickle/_db.py の正常系・冪等性・PK 衝突回避を網羅する。
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.storage._db import _reset_engine_for_testing
from src.storage._repo import RawDataRepo


@pytest.fixture(autouse=True)
def _reset_engine():
    # 各テスト前後でエンジンをリセット（前のテストの DB を引きずらない）
    _reset_engine_for_testing()
    yield
    _reset_engine_for_testing()


def _results_df() -> pd.DataFrame:
    """raw_results 相当のダミー DataFrame（index=race_id, 列に 馬番/horse_id）。"""
    df = pd.DataFrame(
        {
            "馬番": [1, 2, 3],
            "horse_id": ["H001", "H002", "H003"],
            "着順": [3, 1, 2],
            "race_id": ["202401010101", "202401010101", "202401010101"],
        }
    )
    df = df.set_index("race_id")
    return df


def _return_df_multi_rows() -> pd.DataFrame:
    """raw_return_tables 相当: 同一 race_id に複数行（馬券種別ごと）。"""
    df = pd.DataFrame(
        {
            0: ["単勝", "複勝", "馬連"],
            1: ["3", "3br2", "2-3"],
            2: ["150", "120br110", "320"],
            "race_id": ["202401010101", "202401010101", "202401010101"],
        }
    )
    df = df.set_index("race_id")
    return df


class TestUpsertReadRoundtrip:
    """upsert → read で index/列が保持されることを確認する。"""

    def test_upsert_then_read_preserves_index(self, tmp_path):
        repo = RawDataRepo(db_path=str(tmp_path / "test.db"))
        df = _results_df()

        inserted = repo.upsert("raw_results", df)
        assert inserted == 3

        out = repo.read("raw_results")
        assert out.index.name == "race_id"
        assert set(out.index.unique()) == {"202401010101"}
        assert "馬番" in out.columns
        assert "horse_id" in out.columns

    def test_read_restores_numeric_dtypes(self, tmp_path):
        """DB(テキスト保持)から読むと数値列は数値型に復元され、ID/混在列は文字列維持。"""
        import pandas as pd

        repo = RawDataRepo(db_path=str(tmp_path / "test.db"))
        df = _results_df().reset_index()
        df["着順"] = ["1", "中", "2"]  # 非数値混在 → 文字列維持されるべき
        df = df.set_index("race_id")
        repo.upsert("raw_results", df)

        out = repo.read("raw_results")
        # 純粋数値列（馬番）は数値型に復元され、除算等が可能
        assert pd.api.types.is_numeric_dtype(out["馬番"])
        assert (out["馬番"] / 2).notna().all()
        # ID 列・混在列は文字列のまま
        assert out.index.dtype == object
        assert set(out.index.unique()) == {"202401010101"}
        assert out["horse_id"].map(type).eq(str).all()
        assert out["着順"].map(lambda v: isinstance(v, str)).all()

    def test_upsert_inserts_all_rows(self, tmp_path):
        repo = RawDataRepo(db_path=str(tmp_path / "test.db"))
        df = _results_df()
        repo.upsert("raw_results", df)
        out = repo.read("raw_results")
        assert len(out) == 3


class TestIdempotency:
    """同じ DataFrame を 2 回 upsert しても重複しない（INSERT OR IGNORE）。"""

    def test_double_upsert_does_not_duplicate(self, tmp_path):
        repo = RawDataRepo(db_path=str(tmp_path / "test.db"))
        df = _results_df()

        first = repo.upsert("raw_results", df)
        second = repo.upsert("raw_results", df)
        out = repo.read("raw_results")

        assert first == 3
        # 2 回目は全て PK 衝突で IGNORE され、件数は増えない
        assert len(out) == 3
        # second の rowcount は SQLite 実装依存だが、件数が増えていないことが本質
        _ = second


class TestDeleteAndReupsert:
    """delete 後に再 upsert すると新行として入る。"""

    def test_delete_by_pk_then_reupsert(self, tmp_path):
        repo = RawDataRepo(db_path=str(tmp_path / "test.db"))
        df = _results_df()
        repo.upsert("raw_results", df)

        # 1 行だけ消す
        deleted = repo.delete("raw_results", [("202401010101", "1")])
        assert deleted == 1
        assert len(repo.read("raw_results")) == 2

        # 同じデータを再 upsert → 消した 1 行だけが入る
        re_inserted = repo.upsert("raw_results", df)
        assert re_inserted == 1
        assert len(repo.read("raw_results")) == 3

    def test_delete_by_index_clears_all_rows_for_race(self, tmp_path):
        repo = RawDataRepo(db_path=str(tmp_path / "test.db"))
        df = _results_df()
        repo.upsert("raw_results", df)

        deleted = repo.delete_by_index("raw_results", ["202401010101"])
        assert deleted == 3
        assert len(repo.read("raw_results")) == 0


class TestReturnTablesPK:
    """raw_return_tables: (race_id, row_idx) PK で同一 race_id 複数行が衝突しない。"""

    def test_multiple_rows_per_race_inserted(self, tmp_path):
        repo = RawDataRepo(db_path=str(tmp_path / "test.db"))
        df = _return_df_multi_rows()

        inserted = repo.upsert("raw_return_tables", df)
        assert inserted == 3

        out = repo.read("raw_return_tables")
        # 3 行全てが残り、row_idx は 0/1/2 を持つ
        assert len(out) == 3
        assert set(out["row_idx"].astype(int).tolist()) == {0, 1, 2}

    def test_re_upsert_does_not_double_rows(self, tmp_path):
        repo = RawDataRepo(db_path=str(tmp_path / "test.db"))
        df = _return_df_multi_rows()

        repo.upsert("raw_return_tables", df)
        repo.upsert("raw_return_tables", df)

        out = repo.read("raw_return_tables")
        assert len(out) == 3


class TestAutoMigrate:
    """auto_migrate_if_empty: 空 DB + pickle 有りで初回のみ移行する。"""

    def test_migrates_when_db_empty_and_pickle_exists(self, tmp_path):
        # ダミー pickle を作る
        pkl_path = tmp_path / "results.pkl"
        _results_df().to_pickle(str(pkl_path))

        repo = RawDataRepo(db_path=str(tmp_path / "test.db"))
        assert not repo.has_rows("raw_results")

        migrated = repo.auto_migrate_if_empty("raw_results", str(pkl_path))
        assert migrated == 3
        assert repo.has_rows("raw_results")

    def test_skips_when_db_already_has_rows(self, tmp_path):
        pkl_path = tmp_path / "results.pkl"
        _results_df().to_pickle(str(pkl_path))

        repo = RawDataRepo(db_path=str(tmp_path / "test.db"))
        repo.upsert("raw_results", _results_df())

        # 既に行があるので migrate は no-op
        migrated = repo.auto_migrate_if_empty("raw_results", str(pkl_path))
        assert migrated == 0

    def test_skips_when_pickle_missing(self, tmp_path):
        repo = RawDataRepo(db_path=str(tmp_path / "test.db"))
        migrated = repo.auto_migrate_if_empty("raw_results", str(tmp_path / "nope.pkl"))
        assert migrated == 0


class TestRangeIndexGuard:
    """既定 RangeIndex を race_id に昇格させない（行番号→race_id のデータ破損を防ぐ）。"""

    def test_rangeindex_without_race_id_raises(self, tmp_path):
        repo = RawDataRepo(db_path=str(tmp_path / "test.db"))
        # race_id 列も race_id index も無い（既定 RangeIndex）→ 破損を防ぐため弾く
        df = pd.DataFrame({"馬番": [1, 2, 3], "horse_id": ["H1", "H2", "H3"]})
        with pytest.raises(ValueError, match="RangeIndex"):
            repo.upsert("raw_results", df)

    def test_race_id_column_with_rangeindex_is_ok(self, tmp_path):
        """正準形式（RangeIndex + race_id 列）は従来どおり通る。"""
        repo = RawDataRepo(db_path=str(tmp_path / "test.db"))
        df = pd.DataFrame(
            {
                "馬番": [1, 2, 3],
                "horse_id": ["H1", "H2", "H3"],
                "race_id": ["202401010101", "202401010101", "202401010101"],
            }
        )  # RangeIndex + race_id 列
        inserted = repo.upsert("raw_results", df)
        assert inserted == 3
        out = repo.read("raw_results")
        assert set(out.index.unique()) == {"202401010101"}


class TestUnknownAlias:
    def test_upsert_unknown_alias_raises(self, tmp_path):
        repo = RawDataRepo(db_path=str(tmp_path / "test.db"))
        with pytest.raises(ValueError, match="unknown alias"):
            repo.upsert("does_not_exist", _results_df())

    def test_read_unknown_alias_raises(self, tmp_path):
        repo = RawDataRepo(db_path=str(tmp_path / "test.db"))
        with pytest.raises(ValueError, match="unknown alias"):
            repo.read("does_not_exist")
