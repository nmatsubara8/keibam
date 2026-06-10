"""raw データ用 SQLite リポジトリ。

`RawDataRepo` は `TABLE_SPECS` レジストリを介して、各 raw DataFrame の
upsert / read / delete / 自動移行を提供する。

設計の要点:
- 既存 Processor が読むのは pickle なので、ここの API は「pickle 揮発時の保険」
  および「監査ログ的な持続化」に徹する。読み取り API は `read(alias)` のみで、
  通常運用では呼ばれない。
- 冪等 upsert: SQLite の `INSERT OR IGNORE` を使う。同じ主キーは無視されるため、
  同じ DataFrame を何度 upsert しても重複しない。
- 列の動的追加: scrape DataFrame は alias ごとに列が一定でないため、初回 upsert 時に
  `ensure_columns` で `ALTER TABLE ADD COLUMN` を呼ぶ。型はすべて TEXT、read 時に dtype 推論する。
- 自動移行 `auto_migrate_if_empty`: 初回起動時に DB が空 & pickle あり → 一括取込。
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import pandas as pd
from sqlalchemy import text

from src.storage._db import TABLE_SPECS
from src.storage._db import alias_to_pickle_path
from src.storage._db import ensure_columns
from src.storage._db import get_engine

logger = logging.getLogger(__name__)


class RawDataRepo:
    """raw データの SQLite 永続化リポジトリ。

    `db_path=None` の場合は `LocalPaths.DB_PATH` を使用する。
    テスト時には `db_path=tmp_path / "test.db"` のように差し替える。
    """

    def __init__(self, db_path: Optional[str] = None) -> None:
        self._engine = get_engine(db_path)

    # ------------------------------------------------------------------
    # upsert
    # ------------------------------------------------------------------

    def upsert(self, alias: str, df: pd.DataFrame) -> int:
        """DataFrame の各行を `INSERT OR IGNORE` で投入する。

        Returns
        -------
        int : 実際に INSERT された行数（重複 IGNORE された行は含まない）。
        """
        if alias not in TABLE_SPECS:
            raise ValueError(f"unknown alias: {alias}")
        if df is None or df.empty:
            return 0

        spec = TABLE_SPECS[alias]

        # DataFrame を「すべてカラム持ちの DataFrame」に正規化する。
        # 1) index を column 化（index_col が指定されていれば）
        # 2) auto_row_idx_col が True なら、index ごとの cumcount で row_idx 列を付与
        df_norm = df.copy()
        if spec.index_col is not None:
            if df_norm.index.name != spec.index_col:
                # index 名が違う場合でも値は妥当（呼出元の DataFrame 構造を信用）
                df_norm.index = df_norm.index.rename(spec.index_col)
            df_norm = df_norm.reset_index()

        if spec.auto_row_idx_col:
            # 主キーが (race_id, row_idx) のような場合、同じ index 内での連番を付与
            # 既に row_idx 列がある場合はそれを尊重する
            if "row_idx" not in df_norm.columns:
                group_key = spec.index_col if spec.index_col else spec.primary_key[0]
                df_norm["row_idx"] = df_norm.groupby(group_key).cumcount()

        # 主キー列が DataFrame に揃っているかチェック
        missing_pk = [c for c in spec.primary_key if c not in df_norm.columns]
        if missing_pk:
            raise ValueError(
                f"upsert({alias}): 主キー列 {missing_pk} が DataFrame に存在しません"
            )

        # 列名を全て str 化する（払戻 raw のように int 列名を持つ DataFrame でも
        # SQLite の列名（PRAGMA table_info）と一致するように揃える）。
        df_norm.columns = [str(c) for c in df_norm.columns]

        # 全列を文字列化（SQLite TEXT に統一保存）。NaN は None に変換。
        # 列名は scrape 由来の日本語列を含むため、二重引用符でクォートする。
        df_norm = df_norm.where(pd.notna(df_norm), None)
        for col in df_norm.columns:
            df_norm[col] = df_norm[col].map(lambda v: None if v is None else str(v))

        # 不足列を ALTER TABLE で追加（PK 以外の自由列のみ対象）
        free_cols = [c for c in df_norm.columns if c not in spec.primary_key]
        if free_cols:
            ensure_columns(self._engine, spec.table_name, free_cols)

        col_names = list(df_norm.columns)
        col_sql = ", ".join(f'"{c}"' for c in col_names)
        param_sql = ", ".join(f":{_safe_param_name(c, i)}" for i, c in enumerate(col_names))
        sql = (
            f'INSERT OR IGNORE INTO "{spec.table_name}" ({col_sql}) VALUES ({param_sql})'
        )

        # 行ごとに dict 化（パラメタ名は安全な英数字に正規化）
        rows = []
        for row in df_norm.itertuples(index=False, name=None):
            rows.append(
                {_safe_param_name(c, i): v for i, (c, v) in enumerate(zip(col_names, row, strict=True))}
            )

        with self._engine.begin() as conn:
            result = conn.execute(text(sql), rows)
            inserted = result.rowcount if result.rowcount is not None and result.rowcount >= 0 else 0

        logger.info("[RawDataRepo] upsert %s: %d/%d rows inserted", alias, inserted, len(rows))
        return inserted

    # ------------------------------------------------------------------
    # read
    # ------------------------------------------------------------------

    def read(self, alias: str) -> pd.DataFrame:
        """DB から DataFrame を復元する（pickle 互換の index/列構造）。"""
        if alias not in TABLE_SPECS:
            raise ValueError(f"unknown alias: {alias}")
        spec = TABLE_SPECS[alias]

        with self._engine.connect() as conn:
            # ingested_at は内部メタなので返却 DataFrame から除外する
            sql = f'SELECT * FROM "{spec.table_name}"'
            df = pd.read_sql(text(sql), conn)

        if "ingested_at" in df.columns:
            df = df.drop(columns=["ingested_at"])

        # auto_row_idx_col で自動付与した row_idx は、return_tables の場合は
        # 「raw DataFrame の元構造」には含まれていなかった列なので、PK の補助情報として
        # 残すか落とすかは alias に依存する。Phase 1 では「保持」を選び、
        # _build_bet_df 側の正規化に影響しないよう row_idx は数値復元する。
        if spec.auto_row_idx_col and "row_idx" in df.columns:
            df["row_idx"] = pd.to_numeric(df["row_idx"], errors="coerce").astype("Int64")

        # index_col 指定があれば index に戻す
        if spec.index_col is not None and spec.index_col in df.columns:
            df = df.set_index(spec.index_col)

        return df

    # ------------------------------------------------------------------
    # delete
    # ------------------------------------------------------------------

    def delete(self, alias: str, keys: list[tuple]) -> int:
        """主キー値で行を削除する（--force 用）。

        keys は spec.primary_key と同じ順序のタプルのリスト。
        例: alias='raw_results', keys=[("202401010101", "1"), ("202401010101", "2")]
        """
        if alias not in TABLE_SPECS:
            raise ValueError(f"unknown alias: {alias}")
        spec = TABLE_SPECS[alias]
        if not keys:
            return 0

        where_sql = " AND ".join(f'"{c}" = :{_safe_param_name(c, i)}' for i, c in enumerate(spec.primary_key))
        sql = f'DELETE FROM "{spec.table_name}" WHERE {where_sql}'

        rows = []
        for k in keys:
            if len(k) != len(spec.primary_key):
                raise ValueError(
                    f"delete({alias}): キーの長さが PK 列数 {len(spec.primary_key)} と一致しません: {k}"
                )
            rows.append(
                {
                    _safe_param_name(c, i): str(v)
                    for i, (c, v) in enumerate(zip(spec.primary_key, k, strict=True))
                }
            )

        with self._engine.begin() as conn:
            result = conn.execute(text(sql), rows)
            deleted = result.rowcount if result.rowcount is not None and result.rowcount >= 0 else 0

        logger.info("[RawDataRepo] delete %s: %d rows deleted", alias, deleted)
        return deleted

    def delete_by_index(self, alias: str, index_values: list) -> int:
        """index_col の値（race_id / horse_id 等）で行を一括削除する。

        --force ingest で race_id 単位に既存行を消すユースケース向け。
        """
        if alias not in TABLE_SPECS:
            raise ValueError(f"unknown alias: {alias}")
        spec = TABLE_SPECS[alias]
        if spec.index_col is None:
            raise ValueError(f"delete_by_index: alias={alias} には index_col がありません")
        if not index_values:
            return 0

        # IN 句のパラメタを動的生成
        placeholders = ", ".join(f":v{i}" for i in range(len(index_values)))
        sql = f'DELETE FROM "{spec.table_name}" WHERE "{spec.index_col}" IN ({placeholders})'
        params = {f"v{i}": str(v) for i, v in enumerate(index_values)}

        with self._engine.begin() as conn:
            result = conn.execute(text(sql), params)
            deleted = result.rowcount if result.rowcount is not None and result.rowcount >= 0 else 0

        logger.info("[RawDataRepo] delete_by_index %s: %d rows deleted", alias, deleted)
        return deleted

    # ------------------------------------------------------------------
    # メタ操作
    # ------------------------------------------------------------------

    def has_rows(self, alias: str) -> bool:
        """テーブルに 1 行でも存在するか。"""
        if alias not in TABLE_SPECS:
            raise ValueError(f"unknown alias: {alias}")
        spec = TABLE_SPECS[alias]
        with self._engine.connect() as conn:
            row = conn.execute(text(f'SELECT 1 FROM "{spec.table_name}" LIMIT 1')).fetchone()
        return row is not None

    def auto_migrate_if_empty(self, alias: str, pickle_path: Optional[str] = None) -> int:
        """DB が空 & pickle が存在する場合、pickle 全件を DB に取込む。

        起動時に 1 回呼ぶ想定。既に DB に行があれば何もしない（0 を返す）。

        Parameters
        ----------
        pickle_path : 明示指定があればそのパス、None なら alias から逆引きする。
        """
        if pickle_path is None:
            pickle_path = alias_to_pickle_path(alias)
        if pickle_path is None:
            logger.info("[auto_migrate] %s: pickle path 不明のためスキップ", alias)
            return 0

        if self.has_rows(alias):
            return 0
        if not os.path.exists(pickle_path):
            return 0

        try:
            df = pd.read_pickle(pickle_path)
        except Exception as e:  # noqa: BLE001
            logger.warning("[auto_migrate] %s: pickle 読込失敗 %s", alias, e)
            return 0

        inserted = self.upsert(alias, df)
        logger.info("[auto_migrate] %s: %d rows migrated from %s", alias, inserted, pickle_path)
        return inserted


def _safe_param_name(col: str, idx: int) -> str:
    """日本語列名等を SQLAlchemy のパラメタ名として使えるよう英数字に置換する。

    col 名は scrape DataFrame 由来で日本語が混じるため、安全のため idx ベースの名前を生成する。
    """
    # idx を含めることで列名重複時の衝突を防ぐ
    return f"p{idx}"
