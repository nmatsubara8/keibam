"""Phase 2: featured_data の Parquet 永続化と SQLite メタ情報管理。

設計方針:
- Parquet (pyarrow) で dtype を完全保持（Categorical / Int64 / Float64 等）。
- SQLite の `featured_data_meta` テーブルに統計情報（行数・列数・日付範囲・作成日時）を記録し、
  ダッシュボードやヘルスチェックで pickle/parquet ファイルが消えていても状態把握できるようにする。
- pickle はモデル学習側の既存コードが読んでいるため引き続きプライマリとして維持する。
  Parquet はセカンダリバックアップ（pickle が消えた場合は parquet から復元可能）。

レイヤ: storage（constants の上位、preprocessing より下位）
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
from typing import Optional

import pandas as pd

from src.storage._db import FEATURED_META_TABLE
from src.storage._db import get_engine

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Parquet 保存 / 読込
# ---------------------------------------------------------------------------


def save_parquet(df: pd.DataFrame, path: str) -> None:
    """featured_data を Parquet 形式で保存する（dtype を完全保持）。

    Categorical 列は pyarrow が int64 に変換してしまうため、
    Parquet ファイルのカスタムメタデータに dtype スキーマ JSON を埋め込む。
    `load_parquet` が読み込み後にスキーマを使って dtype を復元する。

    pyarrow が未インストールの場合は警告のみ吐いて続行する（pickle が主）。
    """
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        logger.warning("save_parquet: pyarrow が未インストールのためスキップします (%s)", path)
        return

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    # dtype スキーマを JSON でメタデータに埋め込む
    dtype_schema = {col: str(dtype) for col, dtype in df.dtypes.items()}
    schema_json = json.dumps(dtype_schema, ensure_ascii=False).encode()

    table = pa.Table.from_pandas(df, preserve_index=True)
    # 既存の pandas メタデータに keibam_dtype_schema キーを追加する
    existing_meta = table.schema.metadata or {}
    new_meta = {**existing_meta, b"keibam_dtype_schema": schema_json}
    table = table.replace_schema_metadata(new_meta)

    pq.write_table(table, path, compression="snappy")
    size_mb = os.path.getsize(path) / 1024 ** 2
    logger.info("save_parquet: %s (%.1f MB)", path, size_mb)


def load_parquet(path: str) -> pd.DataFrame:
    """Parquet から featured_data を読み込む（dtype を復元）。

    Parquet メタデータの keibam_dtype_schema を読んで Categorical 等の dtype を復元する。
    ファイルが存在しない場合は空 DataFrame を返す。
    """
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        import pyarrow.parquet as pq

        table = pq.read_table(path)
        df = table.to_pandas()

        # dtype スキーマをメタデータから復元
        meta = table.schema.metadata or {}
        schema_bytes = meta.get(b"keibam_dtype_schema")
        if schema_bytes:
            dtype_schema: dict[str, str] = json.loads(schema_bytes.decode())
            df = _restore_dtypes(df, dtype_schema)

        logger.info("load_parquet: %s shape=%s", path, df.shape)
        return df
    except Exception as e:  # noqa: BLE001
        logger.error("load_parquet: 読込失敗 %s: %s", path, e)
        return pd.DataFrame()


def _restore_dtypes(df: pd.DataFrame, dtype_schema: dict[str, str]) -> pd.DataFrame:
    """dtype_schema に従って DataFrame の列 dtype を復元する。

    Categorical については `astype("category")` で変換する。元の category 順序は
    再構築できないが、学習コードが `str(dtype) == "category"` で判定するだけなので問題ない。
    """
    for col, dtype_str in dtype_schema.items():
        if col not in df.columns:
            continue
        current = str(df[col].dtype)
        if current == dtype_str:
            continue
        try:
            if dtype_str.startswith("category"):
                df[col] = df[col].astype("category")
            elif dtype_str in ("Int64", "Int32", "Int16", "Int8"):
                df[col] = df[col].astype(dtype_str)
            elif dtype_str in ("Float64", "Float32"):
                df[col] = df[col].astype(dtype_str)
            # その他（bool, datetime 等）は pyarrow が正しく復元するため skip
        except Exception as e:  # noqa: BLE001
            logger.debug("_restore_dtypes: %s %s->%s 失敗 (%s)", col, current, dtype_str, e)
    return df


# ---------------------------------------------------------------------------
# DB メタ情報
# ---------------------------------------------------------------------------


def save_featured_meta(
    df: pd.DataFrame,
    parquet_path: Optional[str] = None,
    db_path: Optional[str] = None,
) -> None:
    """featured_data の統計情報を SQLite の featured_data_meta テーブルに INSERT する。

    Parameters
    ----------
    df : featured_data DataFrame（index = race_id）
    parquet_path : 保存した Parquet ファイルパス（なければ None）
    db_path : SQLite ファイルパス（None なら LocalPaths.DB_PATH）
    """
    engine = get_engine(db_path)

    n_rows = len(df)
    n_cols = len(df.columns)

    # race_id の最小・最大（index が race_id 前提）
    try:
        min_rid = str(df.index.min())
        max_rid = str(df.index.max())
    except Exception:
        min_rid = max_rid = None

    # 列名と dtype の JSON スナップショット（スキーマ追跡用）
    try:
        schema = {col: str(dtype) for col, dtype in df.dtypes.items()}
        schema_json = json.dumps(schema, ensure_ascii=False)
    except Exception:
        schema_json = None

    from sqlalchemy import text

    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO "{FEATURED_META_TABLE}"
                    (created_at, n_rows, n_cols, min_race_id, max_race_id, parquet_path, schema_json)
                VALUES
                    (:created_at, :n_rows, :n_cols, :min_race_id, :max_race_id, :parquet_path, :schema_json)
            """),
            {
                "created_at": dt.datetime.now().isoformat(),
                "n_rows": n_rows,
                "n_cols": n_cols,
                "min_race_id": min_rid,
                "max_race_id": max_rid,
                "parquet_path": parquet_path,
                "schema_json": schema_json,
            },
        )

    logger.info(
        "save_featured_meta: n_rows=%d n_cols=%d min_race_id=%s max_race_id=%s",
        n_rows, n_cols, min_rid, max_rid,
    )


def load_featured_meta(db_path: Optional[str] = None) -> Optional[dict]:
    """最新の featured_data メタ情報を dict で返す（レコードがなければ None）。

    Returns
    -------
    dict | None : {created_at, n_rows, n_cols, min_race_id, max_race_id, parquet_path}
    """
    engine = get_engine(db_path)

    from sqlalchemy import text

    with engine.connect() as conn:
        row = conn.execute(
            text(f'SELECT * FROM "{FEATURED_META_TABLE}" ORDER BY id DESC LIMIT 1')
        ).fetchone()

    if row is None:
        return None

    keys = ("id", "created_at", "n_rows", "n_cols", "min_race_id", "max_race_id", "parquet_path", "schema_json")
    return dict(zip(keys, row, strict=False))
