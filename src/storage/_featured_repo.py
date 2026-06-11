"""前処理済み特徴量データ（featured_data）のスナップショット永続化リポジトリ（Phase 2）。

raw データ（`RawDataRepo`）は列ごと TEXT 保存だったが、featured_data は

- 数百列の数値 / category / bool / date が混在し、学習が dtype をそのまま消費する
- 特徴量定義の進化で列がドリフトする
- raw から常に再生成できるため「監査ログ的な永続化」よりも「再計算を省く高速キャッシュ」
  かつ「バージョン管理されたスナップショット」としての価値が大きい

という性質を持つ。そこで featured_data は **1 スナップショット = 1 行の BLOB**
（parquet バイト列、pyarrow 不在時は pickle にフォールバック）として保存し、
dtype を完全保持する。併せて `featured_meta` 表にダッシュボード統計用のメタ情報
（行数 / レース数 / 特徴量数 / 期間 / 作成日時）を保存する。

設計メモ:
- スキーマは `RawDataRepo` の TABLE_SPECS とは独立に、本リポジトリ内で
  `CREATE TABLE IF NOT EXISTS` する（列ごと保存モデルに乗らないため）。
- `version` は呼出側が任意指定でき、省略時は `YYYYmmdd_HHMMSS` を自動採番する。
- 同一 version で再 save した場合は上書き（`INSERT OR REPLACE`）する。
  raw の冪等 IGNORE と異なり、featured は「最新の再計算結果で更新したい」ため。
- `load()` / `get_meta()` は version 省略で最新（created_at 降順の先頭）を返す。
"""

from __future__ import annotations

import datetime as dt
import io
import json
import logging
import pickle
from typing import Optional

import pandas as pd
from sqlalchemy import text

from src.storage._db import get_engine

logger = logging.getLogger(__name__)


_SNAPSHOTS_TABLE = "featured_snapshots"
_META_TABLE = "featured_meta"

# parquet を優先し、pyarrow が無い / シリアライズ不能な場合は pickle に落とす。
_FORMAT_PARQUET = "parquet"
_FORMAT_PICKLE = "pickle"


class FeaturedDataRepo:
    """featured_data スナップショットの SQLite 永続化リポジトリ。

    `db_path=None` の場合は `LocalPaths.DB_PATH`（raw と同一 DB ファイル）を使う。
    テスト時は `db_path=tmp_path / "test.db"` で差し替える。
    """

    def __init__(self, db_path: Optional[str] = None) -> None:
        self._engine = get_engine(db_path)
        self._ensure_tables()

    # ------------------------------------------------------------------
    # スキーマ
    # ------------------------------------------------------------------

    def _ensure_tables(self) -> None:
        """スナップショット表とメタ表を作成する（冪等）。"""
        with self._engine.begin() as conn:
            conn.execute(
                text(
                    f'CREATE TABLE IF NOT EXISTS "{_SNAPSHOTS_TABLE}" (\n'
                    '  "version" TEXT PRIMARY KEY,\n'
                    '  "format" TEXT NOT NULL,\n'
                    '  "data" BLOB NOT NULL,\n'
                    '  "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n'
                    ")"
                )
            )
            conn.execute(
                text(
                    f'CREATE TABLE IF NOT EXISTS "{_META_TABLE}" (\n'
                    '  "version" TEXT PRIMARY KEY,\n'
                    '  "n_rows" INTEGER,\n'
                    '  "n_races" INTEGER,\n'
                    '  "n_features" INTEGER,\n'
                    '  "date_min" TEXT,\n'
                    '  "date_max" TEXT,\n'
                    '  "columns_json" TEXT,\n'
                    '  "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n'
                    ")"
                )
            )

    # ------------------------------------------------------------------
    # save
    # ------------------------------------------------------------------

    def save(self, df: pd.DataFrame, version: Optional[str] = None) -> str:
        """featured_data を 1 スナップショットとして保存し、version を返す。

        Parameters
        ----------
        df : 保存する featured_data（race_id インデックスを想定）。
        version : スナップショット識別子。省略時は `YYYYmmdd_HHMMSS` を自動採番。

        Returns
        -------
        str : 実際に保存した version。
        """
        if df is None:
            raise ValueError("save: df が None です")
        if version is None:
            version = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

        blob, fmt = _serialize(df)
        created_at = dt.datetime.now().isoformat(timespec="seconds")
        meta = _extract_meta(df)

        with self._engine.begin() as conn:
            conn.execute(
                text(
                    f'INSERT OR REPLACE INTO "{_SNAPSHOTS_TABLE}" '
                    '("version", "format", "data", "created_at") '
                    "VALUES (:version, :format, :data, :created_at)"
                ),
                {"version": version, "format": fmt, "data": blob, "created_at": created_at},
            )
            conn.execute(
                text(
                    f'INSERT OR REPLACE INTO "{_META_TABLE}" '
                    '("version", "n_rows", "n_races", "n_features", '
                    '"date_min", "date_max", "columns_json", "created_at") '
                    "VALUES (:version, :n_rows, :n_races, :n_features, "
                    ":date_min, :date_max, :columns_json, :created_at)"
                ),
                {"version": version, "created_at": created_at, **meta},
            )

        logger.info(
            "[FeaturedDataRepo] save version=%s format=%s rows=%s races=%s features=%s",
            version, fmt, meta["n_rows"], meta["n_races"], meta["n_features"],
        )
        return version

    # ------------------------------------------------------------------
    # load
    # ------------------------------------------------------------------

    def load(self, version: Optional[str] = None) -> Optional[pd.DataFrame]:
        """スナップショットを復元する。version 省略時は最新を返す。存在しなければ None。"""
        if version is None:
            version = self.latest_version()
            if version is None:
                return None

        with self._engine.connect() as conn:
            row = conn.execute(
                text(
                    f'SELECT "format", "data" FROM "{_SNAPSHOTS_TABLE}" WHERE "version" = :v'
                ),
                {"v": version},
            ).fetchone()

        if row is None:
            return None
        fmt, blob = row[0], row[1]
        return _deserialize(blob, fmt)

    def latest_version(self) -> Optional[str]:
        """最新スナップショットの version（created_at 降順の先頭）。無ければ None。"""
        with self._engine.connect() as conn:
            row = conn.execute(
                text(
                    f'SELECT "version" FROM "{_META_TABLE}" '
                    'ORDER BY "created_at" DESC, "version" DESC LIMIT 1'
                )
            ).fetchone()
        return row[0] if row is not None else None

    # ------------------------------------------------------------------
    # メタ / ダッシュボード統計
    # ------------------------------------------------------------------

    def get_meta(self, version: Optional[str] = None) -> Optional[dict]:
        """指定 version（省略時は最新）のメタ情報を dict で返す。無ければ None。"""
        if version is None:
            version = self.latest_version()
            if version is None:
                return None
        with self._engine.connect() as conn:
            row = conn.execute(
                text(
                    f'SELECT "version", "n_rows", "n_races", "n_features", '
                    f'"date_min", "date_max", "columns_json", "created_at" '
                    f'FROM "{_META_TABLE}" WHERE "version" = :v'
                ),
                {"v": version},
            ).fetchone()
        return _row_to_meta(row) if row is not None else None

    def list_meta(self) -> list[dict]:
        """全スナップショットのメタ情報を created_at 降順で返す（ダッシュボード統計用）。"""
        with self._engine.connect() as conn:
            rows = conn.execute(
                text(
                    f'SELECT "version", "n_rows", "n_races", "n_features", '
                    f'"date_min", "date_max", "columns_json", "created_at" '
                    f'FROM "{_META_TABLE}" ORDER BY "created_at" DESC, "version" DESC'
                )
            ).fetchall()
        return [_row_to_meta(r) for r in rows]

    # ------------------------------------------------------------------
    # delete
    # ------------------------------------------------------------------

    def delete(self, version: str) -> int:
        """指定 version のスナップショットとメタを削除する。削除した行数（0/1）を返す。"""
        with self._engine.begin() as conn:
            result = conn.execute(
                text(f'DELETE FROM "{_SNAPSHOTS_TABLE}" WHERE "version" = :v'),
                {"v": version},
            )
            conn.execute(
                text(f'DELETE FROM "{_META_TABLE}" WHERE "version" = :v'),
                {"v": version},
            )
            deleted = result.rowcount if result.rowcount and result.rowcount > 0 else 0
        logger.info("[FeaturedDataRepo] delete version=%s deleted=%d", version, deleted)
        return deleted


# ---------------------------------------------------------------------------
# シリアライズ / メタ抽出（純粋関数）
# ---------------------------------------------------------------------------


def _serialize(df: pd.DataFrame) -> tuple[bytes, str]:
    """DataFrame をバイト列に変換する。parquet を優先し、不能なら pickle。"""
    try:
        buf = io.BytesIO()
        df.to_parquet(buf, engine="pyarrow", index=True)
        return buf.getvalue(), _FORMAT_PARQUET
    except Exception as e:  # noqa: BLE001 - pyarrow 不在 / 非対応 dtype / 非文字列列名 等
        logger.warning("[FeaturedDataRepo] parquet serialize 失敗、pickle にフォールバック: %s", e)
        return pickle.dumps(df, protocol=pickle.HIGHEST_PROTOCOL), _FORMAT_PICKLE


def _deserialize(blob: bytes, fmt: str) -> pd.DataFrame:
    """バイト列を DataFrame に復元する。"""
    if fmt == _FORMAT_PARQUET:
        return pd.read_parquet(io.BytesIO(blob), engine="pyarrow")
    if fmt == _FORMAT_PICKLE:
        return pickle.loads(blob)  # noqa: S301 - 自プロセスで保存した信頼できる BLOB のみ
    raise ValueError(f"unknown format: {fmt}")


def _extract_meta(df: pd.DataFrame) -> dict:
    """ダッシュボード統計用メタ情報を抽出する。

    n_races: index 名が race_id（または index level 0）のユニーク数。
    date_min/max: "date" 列があれば文字列で記録（無ければ None）。
    """
    n_rows = int(len(df))
    n_features = int(df.shape[1])
    try:
        n_races = int(df.index.get_level_values(0).nunique())
    except Exception:  # noqa: BLE001
        n_races = int(df.index.nunique())

    date_min: Optional[str] = None
    date_max: Optional[str] = None
    if "date" in df.columns and len(df) > 0:
        try:
            dates = pd.to_datetime(df["date"], errors="coerce").dropna()
            if len(dates) > 0:
                date_min = str(dates.min().date())
                date_max = str(dates.max().date())
        except Exception:  # noqa: BLE001
            pass

    columns_json = json.dumps([str(c) for c in df.columns], ensure_ascii=False)
    return {
        "n_rows": n_rows,
        "n_races": n_races,
        "n_features": n_features,
        "date_min": date_min,
        "date_max": date_max,
        "columns_json": columns_json,
    }


def _row_to_meta(row) -> dict:  # noqa: ANN001 - SQLAlchemy Row
    """SELECT 行をメタ dict に変換する（columns_json はリストに復元）。"""
    columns = []
    if row[6]:
        try:
            columns = json.loads(row[6])
        except (json.JSONDecodeError, TypeError):
            columns = []
    return {
        "version": row[0],
        "n_rows": row[1],
        "n_races": row[2],
        "n_features": row[3],
        "date_min": row[4],
        "date_max": row[5],
        "columns": columns,
        "created_at": row[7],
    }
