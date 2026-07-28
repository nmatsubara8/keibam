"""JRDB 固定長データの SQLite 永続化ストア（冪等・重複防止つき）。

netkeiba の `RawDataRepo` と同じ `data/keibam.db` を共用しつつ、JRDB 専用テーブルと
「処理済みファイル台帳」を持つ。ユーザ要件の 2 軸の重複防止を提供する:

① 既存データ内の重複排除
   `raw_jrdb_<type>` テーブルの主キー **(race_id, umaban)** + **INSERT OR REPLACE**（keep-last）。
   同一出走の行は常に 1 行へ集約され、後から来た訂正/確定版が勝つ（JRDB は速報→確定で
   同一キーを上書き配布するため keep-last が正しい。netkeiba raw の INSERT OR IGNORE=keep-first
   とは意図的に異なる）。同一ファイル内に重複キーがあっても upsert 前に keep-last で潰す。

② 新規ロード時の重複チェック
   `jrdb_ingested_files` 台帳（**sha1 主キー**）。同一内容（sha1 一致）のファイルは二度と
   パースしない。内容が変わった（訂正配布 = sha1 変化）ファイルだけ再ロードして keep-last
   upsert する。`extract_dir` の「同名 .txt はスキップ」冪等性と二段で効く。

netkeiba(raw_results 等) とは別テーブルなので、同種データ（SED 成績・確定オッズ等）でも
衝突しない。突合したい場合は `read('SED')` 等で取り出し、source を明示して扱う。

現状の対応形式は KYI/SED/SKB/TYB（すべて 1 出走 = (race_id, 馬番) 単位）。レース単位の
形式（HJC 払戻・SRB・BAC 等）を足すときは `_PK_BY_TYPE` にキーを追加する。
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import text

from src.storage._db import ensure_columns
from src.storage._db import get_engine

logger = logging.getLogger(__name__)

# 対応形式 → 物理テーブル名。全て (race_id, umaban) を主キーとする horse-in-race 記録。
RECORD_TYPES: tuple[str, ...] = ("KYI", "SED", "SKB", "TYB")
_TABLE = {rt: f"raw_jrdb_{rt.lower()}" for rt in RECORD_TYPES}
# 形式別の主キー。将来レース単位形式を足すときはここに ("race_id",) 等を追加する。
_PK_BY_TYPE: dict[str, tuple[str, ...]] = {rt: ("race_id", "umaban") for rt in RECORD_TYPES}

LEDGER_TABLE = "jrdb_ingested_files"


def _to_db_str(v) -> Optional[str]:
    """DB 保存用の正準文字列化。None/NA はそのまま None、整数値 float は int 表記に揃える。

    race_id が int 由来（"201502020201"）と float 由来（"...0"）で別文字列になると PK 重複
    判定が壊れるため、整数値 float は int 表記へ正準化する（netkeiba 側 `_to_db_str` と同じ）。
    """
    if v is None:
        return None
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v)


def file_sha1(path: str) -> str:
    """ファイル内容の SHA-1（台帳の内容キー）。訂正配布は sha1 が変わる。"""
    h = hashlib.sha1()  # noqa: S324 — セキュリティ用途ではなく内容一致判定のみ
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


class JrdbStore:
    """JRDB raw データの冪等永続化ストア（`data/keibam.db` を共用）。

    `db_path=None` で `LocalPaths.DB_PATH`。テスト時は `db_path=str(tmp_path/"t.db")`。
    """

    def __init__(self, db_path: Optional[str] = None) -> None:
        self._engine = get_engine(db_path)  # netkeiba テーブルも作られるが無害
        self._create_tables()

    # ------------------------------------------------------------------
    # スキーマ
    # ------------------------------------------------------------------

    def _create_tables(self) -> None:
        """JRDB テーブル群 + 台帳を作成（冪等）。自由列は upsert 時に ALTER で追加。"""
        with self._engine.begin() as conn:
            for rt, table in _TABLE.items():
                pk = _PK_BY_TYPE[rt]
                pk_cols = ",\n  ".join(f'"{c}" TEXT NOT NULL' for c in pk)
                pk_def = ", ".join(f'"{c}"' for c in pk)
                conn.execute(text(
                    f'CREATE TABLE IF NOT EXISTS "{table}" (\n'
                    f'  {pk_cols},\n'
                    f'  "ingested_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n'
                    f'  PRIMARY KEY ({pk_def})\n)'
                ))
            conn.execute(text(
                f'CREATE TABLE IF NOT EXISTS "{LEDGER_TABLE}" (\n'
                '  "sha1"        TEXT PRIMARY KEY,\n'
                '  "filename"    TEXT,\n'
                '  "record_type" TEXT,\n'
                '  "n_rows"      INTEGER,\n'
                '  "n_written"   INTEGER,\n'
                '  "ingested_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n)'
            ))

    # ------------------------------------------------------------------
    # ① 既存データ内の重複排除: keep-last upsert
    # ------------------------------------------------------------------

    def upsert(self, record_type: str, df: pd.DataFrame) -> int:
        """DataFrame を主キー (race_id, umaban) で `INSERT OR REPLACE`（keep-last）投入する。

        - 同一ファイル内で PK が重複する行は upsert 前に keep-last で 1 行へ潰す。
        - PK 列が欠損（race_id/umaban が NA）の行は保存できないため落とす。
        Returns: 書き込んだ行数（REPLACE を含む）。
        """
        rt = record_type.upper()
        if rt not in _TABLE:
            raise ValueError(f"未対応の record_type: {record_type}（対応: {RECORD_TYPES}）")
        if df is None or df.empty:
            return 0
        pk = _PK_BY_TYPE[rt]
        missing = [c for c in pk if c not in df.columns]
        if missing:
            raise ValueError(f"upsert({rt}): 主キー列 {missing} が DataFrame にありません")

        d = df.copy()
        d = d.dropna(subset=list(pk))            # PK 欠損行は保存不可
        d = d.drop_duplicates(subset=list(pk), keep="last")  # ファイル内重複を keep-last
        if d.empty:
            return 0

        d.columns = [str(c) for c in d.columns]
        d = d.where(pd.notna(d), None)
        for col in d.columns:
            d[col] = d[col].map(_to_db_str)

        table = _TABLE[rt]
        free = [c for c in d.columns if c not in pk]
        if free:
            ensure_columns(self._engine, table, free)

        cols = list(d.columns)
        col_sql = ", ".join(f'"{c}"' for c in cols)
        param_sql = ", ".join(f":p{i}" for i in range(len(cols)))
        sql = f'INSERT OR REPLACE INTO "{table}" ({col_sql}) VALUES ({param_sql})'
        rows = [
            {f"p{i}": v for i, v in enumerate(row)}
            for row in d.itertuples(index=False, name=None)
        ]
        with self._engine.begin() as conn:
            conn.execute(text(sql), rows)
        logger.info("[JrdbStore] upsert %s: %d rows (keep-last)", rt, len(rows))
        return len(rows)

    def read(self, record_type: str) -> pd.DataFrame:
        """テーブルを DataFrame で読み出す（内部メタ ingested_at は除外）。"""
        rt = record_type.upper()
        if rt not in _TABLE:
            raise ValueError(f"未対応の record_type: {record_type}")
        with self._engine.connect() as conn:
            df = pd.read_sql(text(f'SELECT * FROM "{_TABLE[rt]}"'), conn)
        if "ingested_at" in df.columns:
            df = df.drop(columns=["ingested_at"])
        return df

    # ------------------------------------------------------------------
    # ② 新規ロード時の重複チェック: 処理済みファイル台帳
    # ------------------------------------------------------------------

    def is_ingested(self, sha1: str) -> bool:
        """この内容（sha1）のファイルが取込済みか。"""
        with self._engine.connect() as conn:
            row = conn.execute(
                text(f'SELECT 1 FROM "{LEDGER_TABLE}" WHERE "sha1" = :s'), {"s": sha1}
            ).fetchone()
        return row is not None

    def _record_file(self, sha1: str, filename: str, record_type: str,
                     n_rows: int, n_written: int) -> None:
        with self._engine.begin() as conn:
            conn.execute(text(
                f'INSERT OR REPLACE INTO "{LEDGER_TABLE}" '
                '("sha1","filename","record_type","n_rows","n_written") '
                'VALUES (:sha1,:fn,:rt,:nr,:nw)'
            ), {"sha1": sha1, "fn": filename, "rt": record_type, "nr": n_rows, "nw": n_written})

    def ingested_files(self) -> pd.DataFrame:
        """台帳の一覧（監査用）。"""
        with self._engine.connect() as conn:
            return pd.read_sql(text(f'SELECT * FROM "{LEDGER_TABLE}"'), conn)

    # ------------------------------------------------------------------
    # オーケストレーション
    # ------------------------------------------------------------------

    def ingest_files(self, files_by_type: dict[str, list[str]], *, force: bool = False,
                     allow_length_mismatch: bool = False) -> dict:
        """種別→パス群を受け、未取込（sha1 未登録）のファイルだけパース→keep-last upsert する。

        force=True で台帳を無視して全て再取込する（訂正の強制反映・再構築用）。
        allow_length_mismatch=False（既定）は、レコード長が仕様と乖離するファイル
        （フォーマット版差の疑い）を**スキップ**して誤取込を防ぐ。True で強制取込。
        Returns: {record_type: {"files": 取込数, "skipped": 台帳一致 skip 数,
                  "badlen": 版差 skip 数, "rows": 書込行数}}。
        """
        from src.jrdb._parser import check_record_length  # noqa: PLC0415
        from src.jrdb._parser import parse  # noqa: PLC0415

        summary: dict[str, dict] = {}
        for rt in RECORD_TYPES:
            paths = files_by_type.get(rt, [])
            s = {"files": 0, "skipped": 0, "badlen": 0, "rows": 0}
            for path in paths:
                sha1 = file_sha1(path)
                if not force and self.is_ingested(sha1):
                    s["skipped"] += 1
                    continue
                ok, dom, exp = check_record_length(path, rt)
                if not ok and not allow_length_mismatch:
                    logger.warning(
                        "[JrdbStore] %s: レコード長 %d≠仕様 %d のためスキップ（フォーマット版差の疑い。"
                        "取り込むなら allow_length_mismatch=True / CLI --allow-length-mismatch）。",
                        Path(path).name, dom, exp,
                    )
                    s["badlen"] += 1
                    continue
                df = parse(path, rt)
                written = self.upsert(rt, df)
                self._record_file(sha1, Path(path).name, rt, len(df), written)
                s["files"] += 1
                s["rows"] += written
            summary[rt] = s
        return summary

    def ingest_dir(self, src_dir: str, extract_to: str = "data/jrdb_txt",
                   *, force: bool = False, allow_length_mismatch: bool = False) -> dict:
        """ディレクトリ内の .txt/.zip/.lzh を展開・分類し、未取込分を冪等取込する。"""
        from src.jrdb._extract import extract_dir  # noqa: PLC0415

        by_type = extract_dir(src_dir, extract_to)
        return self.ingest_files(by_type, force=force, allow_length_mismatch=allow_length_mismatch)
