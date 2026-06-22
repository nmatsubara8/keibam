"""SQLite データベースのエンジン取得・スキーマ定義・テーブル仕様レジストリ。

Phase 1 の責務:
- `get_engine()` で `LocalPaths.DB_PATH` を指す SQLAlchemy Engine をシングルトン提供する。
- 初回呼出で WAL モードに切り替え、必要なテーブルを `CREATE TABLE IF NOT EXISTS` で生成する。
- `TABLE_SPECS` レジストリで「alias → テーブル名 / 主キー / index 名」を一元管理し、
  `RawDataRepo` が DataFrame と DB 行の橋渡しを行う際の SSOT（Single Source of Truth）にする。
- pickle 蓄積層（`data/raw/*.pkl`）のファイルパス ↔ alias の逆引き辞書 `PICKLE_PATH_TO_ALIAS`
  も同梱し、`update_rawdata(filepath, df)` 側で alias 解決を行えるようにする。

設計メモ:
- 既存 Processor は pickle 経由を継続するため、DB はあくまで「pickle 揮発時の保険」。
- 主キー衝突時は `INSERT OR IGNORE` で旧行を保持する（誤情報修正は --force で DELETE して再投入）。
- `index_col` が指定されたテーブルは、`upsert` 時に DataFrame の index も DB 列として保存し、
  `read` で復元する。これにより pickle ラウンドトリップで形が崩れない。
"""

from __future__ import annotations

import dataclasses
import threading
from typing import Optional

from sqlalchemy import Engine
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy import text

from src.constants._local_paths import LocalPaths


# ---------------------------------------------------------------------------
# テーブル仕様レジストリ
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class TableSpec:
    """1 つのテーブルに対応する仕様。

    Attributes
    ----------
    table_name : 物理テーブル名（DB 内）
    primary_key : 主キーを構成する列名のタプル（順序保持）
    index_col : 入力 DataFrame の index が表す DB 列名（index を持たないなら None）。
        upsert で DataFrame を行に展開する際、index をこの名前の列として書き出す。
    auto_row_idx_col : True なら、入力 DataFrame の「同一 index 内の出現順」を
        `row_idx` 列として自動付与する（払戻テーブルのように 1 レース内複数行があり、
        index だけでは PK にならないケースで使う）。
    """

    table_name: str
    primary_key: tuple[str, ...]
    index_col: Optional[str] = None
    auto_row_idx_col: bool = False


# alias → TableSpec
# alias は既存コード（_get_rawdata の UrlPaths）とは独立に Phase 1 で命名し直したもの。
# pickle 蓄積層のパス（LocalPaths.RAW_*_PATH）と 1:1 で対応する。
TABLE_SPECS: dict[str, TableSpec] = {
    "raw_results": TableSpec(
        table_name="raw_results",
        primary_key=("race_id", "馬番"),
        index_col="race_id",
    ),
    "raw_race_info": TableSpec(
        table_name="raw_race_info",
        primary_key=("race_id",),
        index_col="race_id",
    ),
    "raw_return_tables": TableSpec(
        table_name="raw_return_tables",
        # 払戻 raw は (race_id, row_idx) で一意。row_idx は upsert 時に自動付与する。
        primary_key=("race_id", "row_idx"),
        index_col="race_id",
        auto_row_idx_col=True,
    ),
    "raw_horse_results": TableSpec(
        table_name="raw_horse_results",
        # 実 DataFrame: index は名前なし RangeIndex、horse_id と 日付 は通常列。
        # 1頭が同日に複数出走することはないため (horse_id, 日付) で一意。
        primary_key=("horse_id", "日付"),
        index_col=None,
    ),
    "raw_horse_info": TableSpec(
        table_name="raw_horse_info",
        # 実 DataFrame: index は名前なし RangeIndex、horse_id は通常列。
        primary_key=("horse_id",),
        index_col=None,
    ),
    "raw_peds": TableSpec(
        table_name="raw_peds",
        # 実 DataFrame: index は名前なし RangeIndex、horse_id は通常列。
        primary_key=("horse_id",),
        index_col=None,
    ),
    "raw_odds_snapshots": TableSpec(
        table_name="raw_odds_snapshots",
        # OddsSnapshot dataclass の (race_id, captured_at, bet_type, combo) で一意。
        # combo は "3-7-11" のような文字列、captured_at は ISO8601 文字列で保存する
        # （odds_scheduler.persist → snapshots_to_records が変換して upsert する）。
        primary_key=("race_id", "captured_at", "bet_type", "combo"),
        index_col=None,
    ),
    "raw_odds_predictions": TableSpec(
        table_name="raw_odds_predictions",
        # オッズ力学モデルのチェックポイント別予測（odds_watch が upsert する）。
        # 同一チェックポイントの再計算は checkpoint キーが同じため IGNORE され冪等。
        primary_key=("race_id", "checkpoint", "model", "umaban"),
        index_col=None,
    ),
    # レース当日ノート（調教評価/パドック/厩舎コメント）。いずれも (race_id, 馬番) で一意、
    # results と同じく race_id を index に持たせる。再取得は当該 race_id の行を総入替する。
    "raw_training": TableSpec(
        table_name="raw_training",
        primary_key=("race_id", "馬番"),
        index_col="race_id",
    ),
    "raw_paddock": TableSpec(
        table_name="raw_paddock",
        primary_key=("race_id", "馬番"),
        index_col="race_id",
    ),
    "raw_comment": TableSpec(
        table_name="raw_comment",
        primary_key=("race_id", "馬番"),
        index_col="race_id",
    ),
    # 予想印（レース×馬×予想家のロング）。(race_id,馬番,predictor_yid) で一意。
    # race_id を index に持たせ、再取得は当該 race_id の行を総入替する。
    "raw_yoso_marks": TableSpec(
        table_name="raw_yoso_marks",
        primary_key=("race_id", "馬番", "predictor_yid"),
        index_col="race_id",
    ),
}


# pickle ファイルパス → alias の逆引き辞書。
# `update_rawdata(filepath, df)` から alias を解決するために使う。
PICKLE_PATH_TO_ALIAS: dict[str, str] = {
    LocalPaths.RAW_RESULTS_PATH: "raw_results",
    LocalPaths.RAW_RACE_INFO_PATH: "raw_race_info",
    LocalPaths.RAW_RETURN_TABLES_PATH: "raw_return_tables",
    LocalPaths.RAW_HORSE_RESULTS_PATH: "raw_horse_results",
    LocalPaths.RAW_HORSE_INFO_PATH: "raw_horse_info",
    LocalPaths.RAW_PEDS_PATH: "raw_peds",
    LocalPaths.RAW_ODDS_SNAPSHOT_PATH: "raw_odds_snapshots",
    LocalPaths.RAW_ODDS_PREDICTIONS_PATH: "raw_odds_predictions",
    LocalPaths.RAW_TRAINING_PATH: "raw_training",
    LocalPaths.RAW_PADDOCK_PATH: "raw_paddock",
    LocalPaths.RAW_COMMENT_PATH: "raw_comment",
    LocalPaths.RAW_YOSO_MARKS_PATH: "raw_yoso_marks",
}


def alias_to_pickle_path(alias: str) -> Optional[str]:
    """alias から pickle パスを正引きする（auto_migrate 用）。"""
    for path, a in PICKLE_PATH_TO_ALIAS.items():
        if a == alias:
            return path
    return None


# ---------------------------------------------------------------------------
# Phase 2: featured_data メタ情報テーブル名（固定スキーマ、TABLE_SPECS 外）
# ---------------------------------------------------------------------------

FEATURED_META_TABLE = "featured_data_meta"

# Phase 3: cron/CLI ジョブの実行記録テーブル名（固定スキーマ、TABLE_SPECS 外）。
EXECUTION_LOG_TABLE = "execution_log"


# ---------------------------------------------------------------------------
# エンジン管理（シングルトン）
# ---------------------------------------------------------------------------


# 単一プロセス内では同じ SQLite ファイルに対して 1 つの Engine を共有する。
# DB_PATH を上書きしたい場合（テスト）は `_reset_engine_for_testing` を使う。
_engine_lock = threading.Lock()
_engine: Optional[Engine] = None
_engine_path: Optional[str] = None


def get_engine(db_path: Optional[str] = None) -> Engine:
    """SQLite Engine を取得する（シングルトン）。

    Parameters
    ----------
    db_path : 明示指定があればその SQLite ファイルパス、None なら LocalPaths.DB_PATH。
        既に作成済みの Engine と異なるパスを渡した場合は、新しいパス用に作り直す。
    """
    global _engine, _engine_path

    target_path = db_path if db_path is not None else LocalPaths.DB_PATH

    with _engine_lock:
        if _engine is not None and _engine_path == target_path:
            return _engine

        # 親ディレクトリを作成（data/ が無い CI 等での初回起動を想定）
        import os
        os.makedirs(os.path.dirname(target_path) or ".", exist_ok=True)

        # check_same_thread=False で複数スレッドからの読みを許可（pytest 並列実行向け）
        url = f"sqlite:///{target_path}"
        engine = create_engine(url, future=True, connect_args={"check_same_thread": False})

        # WAL モードを永続的に有効化（同時読み書き安定性向上）
        @event.listens_for(engine, "connect")
        def _set_sqlite_pragma(dbapi_connection, _connection_record):  # noqa: ANN001
            # journal_mode は接続毎ではなくファイル毎の設定だが、念のため毎接続で確認する
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

        # スキーマ作成（冪等）
        _create_tables(engine)

        _engine = engine
        _engine_path = target_path
        return engine


def _reset_engine_for_testing() -> None:
    """テスト用: シングルトンをクリアして次回 `get_engine` で再作成させる。"""
    global _engine, _engine_path
    with _engine_lock:
        if _engine is not None:
            _engine.dispose()
        _engine = None
        _engine_path = None


# ---------------------------------------------------------------------------
# スキーマ作成
# ---------------------------------------------------------------------------


# 各テーブルの列は scrape DataFrame の列数が可変（_logged 系の自由列）であるため、
# 固定列（主キー + index_col + auto row_idx + ingested_at）のみを宣言し、
# 残りの自由列は upsert 時に `ALTER TABLE ADD COLUMN` で動的追加する。
# こうすることで「Phase 1 は raw のまま全列を持つ」要件と SQLite の現実的制約を両立する。


def _create_featured_meta_table(engine: Engine) -> None:
    """Phase 2: featured_data_meta テーブルを作成する（固定スキーマ）。"""
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS "{FEATURED_META_TABLE}" (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                n_rows      INTEGER NOT NULL,
                n_cols      INTEGER NOT NULL,
                min_race_id TEXT,
                max_race_id TEXT,
                parquet_path TEXT,
                schema_json TEXT
            )
        """))


def _create_execution_log_table(engine: Engine) -> None:
    """Phase 3: execution_log テーブルを作成する（固定スキーマ）。"""
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS "{EXECUTION_LOG_TABLE}" (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                job          TEXT NOT NULL,
                status       TEXT NOT NULL,
                started_at   TIMESTAMP,
                finished_at  TIMESTAMP,
                duration_sec REAL,
                message      TEXT
            )
        """))


def _create_tables(engine: Engine) -> None:
    """全テーブルを作成する。既存テーブルの PK 列が現在の仕様と合わない場合は DROP して再作成する。

    TableSpec の primary_key が変わった場合（スキーマ修正時）に古い DB を持ち込んでも
    サイレントに正しいスキーマへ移行できるよう、PK 列の存在チェックを行う。
    テーブルを DROP するとデータも消えるが、pickle から再 migrate すれば復元できる。
    """
    with engine.begin() as conn:
        for spec in TABLE_SPECS.values():
            # 既存テーブルの列を調べ、PK 列が揃っているか確認する
            rows = conn.execute(text(f'PRAGMA table_info("{spec.table_name}")')).fetchall()
            if rows:
                existing_cols = {row[1] for row in rows}
                missing_pk = [c for c in spec.primary_key if c not in existing_cols]
                if missing_pk:
                    # 古いスキーマ（PK 列不足）はデータを捨てて再作成する
                    # pickle から auto_migrate_if_empty で再投入できる
                    import logging
                    logging.getLogger(__name__).warning(
                        "_create_tables: %s は PK 列 %s が不足しているため DROP して再作成します",
                        spec.table_name, missing_pk,
                    )
                    conn.execute(text(f'DROP TABLE "{spec.table_name}"'))

            cols: list[str] = []
            # 主キー列は TEXT（race_id / horse_id / 日付 / combo 等は全て文字列で扱える）
            for pk_col in spec.primary_key:
                cols.append(f'"{pk_col}" TEXT NOT NULL')
            # index_col が PK に含まれていなければ追加（基本は PK と重なる想定）
            if spec.index_col is not None and spec.index_col not in spec.primary_key:
                cols.append(f'"{spec.index_col}" TEXT')
            # 取込日時
            cols.append('"ingested_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
            pk_def = ", ".join(f'"{c}"' for c in spec.primary_key)
            pk_sql = f"PRIMARY KEY ({pk_def})"
            cols_sql = ",\n  ".join(cols)
            sql = f'CREATE TABLE IF NOT EXISTS "{spec.table_name}" (\n  {cols_sql},\n  {pk_sql}\n)'
            conn.execute(text(sql))

    # Phase 2: featured_data_meta テーブル（固定スキーマ、別関数に委譲）
    _create_featured_meta_table(engine)
    _create_execution_log_table(engine)


def ensure_columns(engine: Engine, table_name: str, columns: list[str]) -> None:
    """既存テーブルに不足列があれば `ALTER TABLE ADD COLUMN` で追加する。

    SQLite の ALTER TABLE は ADD COLUMN のみ可能（DROP 不可）だが
    Phase 1 では追加だけで十分。型は全て TEXT で保存する（pandas 側で復元時に dtype 推論）。
    """
    with engine.begin() as conn:
        # 現存する列を取得
        rows = conn.execute(text(f'PRAGMA table_info("{table_name}")')).fetchall()
        existing = {row[1] for row in rows}
        for col in columns:
            if col not in existing:
                # 列名に二重引用符を含むケースは scrape DataFrame では発生しない想定
                conn.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" TEXT'))
