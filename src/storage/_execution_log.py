"""cron/CLI ジョブの実行記録（execution_log）リポジトリ（Phase 3）。

`run_pipeline` の各サブコマンド（ingest/retrain/doctor/evaluate-odds-dynamics）の
開始/終了を記録し、ダッシュボードで「いつ・成否・所要時間」を可視化する。

設計は `_featured.py`（save_featured_meta/load_featured_meta）と同型。テーブルは
`_db.py::_create_execution_log_table` が `get_engine` 呼出時に作成する。
"""

from __future__ import annotations

import logging
from typing import Optional

from src.storage._db import EXECUTION_LOG_TABLE
from src.storage._db import get_engine

logger = logging.getLogger(__name__)


def record_execution(
    job: str,
    status: str,
    *,
    started_at: Optional[str] = None,
    finished_at: Optional[str] = None,
    duration_sec: Optional[float] = None,
    message: str = "",
    db_path: Optional[str] = None,
) -> None:
    """ジョブ 1 回の実行記録を INSERT する（失敗は warning で握り潰す）。"""
    from sqlalchemy import text

    try:
        engine = get_engine(db_path)
        with engine.begin() as conn:
            conn.execute(
                text(f"""
                    INSERT INTO "{EXECUTION_LOG_TABLE}"
                        (job, status, started_at, finished_at, duration_sec, message)
                    VALUES (:job, :status, :started_at, :finished_at, :duration_sec, :message)
                """),
                {
                    "job": job,
                    "status": status,
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "duration_sec": duration_sec,
                    "message": message[:2000] if message else "",
                },
            )
        logger.info("[execution_log] %s: %s (%.1fs)", job, status, duration_sec or 0.0)
    except Exception as e:  # noqa: BLE001
        logger.warning("[execution_log] 記録失敗 (non-fatal): %s", e)


def load_executions(limit: int = 50, db_path: Optional[str] = None) -> list[dict]:
    """直近の実行記録を新しい順（id 降順）で返す。"""
    from sqlalchemy import text

    try:
        engine = get_engine(db_path)
        with engine.connect() as conn:
            rows = conn.execute(
                text(f'SELECT * FROM "{EXECUTION_LOG_TABLE}" ORDER BY id DESC LIMIT :lim'),
                {"lim": int(limit)},
            ).fetchall()
    except Exception as e:  # noqa: BLE001
        logger.warning("[execution_log] 読込失敗: %s", e)
        return []

    keys = ("id", "job", "status", "started_at", "finished_at", "duration_sec", "message")
    return [dict(zip(keys, r, strict=False)) for r in rows]
