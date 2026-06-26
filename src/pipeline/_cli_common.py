"""run_pipeline 共通インフラ（DB 優先読込・ジョブ計測/記録/通知）。

各サブコマンドのハンドラが共有する横断ヘルパを集約し、run_pipeline.py の肥大化を抑える。
DB を source of truth とする読込（pickle 揮発の保険）と、ジョブ実行の計測・execution_log
記録・失敗通知を提供する。重い依存（storage/operation）はすべて関数内で遅延 import する。
"""

from __future__ import annotations

import argparse
import logging
import os

logger = logging.getLogger(__name__)


def _auto_migrate_db() -> None:
    """既存 pickle を SQLite へ自動移行する（DB が空のテーブルのみ、non-fatal）。

    pickle 揮発時の保険である DB が空のまま運用されるのを防ぐため、
    ingest / retrain の起動時に毎回呼ぶ（移行済みなら has_rows チェックのみで安価）。
    """
    try:
        from src.storage import RawDataRepo

        migrated = RawDataRepo().auto_migrate_all()
        if migrated:
            logger.info("[pipeline] DB auto-migrate: %s", migrated)
    except Exception as e:  # noqa: BLE001
        logger.warning("[pipeline] DB auto-migrate 失敗 (non-fatal): %s", e)


def _load_raw_db_first(alias: str, pickle_path: str):
    """raw データを DB 優先で読む（DB が source of truth。stale な pickle を回避）。

    DB（RawDataRepo）に行があれば DB を、無ければ pickle を返す。``(df, source)`` を返す。
    DB 復元後に pickle が古いまま（merge バグ等で縮小）でも最新データを使えるようにする。
    """
    import pandas as pd

    from src.pipeline._ingestion import load_raw

    try:
        from src.storage import RawDataRepo

        repo = RawDataRepo()
        if repo.has_rows(alias):
            df = repo.read(alias)
            if df is not None and not df.empty:
                return df, "db"
    except Exception as e:  # noqa: BLE001 — DB 不可時は pickle にフォールバック
        logger.warning("[calibrate-takeout] DB(%s) 読込失敗、pickle にフォールバック: %s", alias, e)
    df = load_raw(pickle_path)
    return (df if df is not None else pd.DataFrame()), "pickle"


def _return_processor_db_first():
    """ReturnProcessor を DB 優先で構築する（DB にあれば一時 pickle 経由で読む）。"""
    import tempfile

    from src.constants._local_paths import LocalPaths
    from src.preprocessing._return_processor import ReturnProcessor

    df, source = _load_raw_db_first("raw_return_tables", LocalPaths.RAW_RETURN_TABLES_PATH)
    if source == "db" and not df.empty:
        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tf:
            tmp = tf.name
        try:
            df.to_pickle(tmp)
            return ReturnProcessor(tmp), "db"
        finally:
            os.unlink(tmp)
    return ReturnProcessor(LocalPaths.RAW_RETURN_TABLES_PATH), "pickle"


def _finish_log(job: str, status: str, started_at: str, start_perf: float, message: str) -> None:
    """ジョブ実行を execution_log に記録する（非致命）。"""
    import datetime as dt
    import time

    try:
        from src.storage import record_execution

        record_execution(
            job, status,
            started_at=started_at,
            finished_at=dt.datetime.now().isoformat(timespec="seconds"),
            duration_sec=time.perf_counter() - start_perf,
            message=message,
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("[run] execution_log 記録失敗 (non-fatal): %s", e)


def _notify_failure(job: str, message: str) -> None:
    """ジョブ失敗を通知する（NOTIFY_SLACK_WEBHOOK があれば Slack、無ければ no-op）。"""
    try:
        from src.operation._notifier import create_notifier

        create_notifier().notify(f"keibam {job} 失敗", message, level="error")
    except Exception as e:  # noqa: BLE001
        logger.warning("[run] 失敗通知に失敗 (non-fatal): %s", e)


def _run_job(job: str, handler, args: argparse.Namespace) -> None:
    """ハンドラを計測・記録付きで実行する（成否を execution_log に記録、失敗時は通知）。"""
    import datetime as dt
    import time

    started_at = dt.datetime.now().isoformat(timespec="seconds")
    start_perf = time.perf_counter()
    try:
        handler(args)
    except SystemExit as e:  # doctor --strict 等の意図的終了
        ok = e.code in (0, None)
        _finish_log(job, "ok" if ok else "failed", started_at, start_perf, f"exit={e.code}")
        raise
    except Exception as e:
        message = f"{type(e).__name__}: {e}"
        _finish_log(job, "failed", started_at, start_perf, message)
        _notify_failure(job, message)
        raise
    else:
        _finish_log(job, "ok", started_at, start_perf, "")
