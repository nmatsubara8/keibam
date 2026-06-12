"""src/storage/_execution_log.py: ジョブ実行記録のテスト（DB は tmp に隔離）。"""

from __future__ import annotations

import pytest

from src.storage._db import _reset_engine_for_testing
from src.storage._execution_log import load_executions, record_execution


@pytest.fixture(autouse=True)
def _reset_engine():
    _reset_engine_for_testing()
    yield
    _reset_engine_for_testing()


class TestRecordAndLoad:
    def test_roundtrip(self, tmp_path):
        db = str(tmp_path / "test.db")
        record_execution(
            "ingest", "ok",
            started_at="2026-06-12T00:00:00", finished_at="2026-06-12T00:01:00",
            duration_sec=60.0, message="done", db_path=db,
        )
        rows = load_executions(db_path=db)
        assert len(rows) == 1
        assert rows[0]["job"] == "ingest"
        assert rows[0]["status"] == "ok"
        assert rows[0]["duration_sec"] == 60.0

    def test_newest_first_and_limit(self, tmp_path):
        db = str(tmp_path / "test.db")
        for i in range(5):
            record_execution(f"job{i}", "ok", duration_sec=float(i), db_path=db)
        rows = load_executions(limit=3, db_path=db)
        assert len(rows) == 3
        # id 降順 → 最後に入れた job4 が先頭
        assert rows[0]["job"] == "job4"

    def test_failed_status_recorded(self, tmp_path):
        db = str(tmp_path / "test.db")
        record_execution("retrain", "failed", message="boom", db_path=db)
        rows = load_executions(db_path=db)
        assert rows[0]["status"] == "failed"
        assert "boom" in rows[0]["message"]

    def test_empty_when_no_records(self, tmp_path):
        db = str(tmp_path / "test.db")
        assert load_executions(db_path=db) == []
