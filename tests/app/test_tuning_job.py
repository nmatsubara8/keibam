"""app/_tuning_job.py: Optuna チューニングの UI 起動・状態監視のテスト。

実プロセスは起動せず、popen / is_alive を差し替えて決定的に検証する。
完了検出はログ末尾の終了センチネル `__TUNING_EXIT__ <code>` を直接書いて確認する。
"""

from __future__ import annotations

import datetime as dt
import os

import pytest

from app import _tuning_job as tj


class _FakeProc:
    def __init__(self, pid: int = 4242):
        self.pid = pid
        self.args = None
        self.kwargs = None


def _fixed_now():
    return dt.datetime(2026, 6, 11, 12, 0, 0)


# ---------------------------------------------------------------------------
# start_tuning_job
# ---------------------------------------------------------------------------


class TestStart:
    def test_writes_running_status_and_invokes_popen(self, tmp_path):
        captured = {}

        def fake_popen(cmd, **kwargs):
            captured["cmd"] = cmd
            captured["kwargs"] = kwargs
            return _FakeProc(pid=999)

        md = str(tmp_path / "models")
        status = tj.start_tuning_job(
            md, popen=fake_popen, now=_fixed_now, is_alive=lambda pid: False
        )

        assert status["status"] == "running"
        assert status["pid"] == 999
        # retrain --with-tuning がコマンドに含まれる（shell 文字列）
        assert "retrain --with-tuning" in captured["cmd"]
        # デタッチ起動
        assert captured["kwargs"].get("start_new_session") is True
        assert captured["kwargs"].get("shell") is True
        # 状態ファイルが書かれている
        assert os.path.exists(tj.status_path(md))
        assert tj.read_job_status(md)["pid"] == 999

    def test_blocks_double_start_while_running(self, tmp_path):
        md = str(tmp_path / "models")
        tj.start_tuning_job(md, popen=lambda *a, **k: _FakeProc(1234), now=_fixed_now,
                            is_alive=lambda pid: False)
        # 2 回目は pid が生存しているとみなすとブロックされる
        with pytest.raises(RuntimeError, match="既に実行中"):
            tj.start_tuning_job(md, popen=lambda *a, **k: _FakeProc(5678),
                                now=_fixed_now, is_alive=lambda pid: True)


# ---------------------------------------------------------------------------
# refresh_job_status（プロセス完了の検出）
# ---------------------------------------------------------------------------


def _start(md, pid=4242):
    return tj.start_tuning_job(md, popen=lambda *a, **k: _FakeProc(pid),
                               now=_fixed_now, is_alive=lambda p: False)


class TestRefresh:
    def test_running_when_process_alive(self, tmp_path):
        md = str(tmp_path / "models")
        _start(md)
        out = tj.refresh_job_status(md, is_alive=lambda pid: True)
        assert out["status"] == "running"

    def test_completed_when_exit_zero(self, tmp_path):
        md = str(tmp_path / "models")
        _start(md)
        with open(tj.log_path(md), "a", encoding="utf-8") as f:
            f.write("...\n__TUNING_EXIT__ 0\n")
        out = tj.refresh_job_status(md, is_alive=lambda pid: False, now=_fixed_now)
        assert out["status"] == "completed"
        assert out["exit_code"] == 0
        assert out["finished_at"] is not None

    def test_failed_when_exit_nonzero(self, tmp_path):
        md = str(tmp_path / "models")
        _start(md)
        with open(tj.log_path(md), "a", encoding="utf-8") as f:
            f.write("Traceback...\n__TUNING_EXIT__ 1\n")
        out = tj.refresh_job_status(md, is_alive=lambda pid: False, now=_fixed_now)
        assert out["status"] == "failed"
        assert out["exit_code"] == 1

    def test_unknown_when_dead_without_sentinel(self, tmp_path):
        md = str(tmp_path / "models")
        _start(md)
        out = tj.refresh_job_status(md, is_alive=lambda pid: False, now=_fixed_now)
        assert out["status"] == "unknown"
        assert out["exit_code"] is None

    def test_none_when_no_job(self, tmp_path):
        md = str(tmp_path / "models")
        assert tj.refresh_job_status(md) is None

    def test_terminal_status_is_stable(self, tmp_path):
        md = str(tmp_path / "models")
        _start(md)
        with open(tj.log_path(md), "a", encoding="utf-8") as f:
            f.write("__TUNING_EXIT__ 0\n")
        tj.refresh_job_status(md, is_alive=lambda pid: False, now=_fixed_now)
        # 完了後は is_alive を変えても running に戻らない
        again = tj.refresh_job_status(md, is_alive=lambda pid: True)
        assert again["status"] == "completed"


# ---------------------------------------------------------------------------
# stop_tuning_job
# ---------------------------------------------------------------------------


class TestStop:
    def test_sends_signal_and_marks_cancelled(self, tmp_path):
        md = str(tmp_path / "models")
        _start(md, pid=4242)
        killed = {}

        def fake_killer(pgid, sig):
            killed["pgid"] = pgid
            killed["sig"] = sig

        out = tj.stop_tuning_job(
            md, is_alive=lambda pid: True, killer=fake_killer,
            get_pgid=lambda pid: pid, now=_fixed_now,
        )
        assert out["status"] == "cancelled"
        assert killed["pgid"] == 4242  # プロセスグループへシグナル送信された
        assert "sig" in killed

    def test_noop_when_not_running(self, tmp_path):
        md = str(tmp_path / "models")
        assert tj.stop_tuning_job(md) is None


# ---------------------------------------------------------------------------
# tail_log / read_job_status
# ---------------------------------------------------------------------------


class TestReaders:
    def test_tail_log_empty_when_missing(self, tmp_path):
        assert tj.tail_log(str(tmp_path / "models")) == ""

    def test_tail_log_returns_last_lines(self, tmp_path):
        md = str(tmp_path / "models")
        os.makedirs(md)
        with open(tj.log_path(md), "w", encoding="utf-8") as f:
            f.write("\n".join(f"line{i}" for i in range(100)))
        out = tj.tail_log(md, n=5)
        assert "line99" in out
        assert "line0\n" not in out

    def test_read_status_none_when_missing(self, tmp_path):
        assert tj.read_job_status(str(tmp_path / "models")) is None
