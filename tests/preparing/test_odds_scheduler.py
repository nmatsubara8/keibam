"""odds_scheduler の永続化・取得オーケストレーションのテスト（selenium 不要）。

スタブ scraper を DI し、ファイル冪等追記と複数レース・失敗継続の挙動を検証する。
"""

import datetime as dt
import os

from src.constants._bet_types import BetType
from src.preparing import odds_scheduler
from src.preparing._odds_snapshot import make_snapshot


class _StubScraper:
    """指定された (race_id, bet_type) ごとに固定スナップショットを返すスタブ。"""

    def __init__(self, fail_on=None):
        self._fail_on = fail_on or set()

    def capture(self, race_id, bet_type, post_time, captured_at):
        if (race_id, bet_type) in self._fail_on:
            raise RuntimeError("boom")
        return [make_snapshot(race_id, bet_type, [1], 2.0, post_time, captured_at)]


def test_persist_roundtrip_and_idempotent(tmp_path):
    path = os.path.join(tmp_path, "odds.pkl")
    post = dt.datetime(2024, 1, 1, 15, 40)
    captured = dt.datetime(2024, 1, 1, 15, 35)
    snap = make_snapshot("r1", BetType.TANSHO, [1], 2.0, post, captured)

    first = odds_scheduler.persist([snap], path)
    assert len(first) == 1
    # 同じものを再投入しても増えない（冪等）
    second = odds_scheduler.persist([snap], path)
    assert len(second) == 1


def test_run_collects_multiple_races(tmp_path):
    path = os.path.join(tmp_path, "odds.pkl")
    post = dt.datetime(2024, 1, 1, 15, 40)
    captured = dt.datetime(2024, 1, 1, 15, 35)
    merged = odds_scheduler.run(
        ["r1", "r2"], post, [BetType.TANSHO], _StubScraper(), path=path, captured_at=captured
    )
    assert {s.race_id for s in merged} == {"r1", "r2"}


def test_run_continues_after_capture_failure(tmp_path):
    path = os.path.join(tmp_path, "odds.pkl")
    post = dt.datetime(2024, 1, 1, 15, 40)
    captured = dt.datetime(2024, 1, 1, 15, 35)
    scraper = _StubScraper(fail_on={("r1", BetType.TANSHO)})
    merged = odds_scheduler.run(
        ["r1", "r2"], post, [BetType.TANSHO], scraper, path=path, captured_at=captured
    )
    # r1 は失敗、r2 のみ収集される
    assert {s.race_id for s in merged} == {"r2"}


def test_load_snapshots_missing_file_returns_empty(tmp_path):
    path = os.path.join(tmp_path, "nope.pkl")
    assert odds_scheduler.load_snapshots(path) == []


def test_parse_args_defaults_bet_type(tmp_path):
    args = odds_scheduler._parse_args(
        ["--phase", "just_before", "--race-id", "r1", "--post-time", "2024-01-01T15:40"]
    )
    assert args.bet_types is None  # main 側で tansho を既定にする
    assert args.race_ids == ["r1"]
