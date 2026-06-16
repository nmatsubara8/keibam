"""odds_scheduler の永続化・取得オーケストレーションのテスト（selenium 不要）。

スタブ scraper を DI し、ファイル冪等追記と複数レース・失敗継続の挙動を検証する。
"""

import datetime as dt
import os

import pytest

from src.constants._bet_types import BetType
from src.preparing import odds_scheduler
from src.preparing._odds_snapshot import make_snapshot


@pytest.fixture(autouse=True)
def _isolate_db(tmp_path, monkeypatch):
    """persist() → _upsert_db が実 DB (data/keibam.db) を汚染しないよう tmp DB に隔離する。"""
    import src.storage
    from src.storage import RawDataRepo

    db_path = os.path.join(tmp_path, "isolated.db")

    class _TmpRepo(RawDataRepo):
        def __init__(self):
            super().__init__(db_path)

    monkeypatch.setattr(src.storage, "RawDataRepo", _TmpRepo)


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


def test_build_race_post_times_skips_empty_time():
    pairs = odds_scheduler.build_race_post_times(
        ["r1", "r2", "r3"], ["10:05", "", "15:40"], "20240101"
    )
    assert pairs == [
        ("r1", dt.datetime(2024, 1, 1, 10, 5)),
        ("r3", dt.datetime(2024, 1, 1, 15, 40)),
    ]


def test_select_target_races_window_filtering():
    now = dt.datetime(2024, 1, 1, 15, 0)
    pairs = [
        ("past", dt.datetime(2024, 1, 1, 14, 50)),  # 発走済み → 除外
        ("soon", dt.datetime(2024, 1, 1, 15, 30)),  # 30 分後 → 対象
        ("late", dt.datetime(2024, 1, 1, 16, 30)),  # 90 分後 → window 外
    ]
    targets = odds_scheduler.select_target_races(pairs, now, window_minutes=45)
    assert [rid for rid, _ in targets] == ["soon"]


def test_run_auto_captures_only_window_races(tmp_path):
    path = os.path.join(tmp_path, "odds.pkl")
    now = dt.datetime(2024, 1, 1, 15, 0)

    def fake_fetcher(date_str):
        assert date_str == "20240101"
        return ["r1", "r2"], ["15:30", "17:00"]

    merged = odds_scheduler.run_auto(
        _StubScraper(),
        [BetType.TANSHO],
        window_minutes=45,
        path=path,
        now=now,
        race_id_time_fetcher=fake_fetcher,
    )
    assert {s.race_id for s in merged} == {"r1"}
    # minutes_to_post = 30 → thirty_min フェーズ
    assert merged[0].minutes_to_post == 30


def test_run_auto_no_target_returns_existing(tmp_path):
    path = os.path.join(tmp_path, "odds.pkl")
    now = dt.datetime(2024, 1, 1, 23, 0)

    def fake_fetcher(date_str):
        return ["r1"], ["15:30"]

    merged = odds_scheduler.run_auto(
        _StubScraper(),
        [BetType.TANSHO],
        window_minutes=45,
        path=path,
        now=now,
        race_id_time_fetcher=fake_fetcher,
    )
    assert merged == []
    assert not os.path.exists(path)


def test_parse_args_auto_mode_defaults():
    args = odds_scheduler._parse_args(["--auto"])
    assert args.auto is True
    assert args.window_minutes == 60
    assert args.date is None


def test_parse_args_manual_mode_requires_args():
    import pytest

    with pytest.raises(SystemExit):
        odds_scheduler._parse_args(["--race-id", "r1"])  # --phase / --post-time 不足


def test_persist_upserts_to_db(tmp_path):
    """persist は pickle 保存に加えて SQLite にも冪等 upsert する。

    DB の向き先は autouse の _isolate_db fixture が tmp_path/isolated.db に隔離済み。
    検証読出しは未パッチの実クラス（src.storage._repo）で同じ DB を直接開く。
    """
    from src.storage._repo import RawDataRepo as _RealRepo

    db_path = os.path.join(tmp_path, "isolated.db")

    path = os.path.join(tmp_path, "odds.pkl")
    post = dt.datetime(2024, 1, 1, 15, 40)
    captured = dt.datetime(2024, 1, 1, 15, 35)
    snap = make_snapshot("r1", BetType.TANSHO, [1], 2.0, post, captured)

    odds_scheduler.persist([snap], path)
    # 同じスナップショットの再投入は INSERT OR IGNORE で重複しない
    odds_scheduler.persist([snap], path)

    df = _RealRepo(db_path).read("raw_odds_snapshots")
    assert len(df) == 1
    assert df.iloc[0]["combo"] == "1"
