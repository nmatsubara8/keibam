"""オッズ時系列蓄積の不変条件（保証）テスト。

2026-07 の「各レースを単一時刻でしか取得せず evaluate-odds-dynamics が全 NaN」を
再発させないためのガード。ここでは **同一 race_id を複数の captured_at で取得したとき、
DB `raw_odds_snapshots` が取得時刻ごとに行を累積し（上書きしない）、post_time が
復元可能である** ことを回帰テストで固定する。

pickle は (race_id, bet_type, combo, phase) で dedup するため同一 phase 内の複数取得を
潰す（時間解像度を失う）が、DB は主キーに captured_at を含むため全時系列を保持する。
評価に使える真の系列は DB 側であり、その累積保証をここで担保する。
"""
from __future__ import annotations

import datetime as dt
import os

import pytest
from sqlalchemy import text

from src.constants._bet_types import BetType
from src.preparing import odds_scheduler
from src.preparing._odds_snapshot import compute_minutes_to_post
from src.preparing._odds_snapshot import make_snapshot
from src.storage._db import _reset_engine_for_testing
from src.storage._db import get_engine


@pytest.fixture()
def db_path(tmp_path, monkeypatch):
    """persist() → _upsert_db が向く DB を tmp に隔離し、同じパスを読み返せるようにする。"""
    _reset_engine_for_testing()
    import src.storage
    from src.storage import RawDataRepo

    path = os.path.join(tmp_path, "acc.db")

    class _TmpRepo(RawDataRepo):
        def __init__(self):
            super().__init__(path)

    monkeypatch.setattr(src.storage, "RawDataRepo", _TmpRepo)
    yield path
    _reset_engine_for_testing()


def _distinct_captured_at(path: str, race_id: str) -> int:
    eng = get_engine(path)
    with eng.connect() as conn:
        row = conn.execute(
            text(
                "SELECT COUNT(DISTINCT captured_at) FROM raw_odds_snapshots WHERE race_id = :r"
            ),
            {"r": race_id},
        ).fetchone()
    return int(row[0]) if row else 0


def _total_rows(path: str, race_id: str) -> int:
    eng = get_engine(path)
    with eng.connect() as conn:
        row = conn.execute(
            text("SELECT COUNT(*) FROM raw_odds_snapshots WHERE race_id = :r"), {"r": race_id}
        ).fetchone()
    return int(row[0]) if row else 0


def test_same_race_multiple_times_accumulate_in_db(db_path):
    """同一 race を3つの取得時刻で保存 → DB は captured_at ごとに累積（=3）する。"""
    post = dt.datetime(2024, 1, 1, 15, 40)
    pkl = os.path.join(os.path.dirname(db_path), "odds.pkl")

    # 30分前・20分前・5分前の3ティック（別 captured_at）。同一 combo=[1] の単勝。
    for captured in (
        dt.datetime(2024, 1, 1, 15, 10),
        dt.datetime(2024, 1, 1, 15, 20),
        dt.datetime(2024, 1, 1, 15, 35),
    ):
        snap = make_snapshot("r1", BetType.TANSHO, [1], 2.0 + captured.minute / 100, post, captured)
        odds_scheduler.persist([snap], pkl)

    # DB は取得時刻ごとに累積（上書きしない）＝これが評価に使える真の系列。
    assert _distinct_captured_at(db_path, "r1") == 3
    assert _total_rows(db_path, "r1") == 3


def test_same_captured_at_is_idempotent(db_path):
    """同一 (race, combo, captured_at) の再取得は冪等（重複行を作らない）。"""
    post = dt.datetime(2024, 1, 1, 15, 40)
    captured = dt.datetime(2024, 1, 1, 15, 35)
    pkl = os.path.join(os.path.dirname(db_path), "odds.pkl")
    snap = make_snapshot("r1", BetType.TANSHO, [1], 2.0, post, captured)

    odds_scheduler.persist([snap], pkl)
    odds_scheduler.persist([snap], pkl)  # 2 回目
    odds_scheduler.persist([snap], pkl)  # 3 回目

    assert _distinct_captured_at(db_path, "r1") == 1
    assert _total_rows(db_path, "r1") == 1


def test_post_time_recoverable_from_captured_at_and_mtp(db_path):
    """post_time は列に無いが captured_at + minutes_to_post で分精度復元できる。"""
    post = dt.datetime(2024, 1, 1, 15, 40)
    captured = dt.datetime(2024, 1, 1, 15, 12, 30)  # 27分30秒前
    pkl = os.path.join(os.path.dirname(db_path), "odds.pkl")
    snap = make_snapshot("r1", BetType.TANSHO, [1], 3.0, post, captured)
    odds_scheduler.persist([snap], pkl)

    eng = get_engine(db_path)
    with eng.connect() as conn:
        cap, mtp = conn.execute(
            text("SELECT captured_at, minutes_to_post FROM raw_odds_snapshots WHERE race_id='r1'")
        ).fetchone()

    recovered = dt.datetime.fromisoformat(cap) + dt.timedelta(minutes=int(mtp))
    # minutes_to_post は分未満切り捨てのため復元誤差は 60 秒未満。
    assert abs((recovered - post).total_seconds()) < 60
    # 保存された mtp は compute_minutes_to_post と一致する（27分30秒→27）。
    assert int(mtp) == compute_minutes_to_post(post, captured) == 27
