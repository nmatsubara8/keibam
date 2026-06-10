"""段階オッズ スナップショットの純粋ロジックのテスト（selenium/bs4 不要）。"""

import datetime as dt

import pytest

from src.constants._bet_types import BetType
from src.constants._odds_phases import OddsPhase
from src.preparing._odds_snapshot import build_odds_url
from src.preparing._odds_snapshot import compute_minutes_to_post
from src.preparing._odds_snapshot import make_snapshot
from src.preparing._odds_snapshot import merge_snapshots
from src.preparing._odds_snapshot import snapshots_from_rows


def test_build_odds_url_maps_bet_type_to_page_type():
    url = build_odds_url("202401010101", BetType.UMAREN)
    assert "type=b4" in url
    assert "race_id=202401010101" in url


def test_build_odds_url_unknown_bet_type_raises():
    with pytest.raises(ValueError):
        build_odds_url("r1", "unknown")


def test_compute_minutes_to_post_positive_and_negative():
    post = dt.datetime(2024, 1, 1, 15, 40)
    assert compute_minutes_to_post(post, dt.datetime(2024, 1, 1, 15, 10)) == 30
    assert compute_minutes_to_post(post, dt.datetime(2024, 1, 1, 15, 50)) == -10


def test_make_snapshot_assigns_phase_from_minutes():
    post = dt.datetime(2024, 1, 1, 15, 40)
    # 5 分前 → just_before
    s = make_snapshot("r1", BetType.TANSHO, [3], 4.5, post, dt.datetime(2024, 1, 1, 15, 35))
    assert s.minutes_to_post == 5
    assert s.phase == OddsPhase.JUST_BEFORE
    assert s.combo == (3,)
    assert isinstance(s.odds, float)


def test_make_snapshot_phase_buckets():
    post = dt.datetime(2024, 1, 1, 15, 40)
    cases = {
        dt.datetime(2024, 1, 1, 15, 35): OddsPhase.JUST_BEFORE,  # 5分
        dt.datetime(2024, 1, 1, 15, 10): OddsPhase.THIRTY_MIN,  # 30分
        dt.datetime(2024, 1, 1, 13, 0): OddsPhase.HOURS_BEFORE,  # 160分
        dt.datetime(2023, 12, 31, 18, 0): OddsPhase.PREV_DAY,  # 前日
    }
    for captured, expected in cases.items():
        assert make_snapshot("r1", BetType.TANSHO, [1], 2.0, post, captured).phase == expected


def test_snapshots_from_rows_skips_missing_odds():
    post = dt.datetime(2024, 1, 1, 15, 40)
    captured = dt.datetime(2024, 1, 1, 15, 35)
    rows = [((1,), 2.0), ((2,), 0.0), ((3,), None), ((4,), 5.5)]
    snaps = snapshots_from_rows("r1", BetType.TANSHO, rows, post, captured)
    assert [s.combo for s in snaps] == [(1,), (4,)]


def test_merge_snapshots_is_idempotent_on_same_key():
    post = dt.datetime(2024, 1, 1, 15, 40)
    captured = dt.datetime(2024, 1, 1, 15, 35)
    a = make_snapshot("r1", BetType.TANSHO, [1], 2.0, post, captured)
    merged = merge_snapshots([a], [a])
    assert len(merged) == 1


def test_merge_snapshots_overwrites_with_newer_capture():
    post = dt.datetime(2024, 1, 1, 15, 40)
    old = make_snapshot("r1", BetType.TANSHO, [1], 2.0, post, dt.datetime(2024, 1, 1, 15, 35))
    new = make_snapshot("r1", BetType.TANSHO, [1], 3.0, post, dt.datetime(2024, 1, 1, 15, 38))
    merged = merge_snapshots([old], [new])
    assert len(merged) == 1
    assert merged[0].odds == 3.0


def test_merge_snapshots_keeps_distinct_phases():
    post = dt.datetime(2024, 1, 1, 15, 40)
    just = make_snapshot("r1", BetType.TANSHO, [1], 2.0, post, dt.datetime(2024, 1, 1, 15, 35))
    thirty = make_snapshot("r1", BetType.TANSHO, [1], 2.5, post, dt.datetime(2024, 1, 1, 15, 10))
    merged = merge_snapshots([], [just, thirty])
    assert len(merged) == 2


def test_combo_to_str_joins_with_hyphen():
    from src.preparing._odds_snapshot import combo_to_str

    assert combo_to_str((3, 7, 11)) == "3-7-11"
    assert combo_to_str([1]) == "1"


def test_snapshots_to_records_serializes_for_db():
    from src.preparing._odds_snapshot import snapshots_to_records

    post = dt.datetime(2024, 1, 1, 15, 40)
    captured = dt.datetime(2024, 1, 1, 15, 35)
    s = make_snapshot("r1", BetType.TANSHO, [3], 4.5, post, captured)
    records = snapshots_to_records([s])
    assert records == [
        {
            "race_id": "r1",
            "bet_type": BetType.TANSHO,
            "combo": "3",
            "odds": 4.5,
            "captured_at": "2024-01-01T15:35:00",
            "minutes_to_post": 5,
            "phase": OddsPhase.JUST_BEFORE,
        }
    ]
