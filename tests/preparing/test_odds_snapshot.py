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


# ---------------------------------------------------------------------------
# 連系オッズパーサ（id ベース汎用パーサ）
# ---------------------------------------------------------------------------


def test_parse_odds_value_plain_and_range():
    from src.preparing._odds_snapshot import parse_odds_value

    assert parse_odds_value("12.3") == 12.3
    # ワイド・複勝のレンジ表記は保守的に下限を採用
    assert parse_odds_value("1.5 - 2.0") == 1.5
    assert parse_odds_value("---") is None
    assert parse_odds_value("") is None


def test_split_combo_digits():
    from src.preparing._odds_snapshot import _split_combo_digits

    assert _split_combo_digits("0102") == (1, 2)
    assert _split_combo_digits("010203") == (1, 2, 3)
    assert _split_combo_digits("18") == (18,)
    assert _split_combo_digits("123") is None  # 奇数桁
    assert _split_combo_digits("0001") is None  # 馬番 0 は不正
    assert _split_combo_digits("") is None


def test_parse_combo_odds_html_umaren():
    from src.preparing._odds_snapshot import parse_combo_odds_html

    html = """
    <table>
    <tr><td id="odds-4-0102" class="Odds">12.3</td></tr>
    <tr><td id="odds-4-0118" class="Odds">99.9</td></tr>
    </table>
    """
    rows = parse_combo_odds_html(html, BetType.UMAREN)
    assert dict(rows) == {(1, 2): 12.3, (1, 18): 99.9}


def test_parse_combo_odds_html_sanrentan_preserves_order():
    from src.preparing._odds_snapshot import parse_combo_odds_html

    html = '<td id="odds-8-030201">123.4</td>'
    rows = parse_combo_odds_html(html, BetType.SANRENTAN)
    assert rows == [((3, 2, 1), 123.4)]


def test_parse_combo_odds_html_filters_other_bet_types():
    from src.preparing._odds_snapshot import parse_combo_odds_html

    # b1 ページには単勝(1)と複勝(2)の id が同居する
    html = """
    <td id="odds-1-01">2.5</td>
    <td id="odds-2-01">1.3 - 1.8</td>
    """
    tansho = parse_combo_odds_html(html, BetType.TANSHO)
    fukusho = parse_combo_odds_html(html, BetType.FUKUSHO)
    assert tansho == [((1,), 2.5)]
    assert fukusho == [((1,), 1.3)]


def test_parse_combo_odds_html_skips_pending_and_duplicates():
    from src.preparing._odds_snapshot import parse_combo_odds_html

    html = """
    <td id="odds-5-0102">1.5 - 2.0</td>
    <td id="odds-5-0102">9.9</td>
    <td id="odds-5-0103">---</td>
    """
    rows = parse_combo_odds_html(html, BetType.WIDE)
    # 重複は最初の値、未確定はスキップ
    assert rows == [((1, 2), 1.5)]


def test_parse_combo_odds_html_unknown_bet_type_raises():
    from src.preparing._odds_snapshot import parse_combo_odds_html

    with pytest.raises(ValueError):
        parse_combo_odds_html("<html></html>", "unknown")
