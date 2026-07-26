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


def test_build_odds_url_central_uses_race_domain():
    # 中央（場コード 01）は従来どおり race.netkeiba.com/odds/index.html
    url = build_odds_url("202401010101", BetType.TANSHO)
    assert url.startswith("https://race.netkeiba.com/odds/index.html?")


def test_build_odds_url_nar_uses_nar_domain_and_path():
    # 地方（門別30・大井44）は nar.netkeiba.com、パスは /odds/（index.html なし・実 URL 準拠）
    assert build_odds_url("202630072201", BetType.TANSHO).startswith(
        "https://nar.netkeiba.com/odds/?"
    )
    assert build_odds_url("202444010101", BetType.UMAREN).startswith(
        "https://nar.netkeiba.com/odds/?"
    )


def test_build_odds_url_unknown_bet_type_raises():
    with pytest.raises(ValueError):
        build_odds_url("r1", "unknown")


def test_scraper_uses_env_timeouts(monkeypatch):
    """既定スクレイパは長めのタイムアウト（重い連系ページの描画待ち）を使う。"""
    from src.preparing._odds_snapshot import OddsSnapshotScraper

    monkeypatch.setenv("KEIBA_ODDS_SELECTOR_TIMEOUT_MS", "15000")
    monkeypatch.setenv("KEIBA_ODDS_TIMEOUT_MS", "45000")
    inner = OddsSnapshotScraper()._ensure_scraper()
    assert inner._selector_timeout_ms == 15000
    assert inner._timeout_ms == 45000


def test_scraper_default_waits_for_odds_cells():
    """既定の待機セレクタが実オッズセル（id^=odds-）を含む。"""
    from src.preparing._odds_snapshot import OddsSnapshotScraper

    assert "odds-" in OddsSnapshotScraper()._odds_table_selector


def test_build_odds_url_wakuren_maps_to_b3():
    """枠連が b3 ページにマップされる（カバレッジ拡大）。"""
    url = build_odds_url("202401010101", BetType.WAKUREN)
    assert "type=b3" in url


def test_all_seven_bet_types_have_odds_page():
    """単複/枠連/馬連/馬単/ワイド/三連複/三連単の全券種でオッズ URL を構築できる。"""
    from src.preparing._odds_snapshot import ODDS_ID_TYPE
    from src.preparing._odds_snapshot import ODDS_PAGE_TYPE

    for bt in (BetType.TANSHO, BetType.FUKUSHO, BetType.WAKUREN, BetType.UMAREN,
               BetType.UMATAN, BetType.WIDE, BetType.SANRENPUKU, BetType.SANRENTAN):
        assert bt in ODDS_PAGE_TYPE and bt in ODDS_ID_TYPE
        assert build_odds_url("202401010101", bt).startswith("https://race.netkeiba.com/odds/")


def test_compute_minutes_to_post_positive_and_negative():
    post = dt.datetime(2024, 1, 1, 15, 40)
    assert compute_minutes_to_post(post, dt.datetime(2024, 1, 1, 15, 10)) == 30
    assert compute_minutes_to_post(post, dt.datetime(2024, 1, 1, 15, 50)) == -10


def test_make_snapshot_assigns_phase_from_minutes():
    post = dt.datetime(2024, 1, 1, 15, 40)
    # 5 分前 → t5 チェックポイント
    s = make_snapshot("r1", BetType.TANSHO, [3], 4.5, post, dt.datetime(2024, 1, 1, 15, 35))
    assert s.minutes_to_post == 5
    assert s.phase == OddsPhase.T5
    assert s.combo == (3,)
    assert isinstance(s.odds, float)


def test_make_snapshot_phase_buckets():
    post = dt.datetime(2024, 1, 1, 15, 40)
    cases = {
        dt.datetime(2024, 1, 1, 15, 39): OddsPhase.T0,  # 1分
        dt.datetime(2024, 1, 1, 15, 35): OddsPhase.T5,  # 5分
        dt.datetime(2024, 1, 1, 15, 30): OddsPhase.T10,  # 10分
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
    old = make_snapshot("r1", BetType.TANSHO, [1], 2.0, post, dt.datetime(2024, 1, 1, 15, 34))
    new = make_snapshot("r1", BetType.TANSHO, [1], 3.0, post, dt.datetime(2024, 1, 1, 15, 36))
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
            "phase": OddsPhase.T5,
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


def test_parse_combo_odds_html_underscore_separator():
    """現行 b1 ページは券種コードと馬番列を `_` で区切る（``odds-1_07``）。

    旧 DOM のハイフン区切り（``odds-1-07``）と並んで underscore も解釈できること。
    回帰防止: これが壊れると午後の単勝オッズ取得が 0 件になり予測が崩壊する。
    """
    from src.preparing._odds_snapshot import parse_combo_odds_html

    html = """
    <td class="Odds Popular"><span id="odds-1_02">1.9</span></td>
    <td class="Odds Popular"><span id="odds-1_10">5.0</span></td>
    <td class="Odds"><span id="odds-2_02">1.1 - 1.1</span></td>
    """
    tansho = parse_combo_odds_html(html, BetType.TANSHO)
    fukusho = parse_combo_odds_html(html, BetType.FUKUSHO)
    assert sorted(tansho) == [((2,), 1.9), ((10,), 5.0)]
    assert fukusho == [((2,), 1.1)]


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
