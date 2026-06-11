"""オッズソース抽象化（_odds_source）のテスト。"""

import datetime as dt
import json
import os

import pytest

from src.preparing._odds_source import AbstractOddsSource
from src.preparing._odds_source import JraVanFileDropSource
from src.preparing._odds_source import NetkeibaOddsSource
from src.preparing._odds_source import create_odds_source


class TestFactory:
    def test_creates_netkeiba(self):
        assert isinstance(create_odds_source("netkeiba"), NetkeibaOddsSource)

    def test_creates_jravan(self):
        assert isinstance(create_odds_source("jravan"), JraVanFileDropSource)

    def test_unknown_raises(self):
        with pytest.raises(ValueError):
            create_odds_source("unknown")


class TestNetkeibaSource:
    def test_fetch_win_odds_delegates_to_scraper(self):
        class _StubScraper:
            def fetch_html(self, race_id, bet_type):
                return '<td id="odds-1-01">2.5</td><td id="odds-1-02">5.0</td>'

        source = NetkeibaOddsSource(scraper=_StubScraper())
        odds = source.fetch_win_odds("202401010101")
        assert odds == [(1, 2.5), (2, 5.0)]


class TestJraVanFileDropSource:
    def _drop(self, tmp_path, race_id, captured_at, odds_rows, post_time="2026-06-07T15:40:00"):
        payload = {
            "race_id": race_id,
            "post_time": post_time,
            "captured_at": captured_at,
            "win_odds": odds_rows,
        }
        fname = f"{race_id}_{captured_at.replace(':', '')}.json"
        with open(os.path.join(tmp_path, fname), "w") as f:
            json.dump(payload, f)

    def test_reads_latest_payload_per_race(self, tmp_path):
        self._drop(tmp_path, "r1", "2026-06-07T15:00:00", [{"umaban": 1, "odds": 3.0}])
        self._drop(tmp_path, "r1", "2026-06-07T15:30:00", [{"umaban": 1, "odds": 2.4}])
        source = JraVanFileDropSource(str(tmp_path))
        assert source.fetch_win_odds("r1") == [(1, 2.4)]

    def test_fetch_today_races_filters_by_date(self, tmp_path):
        self._drop(tmp_path, "r1", "2026-06-07T15:00:00", [], post_time="2026-06-07T15:40:00")
        self._drop(tmp_path, "r2", "2026-06-08T15:00:00", [], post_time="2026-06-08T15:40:00")
        source = JraVanFileDropSource(str(tmp_path))
        races = source.fetch_today_races("20260607")
        assert races == [("r1", dt.datetime(2026, 6, 7, 15, 40))]

    def test_missing_dir_returns_empty(self, tmp_path):
        source = JraVanFileDropSource(os.path.join(tmp_path, "nope"))
        assert source.fetch_today_races("20260607") == []
        assert source.fetch_win_odds("r1") == []

    def test_broken_json_skipped(self, tmp_path):
        with open(os.path.join(tmp_path, "broken.json"), "w") as f:
            f.write("{not json")
        self._drop(tmp_path, "r1", "2026-06-07T15:00:00", [{"umaban": 3, "odds": 9.9}])
        source = JraVanFileDropSource(str(tmp_path))
        assert source.fetch_win_odds("r1") == [(3, 9.9)]

    def test_invalid_odds_filtered(self, tmp_path):
        self._drop(tmp_path, "r1", "2026-06-07T15:00:00",
                   [{"umaban": 1, "odds": 2.0}, {"umaban": 2, "odds": 0}, {"umaban": 3, "odds": None}])
        source = JraVanFileDropSource(str(tmp_path))
        assert source.fetch_win_odds("r1") == [(1, 2.0)]


def test_abstract_contract():
    assert hasattr(AbstractOddsSource, "fetch_today_races")
    assert hasattr(AbstractOddsSource, "fetch_win_odds")
