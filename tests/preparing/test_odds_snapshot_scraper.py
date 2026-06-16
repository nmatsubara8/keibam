"""§4 移行後 OddsSnapshotScraper のテスト（Playwright AbstractScraper を DI）。"""

from __future__ import annotations

import asyncio
import datetime as dt
import importlib.util

import pytest

from src.constants._bet_types import BetType
from src.preparing._odds_snapshot import OddsSnapshotScraper, build_odds_url

# capture() は parse_win_odds_html(bs4) を経由するため bs4 が無い環境ではスキップ。
_HAS_BS4 = importlib.util.find_spec("bs4") is not None
_bs4_required = pytest.mark.skipif(not _HAS_BS4, reason="bs4 未インストール")


_TANSHO_HTML = """
<html><body><table>
<tr><td class="Umaban">1</td><td class="Odds"><span>2.5</span></td></tr>
<tr><td class="Umaban">2</td><td class="Odds"><span>5.0</span></td></tr>
</table></body></html>
"""

_UMAREN_HTML = """
<html><body><table>
<tr><td id="odds-4-0102" class="Odds">12.3</td></tr>
<tr><td id="odds-4-0103" class="Odds">45.6</td></tr>
</table></body></html>
"""


class _StubScraper:
    """fetch_sync / fetch を記録するスタブ AbstractScraper。"""

    def __init__(self, html):
        self.html = html
        self.fetched_urls: list = []
        self.wait_selectors: list = []

    def fetch_sync(self, url, *, wait_selector=None, wait_until="domcontentloaded"):
        self.fetched_urls.append(url)
        self.wait_selectors.append(wait_selector)
        return self.html

    async def fetch(self, url, *, wait_selector=None, wait_until="domcontentloaded"):
        self.fetched_urls.append(url)
        self.wait_selectors.append(wait_selector)
        return self.html


class TestFetchHtml:
    def test_uses_injected_scraper(self):
        stub = _StubScraper(_TANSHO_HTML)
        scraper = OddsSnapshotScraper(scraper=stub)
        html = scraper.fetch_html("202401010101", BetType.TANSHO)
        assert html == _TANSHO_HTML

    def test_fetches_correct_url(self):
        stub = _StubScraper(_TANSHO_HTML)
        scraper = OddsSnapshotScraper(scraper=stub)
        scraper.fetch_html("202401010101", BetType.TANSHO)
        assert stub.fetched_urls[0] == build_odds_url("202401010101", BetType.TANSHO)

    def test_waits_for_odds_selector(self):
        stub = _StubScraper(_TANSHO_HTML)
        scraper = OddsSnapshotScraper(scraper=stub)
        scraper.fetch_html("202401010101", BetType.TANSHO)
        assert stub.wait_selectors[0] is not None


class TestFetchHtmlAsync:
    def test_async_fetch(self):
        stub = _StubScraper(_TANSHO_HTML)
        scraper = OddsSnapshotScraper(scraper=stub)
        html = asyncio.run(scraper.fetch_html_async("r1", BetType.TANSHO))
        assert html == _TANSHO_HTML


class TestCapture:
    @_bs4_required
    def test_capture_produces_snapshots(self):
        stub = _StubScraper(_TANSHO_HTML)
        scraper = OddsSnapshotScraper(scraper=stub)
        post = dt.datetime(2024, 1, 1, 15, 40)
        captured = dt.datetime(2024, 1, 1, 15, 35)
        snaps = scraper.capture("r1", BetType.TANSHO, post, captured)
        assert len(snaps) == 2
        odds_values = sorted(s.odds for s in snaps)
        assert odds_values == [2.5, 5.0]

    @_bs4_required
    def test_capture_sets_minutes_to_post(self):
        stub = _StubScraper(_TANSHO_HTML)
        scraper = OddsSnapshotScraper(scraper=stub)
        post = dt.datetime(2024, 1, 1, 15, 40)
        captured = dt.datetime(2024, 1, 1, 15, 35)
        snaps = scraper.capture("r1", BetType.TANSHO, post, captured)
        assert all(s.minutes_to_post == 5 for s in snaps)

    @_bs4_required
    def test_capture_non_tansho_no_ids_returns_empty(self):
        """連系: id 属性が無い HTML からは何も取れない（フォールバックは単勝・複勝のみ）。"""
        stub = _StubScraper(_TANSHO_HTML)
        scraper = OddsSnapshotScraper(scraper=stub)
        post = dt.datetime(2024, 1, 1, 15, 40)
        snaps = scraper.capture("r1", BetType.UMAREN, post)
        assert snaps == []

    @_bs4_required
    def test_capture_umaren_produces_combo_snapshots(self):
        stub = _StubScraper(_UMAREN_HTML)
        scraper = OddsSnapshotScraper(scraper=stub)
        post = dt.datetime(2024, 1, 1, 15, 40)
        captured = dt.datetime(2024, 1, 1, 15, 35)
        snaps = scraper.capture("r1", BetType.UMAREN, post, captured)
        assert {(s.combo, s.odds) for s in snaps} == {((1, 2), 12.3), ((1, 3), 45.6)}


class TestDefaultScraperLazy:
    def test_default_scraper_not_created_until_use(self):
        """scraper 未指定でも __init__ では生成しない（遅延生成）。"""
        scraper = OddsSnapshotScraper()
        assert scraper._scraper is None
