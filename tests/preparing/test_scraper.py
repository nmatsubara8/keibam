"""§4 Playwright スクレイパー（AbstractScraper / PlaywrightScraper）のテスト。

playwright は未インストールでも本テストが動くよう、PlaywrightScraper のテストでは
playwright.async_api をスタブ注入する。AbstractScraper の既定実装（fetch_many /
scrape_paginated / 同期ブリッジ）は具象サブクラスで検証する。
"""

from __future__ import annotations

import asyncio
import sys
import types

import pytest

from src.preparing._scraper import AbstractScraper, PlaywrightScraper, _looks_empty


# ---------------------------------------------------------------------------
# AbstractScraper 既定実装
# ---------------------------------------------------------------------------

class _MappingScraper(AbstractScraper):
    """url -> html の辞書を返す具象スクレイパー（fetch のみ実装）。"""

    def __init__(self, mapping: dict):
        self.mapping = mapping
        self.calls: list[str] = []
        self.wait_selectors: list = []

    async def fetch(self, url, *, wait_selector=None, wait_until="domcontentloaded"):
        self.calls.append(url)
        self.wait_selectors.append(wait_selector)
        return self.mapping.get(url, "")


_NONEMPTY = "<html>" + "x" * 300 + "<table></table></html>"


class TestFetchMany:
    def test_returns_in_order(self):
        s = _MappingScraper({"a": "HA", "b": "HB"})
        result = asyncio.run(s.fetch_many(["a", "b"]))
        assert result == ["HA", "HB"]

    def test_calls_each_url(self):
        s = _MappingScraper({"a": "HA", "b": "HB"})
        asyncio.run(s.fetch_many(["a", "b"]))
        assert s.calls == ["a", "b"]


class TestScrapePaginated:
    def test_stops_at_empty_page(self):
        base = "http://x/leading"
        mapping = {
            f"{base}?page=1": _NONEMPTY,
            f"{base}?page=2": _NONEMPTY,
            f"{base}?page=3": "",  # empty → stop
        }
        s = _MappingScraper(mapping)
        pages = asyncio.run(s.scrape_paginated(base, max_pages=10, rate_limit_sec=0))
        assert len(pages) == 2

    def test_respects_max_pages(self):
        base = "http://x/leading"
        mapping = {f"{base}?page={n}": _NONEMPTY for n in range(1, 20)}
        s = _MappingScraper(mapping)
        pages = asyncio.run(s.scrape_paginated(base, max_pages=3, rate_limit_sec=0))
        assert len(pages) == 3

    def test_uses_ampersand_when_query_present(self):
        base = "http://x/leading?foo=bar"
        mapping = {f"{base}&page=1": _NONEMPTY, f"{base}&page=2": ""}
        s = _MappingScraper(mapping)
        pages = asyncio.run(s.scrape_paginated(base, max_pages=5, rate_limit_sec=0))
        assert len(pages) == 1
        assert s.calls[0] == f"{base}&page=1"

    def test_custom_is_empty(self):
        base = "http://x/leading"
        mapping = {f"{base}?page={n}": "NODATA" for n in range(1, 5)}
        s = _MappingScraper(mapping)
        pages = asyncio.run(
            s.scrape_paginated(base, max_pages=5, rate_limit_sec=0, is_empty=lambda h: "NODATA" in h)
        )
        assert pages == []


class TestSyncBridges:
    def test_fetch_sync(self):
        s = _MappingScraper({"u": "HTML"})
        assert s.fetch_sync("u") == "HTML"

    def test_fetch_many_sync(self):
        s = _MappingScraper({"a": "A", "b": "B"})
        assert s.fetch_many_sync(["a", "b"]) == ["A", "B"]


class TestLooksEmpty:
    def test_none_is_empty(self):
        assert _looks_empty(None) is True

    def test_short_is_empty(self):
        assert _looks_empty("<html></html>") is True

    def test_no_table_is_empty(self):
        assert _looks_empty("<html>" + "x" * 300 + "</html>") is True

    def test_with_table_not_empty(self):
        assert _looks_empty(_NONEMPTY) is False


# ---------------------------------------------------------------------------
# PlaywrightScraper（playwright.async_api スタブ）
# ---------------------------------------------------------------------------

class _StubPage:
    def __init__(self, html):
        self._html = html
        self.goto_args = None
        self.waited_selector = None
        self.closed = False

    async def goto(self, url, wait_until=None, timeout=None):
        self.goto_args = (url, wait_until, timeout)

    async def wait_for_selector(self, selector, state=None, timeout=None):
        self.waited_selector = selector

    async def content(self):
        return self._html

    async def close(self):
        self.closed = True


class _StubBrowser:
    def __init__(self, html):
        self._html = html
        self.closed = False
        self.pages: list = []

    async def new_page(self, user_agent=None):
        p = _StubPage(self._html)
        self.pages.append(p)
        return p

    async def close(self):
        self.closed = True


class _StubChromium:
    def __init__(self, html):
        self._html = html
        self.launch_kwargs = None
        self.browser = None

    async def launch(self, headless=True, args=None):
        self.launch_kwargs = {"headless": headless, "args": args}
        self.browser = _StubBrowser(self._html)
        return self.browser


class _StubPW:
    def __init__(self, html):
        self.chromium = _StubChromium(html)
        self.stopped = False

    async def stop(self):
        self.stopped = True


class _StubAsyncPlaywrightCM:
    def __init__(self, html):
        self._html = html
        self.instance = None

    async def start(self):
        self.instance = _StubPW(self._html)
        return self.instance


def _install_playwright_stub(html=_NONEMPTY):
    holder = {}

    def _async_playwright():
        cm = _StubAsyncPlaywrightCM(html)
        holder["cm"] = cm
        return cm

    pw_pkg = types.ModuleType("playwright")
    pw_async = types.ModuleType("playwright.async_api")
    pw_async.async_playwright = _async_playwright
    sys.modules["playwright"] = pw_pkg
    sys.modules["playwright.async_api"] = pw_async
    return holder


@pytest.fixture
def _pw_stub():
    saved = {k: sys.modules.get(k) for k in ("playwright", "playwright.async_api")}
    holder = _install_playwright_stub()
    yield holder
    for k, v in saved.items():
        if v is None:
            sys.modules.pop(k, None)
        else:
            sys.modules[k] = v


class TestPlaywrightScraper:
    def test_fetch_returns_content(self, _pw_stub):
        s = PlaywrightScraper()
        html = asyncio.run(s.fetch("http://x"))
        assert html == _NONEMPTY

    def test_fetch_passes_wait_selector(self, _pw_stub):
        s = PlaywrightScraper()

        async def run():
            await s._start()
            page_html = await s.fetch("http://x", wait_selector=".Odds")
            browser = s._browser
            await s._stop()
            return browser

        browser = asyncio.run(run())
        # last created page recorded the selector
        assert browser.pages[-1].waited_selector == ".Odds"

    def test_fetch_uses_domcontentloaded(self, _pw_stub):
        s = PlaywrightScraper()

        async def run():
            await s._start()
            await s.fetch("http://x")
            page = s._browser.pages[-1]
            await s._stop()
            return page

        page = asyncio.run(run())
        assert page.goto_args[1] == "domcontentloaded"

    def test_lifecycle_closes_browser_when_owned(self, _pw_stub):
        s = PlaywrightScraper()
        # fetch with no prior _start → owns lifecycle → browser closed after
        asyncio.run(s.fetch("http://x"))
        # after fetch, browser/playwright cleaned up
        assert s._browser is None
        assert s._playwright is None

    def test_context_manager(self, _pw_stub):
        async def run():
            async with PlaywrightScraper() as s:
                html = await s.fetch("http://x")
                assert s._browser is not None  # alive within context
                return html, s

        html, scraper = asyncio.run(run())
        assert html == _NONEMPTY
        assert scraper._browser is None  # closed on exit

    def test_launch_headless_and_no_sandbox(self, _pw_stub):
        s = PlaywrightScraper(headless=True)

        async def run():
            await s._start()
            chromium = s._playwright.chromium
            await s._stop()
            return chromium

        chromium = asyncio.run(run())
        assert chromium.launch_kwargs["headless"] is True
        assert "--no-sandbox" in chromium.launch_kwargs["args"]

    def test_fetch_sync(self, _pw_stub):
        s = PlaywrightScraper()
        assert s.fetch_sync("http://x") == _NONEMPTY

    def test_fetch_many_single_browser(self, _pw_stub):
        s = PlaywrightScraper(rate_limit_sec=0)
        result = asyncio.run(s.fetch_many(["http://a", "http://b"]))
        assert result == [_NONEMPTY, _NONEMPTY]
        assert s._browser is None  # cleaned up after
