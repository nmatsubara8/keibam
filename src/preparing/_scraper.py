"""スクレイピングの抽象境界と Playwright 実装（§4 全面移行）。

設計方針（依存性逆転・副作用の隔離）:
- `AbstractScraper` がドメイン側の契約（URL→HTML 文字列）を定義する。上位の取得処理は
  この抽象にのみ依存し、Selenium/Playwright いずれの実装にも縛られない。
- `PlaywrightScraper` は `playwright.async_api` を**遅延 import**する。未インストール環境でも
  本モジュールの import は壊れない（CI/単体テストはスタブを注入）。
- 既存パイプラインは同期コード中心のため、async は PlaywrightScraper 内に閉じ込め、
  境界に同期ブリッジ（`fetch_sync` / `fetch_many_sync`）を置く。新規コードは async を直接使う。

Playwright 採用理由（KB shards 22-24 / batch001 / 00context）:
- 非同期ネイティブ（asyncio.gather で段階オッズの並列取得が容易）
- Linux VPS でのヘッドレス安定性が高い
- `domcontentloaded` 待機で効率化、`wait_for_selector` で JS 描画完了を確実化
"""

from __future__ import annotations

import asyncio
from abc import ABC
from abc import abstractmethod
from typing import Callable
from typing import Sequence


class AbstractScraper(ABC):
    """URL から HTML 文字列を取得する契約（同期ブリッジ込み）。"""

    @abstractmethod
    async def fetch(
        self,
        url: str,
        *,
        wait_selector: str | None = None,
        wait_until: str = "domcontentloaded",
    ) -> str:
        """単一 URL の HTML を取得する。

        wait_selector を指定すると、その要素が現れるまで待ってから HTML を返す
        （JS 描画されるオッズ・出馬表ページ向け）。
        """
        raise NotImplementedError

    async def fetch_many(
        self,
        urls: Sequence[str],
        *,
        wait_selector: str | None = None,
        wait_until: str = "domcontentloaded",
    ) -> list[str]:
        """複数 URL を順次取得する（既定実装）。実装側で並列化してもよい。"""
        results: list[str] = []
        for url in urls:
            results.append(await self.fetch(url, wait_selector=wait_selector, wait_until=wait_until))
        return results

    async def scrape_paginated(
        self,
        base_url: str,
        *,
        page_param: str = "page",
        max_pages: int = 10,
        is_empty: Callable[[str], bool] | None = None,
        rate_limit_sec: float = 1.0,
    ) -> list[str]:
        """複数ページに渡るテーブル（騎手・調教師リーディング等）を取得する（§4 pagination）。

        base_url 末尾に `?{page_param}={n}`（既存クエリがあれば `&`）を付けて 1..max_pages を
        順に取得する。空ページ（is_empty が True）を検出したら即 break して過剰リクエストを防ぐ。
        各ページ取得間に rate_limit_sec の asyncio.sleep を挟む（負荷軽減）。
        """
        if is_empty is None:
            is_empty = _looks_empty
        sep = "&" if "?" in base_url else "?"
        pages: list[str] = []
        for n in range(1, max_pages + 1):
            url = f"{base_url}{sep}{page_param}={n}"
            html = await self.fetch(url)
            if is_empty(html):
                break
            pages.append(html)
            if rate_limit_sec > 0:
                await asyncio.sleep(rate_limit_sec)
        return pages

    # ------------------------------------------------------------------
    # 同期ブリッジ（既存の同期パイプラインから呼ぶ境界）
    # ------------------------------------------------------------------

    def fetch_sync(self, url: str, **kwargs) -> str:
        """fetch を同期的に実行する（asyncio.run 境界）。"""
        return asyncio.run(self.fetch(url, **kwargs))

    def fetch_many_sync(self, urls: Sequence[str], **kwargs) -> list[str]:
        """fetch_many を同期的に実行する（asyncio.run 境界）。"""
        return asyncio.run(self.fetch_many(urls, **kwargs))


def _looks_empty(html: str) -> bool:
    """ページが実質空（テーブル無し）かの既定判定。

    HTML が極端に短い、もしくは `<table` を含まない場合に空とみなす。
    DOM 構造に依存しすぎないラフな既定。必要なら呼び出し側で is_empty を差し替える。
    """
    if html is None:
        return True
    text = html.strip()
    if len(text) < 200:
        return True
    return "<table" not in text.lower()


class PlaywrightScraper(AbstractScraper):
    """Playwright（Chromium ヘッドレス）による AbstractScraper 実装。

    `playwright.async_api` は遅延 import。`async with PlaywrightScraper() as s:` で
    ブラウザのライフサイクルを管理する。単発の同期呼び出し（fetch_sync）では、
    呼び出しごとにブラウザを起動・終了する。

    Parameters
    ----------
    headless : ヘッドレス起動（VPS 既定 True）。
    timeout_ms : ページ遷移・要素待機のタイムアウト（ミリ秒）。
    rate_limit_sec : fetch_many の各取得間に挟むスリープ秒（負荷軽減）。
    click_delay_ms : クリック時の待機（KB batch001「クリック待機 delay」）。
    """

    def __init__(
        self,
        headless: bool = True,
        timeout_ms: int = 30000,
        rate_limit_sec: float = 1.0,
        click_delay_ms: int = 100,
    ) -> None:
        self._headless = headless
        self._timeout_ms = timeout_ms
        self._rate_limit_sec = rate_limit_sec
        self._click_delay_ms = click_delay_ms
        self._playwright = None
        self._browser = None

    async def __aenter__(self) -> "PlaywrightScraper":
        await self._start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self._stop()

    async def _start(self) -> None:
        from playwright.async_api import async_playwright  # 遅延 import

        pw = await async_playwright().start()
        self._playwright = pw
        self._browser = await pw.chromium.launch(headless=self._headless, args=["--no-sandbox"])

    async def _stop(self) -> None:
        if self._browser is not None:
            await self._browser.close()
            self._browser = None
        if self._playwright is not None:
            await self._playwright.stop()
            self._playwright = None

    async def fetch(
        self,
        url: str,
        *,
        wait_selector: str | None = None,
        wait_until: str = "domcontentloaded",
    ) -> str:
        own_lifecycle = self._browser is None
        if own_lifecycle:
            await self._start()
        try:
            assert self._browser is not None  # _start() で必ず設定済み（型ナローイング）
            page = await self._browser.new_page()
            try:
                await page.goto(url, wait_until=wait_until, timeout=self._timeout_ms)
                if wait_selector is not None:
                    await page.wait_for_selector(wait_selector, timeout=self._timeout_ms)
                return await page.content()
            finally:
                await page.close()
        finally:
            if own_lifecycle:
                await self._stop()

    async def fetch_many(
        self,
        urls: Sequence[str],
        *,
        wait_selector: str | None = None,
        wait_until: str = "domcontentloaded",
    ) -> list[str]:
        """ブラウザを 1 度だけ起動し、各 URL を順次取得する（レート制限付き）。"""
        own_lifecycle = self._browser is None
        if own_lifecycle:
            await self._start()
        try:
            results: list[str] = []
            for i, url in enumerate(urls):
                results.append(
                    await self.fetch(url, wait_selector=wait_selector, wait_until=wait_until)
                )
                if self._rate_limit_sec > 0 and i < len(urls) - 1:
                    await asyncio.sleep(self._rate_limit_sec)
            return results
        finally:
            if own_lifecycle:
                await self._stop()
