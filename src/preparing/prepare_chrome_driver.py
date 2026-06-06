"""スクレイパー生成ヘルパー。

§4 Playwright 全面移行により、推奨は `prepare_scraper()`（Playwright）。
`prepare_chrome_driver()` は selenium 依存の旧実装で**非推奨**。selenium は任意依存に
降格したため、未インストール環境では呼び出し時に ImportError になる（import 時は安全）。
"""


def prepare_scraper(headless: bool = True):
    """Playwright ベースの AbstractScraper を生成する（推奨）。

    既存の同期パイプラインは `scraper.fetch_sync(url)` 境界経由で利用できる。
    """
    from src.preparing._scraper import PlaywrightScraper

    return PlaywrightScraper(headless=headless)


def prepare_chrome_driver():
    """[非推奨] selenium WebDriver を生成する旧実装。

    Playwright 移行により新規コードでは `prepare_scraper()` を使うこと。
    selenium / webdriver-manager は遅延 import（未インストールでもモジュール読込は壊れない）。
    """
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.set_window_size(50, 50)
    return driver
