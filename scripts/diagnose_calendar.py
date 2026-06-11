"""カレンダースクレイプの診断スクリプト。

指定した年・月について race.netkeiba.com のカレンダーページを取得し、
- 実際に取得した HTML の長さ
- Calendar_Table が存在するか
- 抽出できた kaisai_date リンクの一覧
を出力する。年次取得が 0 件になる原因を切り分けるために使う。

使い方:
    python scripts/diagnose_calendar.py 2008 1
    python scripts/diagnose_calendar.py 2024 1
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from bs4 import BeautifulSoup

from src.preparing._scraper import PlaywrightScraper


def diagnose(year: int, month: int) -> None:
    url = f"https://race.netkeiba.com/top/calendar.html?year={year}&month={month}"
    print(f"URL: {url}")

    driver = PlaywrightScraper()
    driver.open_sync()
    try:
        html = driver.fetch_sync(url)
    finally:
        driver.close_sync()

    print(f"HTML length: {len(html)}")

    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", class_="Calendar_Table")
    print(f"Calendar_Table found: {table is not None}")

    # kaisai_date= を含むリンクを全部探す
    all_links = soup.find_all("a")
    kaisai_links = []
    for a in all_links:
        href = a.get("href", "")
        found = re.findall(r"(?<=kaisai_date=)\d+", href)
        if found:
            kaisai_links.append(found[0])

    print(f"total <a> tags: {len(all_links)}")
    print(f"kaisai_date links: {len(kaisai_links)}")
    if kaisai_links:
        print(f"  例: {sorted(set(kaisai_links))[:10]}")

    # title やページの状態を確認
    title = soup.find("title")
    print(f"page title: {title.get_text() if title else '(なし)'}")

    # Calendar_Table 内のリンクも確認
    if table is not None:
        t_links = table.find_all("a")
        print(f"Calendar_Table 内の <a>: {len(t_links)}")
        for a in t_links[:5]:
            print(f"  href={a.get('href', '')!r}")


if __name__ == "__main__":
    year = int(sys.argv[1]) if len(sys.argv) > 1 else 2024
    month = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    diagnose(year, month)
