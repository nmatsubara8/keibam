import re
import time
from urllib.request import urlopen

import pandas as pd
from bs4 import BeautifulSoup
from tqdm.auto import tqdm

from modules.constants import UrlPaths


def scrape_kaisai_date(from_: str, to_: str):
    """
    yyyy-mmの形式でfrom_とto_を指定すると、間のレース開催日一覧が返ってくる関数。
    to_の月は含まないので注意。
    """

    print("getting race date from {} to {}".format(from_, to_))
    # 間の年月一覧を作成
    date_range = pd.date_range(start=from_, end=to_, freq="ME")
    # 開催日一覧を入れるリスト
    kaisai_date_list = []
    for year, month in tqdm(zip(date_range.year, date_range.month), total=len(date_range)):
        # 取得したdate_rangeから、スクレイピング対象urlを作成する。
        # urlは例えば、https://race.netkeiba.com/top/calendar.html?year=2022&month=7 のような構造になっている。
        query = [
            "year=" + str(year),
            "month=" + str(month),
        ]
        url = UrlPaths.CALENDAR_URL + "?" + "&".join(query)
        html = urlopen(url).read()
        time.sleep(1)
        soup = BeautifulSoup(html, "html.parser")
        a_list = soup.find("table", class_="Calendar_Table").find_all("a")
        for a in a_list:
            kaisai_date_list.append(re.findall(r"(?<=kaisai_date=)\d+", a["href"])[0])
    return kaisai_date_list


def existing_check(from_: str, to_: str):
    pass
