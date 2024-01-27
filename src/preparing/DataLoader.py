# import shutil
import os
import re

# import datetime
import time

# import urllib.request
from urllib.request import urlopen

import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from tqdm.auto import tqdm

from src.constants import _url_paths
from src.preparing.prepare_chrome_driver import prepare_chrome_driver


class CustomDataLoader:
    def __init__(
        self,
        data_type="URL",
        from_location="https://db.netkeiba.com/race/",
        to_location="./data/html/",
        load_file_name="kaisai_date_list.csv",
        save_file_name="kaisai_date_list.csv",
        rerun=False,
    ):
        self.data_type = data_type
        self.from_location = from_location
        self.to_location = to_location
        self.load_file_name = load_file_name
        self.save_file_name = save_file_name
        self.from_path = os.path.dirname(self.from_location)
        self.to_path = os.path.dirname(self.to_location)
        self.load_file_path = os.path.join(self.from_path, self.load_file_name)
        self.save_file_path = os.path.join(self.to_path, self.save_file_name)
        self.rerun = rerun

    def select_data_source(self):
        if self.from_location.startswith("http://") or self.from_location.startswith("https://"):
            self.scrape_kaisai_date("2020-01-01", "2021-01-01")
        elif os.path.isdir(self.from_location):
            self.load_data_from_local()
        else:
            raise ValueError("Unsupported data location")

    # def load_data_from_url(self):
    #    try:
    #        with urllib.request.urlopen(self.from_location) as response:
    #            self.data = response.read()
    #    except Exception as e:
    #        print(RuntimeError(f"Failed to load data from URL: {e}"))

    def scrape_kaisai_date(self, from_: str, to_: str):
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
            url = _url_paths.UrlPaths.CALENDAR_URL + "?" + "&".join(query)
            html = urlopen(url).read()
            time.sleep(1)
            soup = BeautifulSoup(html, "html.parser")
            a_list = soup.find("table", class_="Calendar_Table").find_all("a")
            for a in a_list:
                kaisai_date_list.append(re.findall(r"(?<=kaisai_date=)\d+", a["href"])[0])
            self.save_data(kaisai_date_list)
        return kaisai_date_list

    def scrape_race_id_list(kaisai_date_list: list, waiting_time=10):
        """
        開催日をyyyymmddの文字列形式でリストで入れると、レースid一覧が返ってくる関数。
        ChromeDriverは要素を取得し終わらないうちに先に進んでしまうことがあるので、
        要素が見つかるまで(ロードされるまで)の待機時間をwaiting_timeで指定。
        """
        race_id_list = []
        driver = prepare_chrome_driver()
        # 取得し終わらないうちに先に進んでしまうのを防ぐため、暗黙的な待機（デフォルト10秒）
        driver.implicitly_wait(waiting_time)
        max_attempt = 2
        print("getting race_id_list")
        for kaisai_date in tqdm(kaisai_date_list):
            try:
                query = ["kaisai_date=" + str(kaisai_date)]
                url = _url_paths.UrlPaths.RACE_LIST_URL + "?" + "&".join(query)
                print("scraping: {}".format(url))
                driver.get(url)

                for i in range(1, max_attempt):
                    try:
                        a_list = driver.find_element(By.CLASS_NAME, "RaceList_Box").find_elements(By.TAG_NAME, "a")
                        break
                    except Exception as e:
                        # 取得できない場合は、リトライを実施
                        print(f"error:{e} retry:{i}/{max_attempt} waiting more {waiting_time} seconds")

                for a in a_list:
                    race_id = re.findall(
                        r"(?<=shutuba.html\?race_id=)\d+|(?<=result.html\?race_id=)\d+",
                        a.get_attribute("href"),
                    )
                    if len(race_id) > 0:
                        race_id_list.append(race_id[0])
            except Exception as e:
                print(e)
                break

        driver.close()
        driver.quit()
        return race_id_list


"""
    def load_data_from_local(self):
        if not os.path.exists(self.save_file_path):
            try:
                os.mkdir(self.to_location)
            except Exception as e:
                print(f"Error creating folder: {e}")

        if not os.path.exists(self.save_file_path):
            try:
                with open(self.save_file_path, "wb") as f:
                    f.write("")

            except Exception as e:
                print(f"Failed to load data from local folder: {e}")

        else:
            with open(self.save_file_path, "a+") as f:
                f.write("")
            print("file loading done")

    def save_data(self, data_to_save):
        if not os.path.exists(self.save_file_path):
            try:
                with open(self.save_file_path, "a+") as f:
                    f.write(data_to_save)

                    print(f"Data saved to: {self.save_file_path}")
            except Exception as e:
                print(f"Failed to save data to local folder: {e}")


# 使用例
from_location = "https://example.com/sample_data.txt"  # ダウンロード対象の外部URL
destination_folder = "./data_folder"  # データを格納するフォルダ

data_loader = CustomDataLoader(from_location, destination_folder)
data_loader.load_data()
data_loader.save_data()
"""
