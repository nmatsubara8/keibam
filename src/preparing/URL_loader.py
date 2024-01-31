import os
import re
import time
from urllib.request import urlopen

import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from tqdm import tqdm

from src.preparing.DataLoader import DataLoader
from src.preparing.prepare_chrome_driver import prepare_chrome_driver


class KaisaiDateLoader(DataLoader):
    def __init__(
        self,
        alias="",
        from_location="",
        to_temp_location="",
        temp_save_file_name="",
        to_location="",
        save_file_name="",
        batch_size="",
        target_data=None,
        rerun=False,
        from_date="2020-01-01",
        to_date="2021-01-01",
    ):
        super().__init__(
            alias,
            from_location,
            to_temp_location,
            temp_save_file_name,
            to_location,
            save_file_name,
            batch_size,
            # target_data,
            rerun,
        )
        self.from_date = from_date
        self.to_date = to_date

    def scrape_kaisai_date(self):
        if not self.rerun:
            # self.clear_temp_to_location()
            # yyyy-mmの形式でfrom_とto_を指定すると、間のレース開催日一覧が返ってくる関数。
            # to_の月は含まないので注意。
            print("getting race date from {} to {}".format(self.from_date, self.to_date))
            # 間の年月一覧を作成
            date_range = pd.date_range(start=self.from_date, end=self.to_date, freq="ME")
            # 開催日一覧を入れるリスト
            kaisai_date_list = []
            data_index = 1
            for year, month in tqdm(zip(date_range.year, date_range.month), total=len(date_range), dynamic_ncols=True):
                # 取得したdate_rangeから、スクレイピング対象urlを作成する。
                # urlは例えば、https://race.netkeiba.com/top/calendar.html?year=2022&month=7 のような構造になっている。
                query = [
                    "year=" + str(year),
                    "month=" + str(month),
                ]
                url = self.from_location + "?" + "&".join(query)
                html = urlopen(url).read()
                time.sleep(1)
                soup = BeautifulSoup(html, "lxml")
                a_list = soup.find("table", class_="Calendar_Table").find_all("a")
                for a in a_list:
                    kaisai_date_list.append(re.findall(r"(?<=kaisai_date=)\d+", a["href"])[0])
                    if data_index % self.batch_size == 0:
                        self.target_data = kaisai_date_list
                        self.save_temp_file("kaisai_date_list")
                    data_index += 1
            self.save_temp_file("kaisai_date_list")
            self.transfer_temp_file()
            return self.target_data

    def scrape_race_id_date(self, kaisai_date_list):
        if not self.rerun:
            # """
            # 開催日をyyyymmddの文字列形式でリストで入れると、レースid一覧が返ってくる関数。
            # ChromeDriverは要素を取得し終わらないうちに先に進んでしまうことがあるので、
            # 要素が見つかるまで(ロードされるまで)の待機時間をwaiting_timeで指定。
            # """
            waiting_time = 10
            race_id_list = []
            driver = prepare_chrome_driver()
            # 取得し終わらないうちに先に進んでしまうのを防ぐため、暗黙的な待機（デフォルト10秒）
            driver.implicitly_wait(waiting_time)
            max_attempt = 2
            print("getting race_id_list")
            for kaisai_date in tqdm(kaisai_date_list):
                try:
                    query = ["kaisai_date=" + str(kaisai_date)]
                    url = self.from_location + "?" + "&".join(query)
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
                            r"(?<=shutuba.html\?race_id=)\d+|(?<=result.html\?race_id=)\d+", a.get_attribute("href")
                        )
                        if len(race_id) > 0:
                            race_id_list.append(race_id[0])
                except Exception as e:
                    print(e)
                    break

            driver.close()
            driver.quit()
        return race_id_list

    def get_number_of_data(self):
        if self.alias == "kaisai_date_list":
            date_range = pd.date_range(start=self.from_date, end=self.to_date, freq="ME")
            num_of_data = len(date_range)
            return num_of_data

        elif self.alias == "race_results_list":
            pass
        elif self.alias == "horse_results_list":
            pass
        elif self.alias == "horse_list":
            pass
        elif self.alias == "pede_list":
            pass
        elif self.alias == "race_results_list":
            pass
        elif self.alias == "shutuba_table":
            pass
        elif self.alias == "race_list":
            pass

    def clear_temp_to_location(self):
        # フォルダ内のファイルを全て取得
        file_list = os.listdir(self.to_temp_location)

        # フォルダ内の各ファイルを削除
        for file_name in file_list:
            file_path = os.path.join(self.to_temp_location, file_name)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
            except Exception as e:
                print(f"Failed to delete {file_path}: {e}")
