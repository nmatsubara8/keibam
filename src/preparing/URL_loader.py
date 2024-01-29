import os
import re
import time
from urllib.request import urlopen

import pandas as pd
from bs4 import BeautifulSoup
from tqdm.auto import tqdm

from src.preparing.DataLoader import DataLoader


class KaisaiDateLoader(DataLoader):
    def __init__(
        self,
        alias,
        from_location,
        to_temp_location,
        temp_save_file_name,
        to_location,
        save_file_name,
        rerun=False,
        from_date="2020-01-01",
        to_date="2021-01-01",
        batch_size=100,
    ):
        super().__init__(
            alias, from_location, to_temp_location, temp_save_file_name, to_location, save_file_name, rerun, batch_size
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

            for year, month in tqdm(zip(date_range.year, date_range.month), total=len(date_range)):
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
                self.save_temp_file(kaisai_date_list, "txt")
            return kaisai_date_list

    def get_kaisai_date_list(self):
        pass

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
