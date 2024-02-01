import re
import time
from urllib.parse import urlencode
from urllib.request import urlopen

import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
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
        from_local_location="",
        from_local_file_name="",
        target_data=None,
        obtained_last_key="",
        skip=False,
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
            from_local_location,
            from_local_file_name,
            target_data,
            obtained_last_key,
            skip,
        )
        self.target_data = []
        self.from_date = from_date
        self.to_date = to_date

    def scrape_kaisai_date(self):
        if not self.skip:
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
                        self.obtained_last_key = query
                    data_index += 1
            self.save_temp_file("kaisai_date_list")
            self.obtained_last_key = query
            self.transfer_temp_file()
        else:  # 前回終了時のキー項目までskip処理を行う
            pass

    def scrape_race_id_list(self):
        if not self.skip:
            # """

            kaisai_date_list = self.load_file_pkl()

            waiting_time = 10
            race_id_list = []
            driver = prepare_chrome_driver()
            # 取得し終わらないうちに先に進んでしまうのを防ぐため、暗黙的な待機（デフォルト10秒）
            # driver.implicitly_wait(waiting_time)

            data_index = 1
            for kaisai_date in tqdm(kaisai_date_list):
                try:
                    query_params = {"kaisai_date": kaisai_date}
                    query_strings = urlencode(query_params)
                    url = self.from_location + "?" + query_strings

                    # print("url: ", url)
                    # print("scraping: {}".format(url))
                    driver.get(url)
                    wait = WebDriverWait(driver, waiting_time)
                    race_list_box = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "RaceList_Box")))
                    a_list = race_list_box.find_elements(By.TAG_NAME, "a")

                    for a in a_list:
                        race_id = re.findall(
                            r"(?<=shutuba.html\?race_id=)\d+|(?<=result.html\?race_id=)\d+", a.get_attribute("href")
                        )
                        if len(race_id) > 0:
                            race_id_list.append(race_id[0])

                except Exception as e:
                    print(e)
                    break

                if data_index % self.batch_size == 0:
                    self.target_data = race_id_list
                    self.save_temp_file("race_id_list")
                    self.obtained_last_key = kaisai_date
                data_index += 1
            # リストの要素を昇順にソート
            self.target_data = sorted(map(str.strip, self.target_data), key=lambda x: float(x))
            self.save_temp_file("race_id_list")
            self.obtained_last_key = kaisai_date
            self.transfer_temp_file()
            driver.close()
            driver.quit()
