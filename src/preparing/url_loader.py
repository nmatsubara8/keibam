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
        processing_id="",
        obtained_last_key="",
        target_data=None,
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
            processing_id,
            obtained_last_key,
            target_data,
            skip,
        )
        self.target_data = []
        self.from_date = from_date
        self.to_date = to_date

    def scrape_kaisai_date(self):
        # skip対象ではないゴミファイルの掃除
        if not self.skip:
            self.delete_files()

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

    def scrape_race_id_list(self):
        # skip対象ではないゴミファイルの掃除
        if not self.skip:
            self.delete_files()

        kaisai_date_list = self.load_file_pkl()

        waiting_time = 10
        race_id_list = []
        driver = prepare_chrome_driver()
        data_index = 1

        print("scraping race_id_list")
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

                if data_index % self.batch_size == 0:
                    self.target_data = race_id_list
                    self.save_temp_file("race_id_list")
                    self.obtained_last_key = kaisai_date

            except Exception as e:
                print(e)
                break

            data_index += 1
        # リストの要素を昇順にソート
        self.target_data = sorted(map(str.strip, self.target_data), key=lambda x: int(x))
        self.save_temp_file("race_id_list")
        self.obtained_last_key = kaisai_date
        self.transfer_temp_file()
        driver.close()
        driver.quit()

    def scrape_horse_id_list(self):
        """
        当日出走するhorse_id一覧を取得
        """
        if not self.skip:
            self.delete_files()

        race_id_list = self.load_file_pkl()
        # waiting_time = 10
        horse_id_list = []
        # driver = prepare_chrome_driver()
        data_index = 1

        print("scraping horse_id_list")

        for race_id in tqdm(race_id_list):
            # wait = WebDriverWait(driver, waiting_time)
            try:
                url = self.from_location + race_id
                html = urlopen(url)
                soup = BeautifulSoup(html, "lxml")
                time.sleep(1)
                horse_td_list = soup.find_all("td", attrs={"class": "txt_l"})
                horse_ids = [a["href"] for td in horse_td_list if (a := td.find("a")) and "/horse/" in a["href"]]
                horse_id = [re.search(r"/horse/(\d+)/", href).group(1) for href in horse_ids]
                horse_id_list.append(horse_id)
                if data_index % self.batch_size == 0:
                    self.target_data = [item for sublist in horse_id_list for item in sublist]
                    self.save_temp_file("horse_id_list")
                    self.obtained_last_key = race_id

            except Exception as e:
                print(e)
                break

            data_index += 1
        # リストの要素を昇順にソート
        self.target_data = [item for sublist in horse_id_list for item in sublist]
        self.target_data = sorted(set(horse_id_list))
        self.save_temp_file("horse_id_list")
        self.obtained_last_key = race_id
        self.transfer_temp_file()

    def scrape_html_race(self):
        """
        netkeiba.comのraceページのhtmlをスクレイピングしてdata/html/raceに保存する関数。
        skip=Trueにすると、すでにhtmlが存在する場合はスキップされ、Falseにすると上書きされる。
        返り値：新しくスクレイピングしたhtmlのファイルパス
        """
        # skip対象ではないゴミファイルの掃除
        if not self.skip:
            self.delete_files()

        race_id_list = self.load_file_pkl()

        data_index = 1
        for race_id in tqdm(race_id_list):
            try:
                self.processing_id = race_id
                # race_idからurlを作る
                url = self.from_location + race_id

                # 相手サーバーに負担をかけないように1秒待機する
                time.sleep(1)
                # スクレイピング実行
                self.target_data = urlopen(url).read()
                # 保存するファイルパスを指定

                if data_index % self.batch_size == 0:
                    self.save_temp_file("race_html")
                    self.obtained_last_key = race_id
                data_index += 1
            except Exception as e:
                print("Error at {}: {}".format(race_id, e))
        self.save_temp_file("race_html")
        self.obtained_last_key = race_id
        self.copy_files()

    def scrape_html_horse(self):
        """
        netkeiba.comのhorseページのhtmlをスクレイピングしてdata/html/horseに保存する関数。
        skip=Trueにすると、すでにhtmlが存在する場合はスキップされ、Falseにすると上書きされる。
        返り値：新しくスクレイピングしたhtmlのファイルパス
        """
        # skip対象ではないゴミファイルの掃除
        if not self.skip:
            self.delete_files()

        horse_id_list = self.load_file_pkl()
        print("horse_id_list", horse_id_list)
        for horse_id in tqdm(horse_id_list):
            try:
                # horse_idからurlを作る
                url = self.from_location + horse_id
                # 相手サーバーに負担をかけないように1秒待機する
                time.sleep(1)
                # スクレイピング実行
                self.target_data = urlopen(url).read()
                # 保存するファイルパスを指定

                self.processing_id = horse_id
                self.save_temp_file("horse_html")
                self.obtained_last_key = horse_id

            except Exception as e:
                print("Error at {}: {}".format(horse_id, e))

        self.obtained_last_key = horse_id
        self.copy_files()
