import datetime
import os
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

from src.constants._master import Master
from src.constants._results_cols import ResultsCols as Cols
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
            from_date,
            to_date,
        )
        self.target_data = []

    def scrape_kaisai_date(self):
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
        kaisai_date_list = self.load_file_pkl()

        waiting_time = 10
        driver = prepare_chrome_driver()
        data_index = 1
        race_id_list = []
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

            except Exception as e:
                print("Error at {}: {}".format(kaisai_date, e))
                break

            if data_index % self.batch_size == 0:
                self.target_data = race_id_list
                self.save_temp_file("race_id_list")
                race_id_list = []
                self.obtained_last_key = kaisai_date
            data_index += 1

        self.save_temp_file("race_id_list")
        self.obtained_last_key = kaisai_date
        self.transfer_temp_file()
        driver.close()
        driver.quit()

    def scrape_horse_id_list(self):
        """
        当日出走するhorse_id一覧を取得
        """

        race_id_list = self.load_file_pkl()
        waiting_time = 10
        driver = prepare_chrome_driver()
        horse_id_list = []
        # driver = prepare_chrome_driver()
        data_index = 1

        print("scraping horse_id_list")

        for race_id in tqdm(race_id_list):
            try:
                url = self.from_location + race_id
                driver.get(url)
                wait = WebDriverWait(driver, waiting_time)
                wait.until(EC.presence_of_all_elements_located)
                html = urlopen(url)
                time.sleep(1)
                soup = BeautifulSoup(html, "lxml")
                # time.sleep(1)
                horse_td_list = soup.find_all("td", attrs={"class": "txt_l"})
                horse_ids = [a["href"] for td in horse_td_list if (a := td.find("a")) and "/horse/" in a["href"]]
                horse_id = [re.search(r"/horse/(\d+)/", href).group(1) for href in horse_ids]
                horse_id_list.append(horse_id)
                if data_index % self.batch_size == 0:
                    self.target_data = [item for sublist in horse_id_list for item in sublist]
                    self.save_temp_file("horse_id_list")
                    self.obtained_last_key = race_id
                    horse_id_list = []
                data_index += 1
            except Exception as e:
                print("Error at {}: {}".format(race_id, e))
                break

        # リストの要素を昇順にソート
        flat_list = [item for sublist in horse_id_list for item in sublist]
        self.target_data = sorted(set(flat_list))
        self.save_temp_file("horse_id_list")
        self.obtained_last_key = race_id
        self.transfer_temp_file()

    def scrape_html_race(self):
        """
        netkeiba.comのraceページのhtmlをスクレイピングしてdata/html/raceに保存する関数。
        skip=Trueにすると、すでにhtmlが存在する場合はスキップされ、Falseにすると上書きされる。
        返り値：新しくスクレイピングしたhtmlのファイルパス
        """

        race_id_list = self.load_file_pkl()
        driver = prepare_chrome_driver()
        waiting_time = 10

        for race_id in tqdm(race_id_list):
            try:
                self.processing_id = race_id
                # print("self.processing_id", self.processing_id)
                # race_idからurlを作る
                url = self.from_location + race_id
                wait = WebDriverWait(driver, waiting_time)
                wait.until(EC.presence_of_all_elements_located)
                # 相手サーバーに負担をかけないように1秒待機する

                # スクレイピング実行
                self.target_data = urlopen(url).read()
                # 保存するファイルパスを指定
                self.save_temp_file("race_html")
                self.obtained_last_key = race_id

            except Exception as e:
                print("Error at {}: {}".format(race_id, e))
                break
        # ドライバーのクローズ
        driver.quit()
        self.copy_files()

    def scrape_html_horse(self):
        """
        netkeiba.comのhorseページのhtmlをスクレイピングしてdata/html/horseに保存する関数。
        skip=Trueにすると、すでにhtmlが存在する場合はスキップされ、Falseにすると上書きされる。
        返り値：新しくスクレイピングしたhtmlのファイルパス
        """

        waiting_time = 10
        driver = prepare_chrome_driver()
        horse_id_list = self.load_file_pkl()
        # "print("horse_id_list", horse_id_list)
        for horse_id in tqdm(horse_id_list):
            try:
                self.processing_id = horse_id
                # print("self.processing_id", self.processing_id)
                # horse_idからurlを作る
                url = self.from_location + horse_id
                wait = WebDriverWait(driver, waiting_time)
                wait.until(EC.presence_of_all_elements_located)
                # 相手サーバーに負担をかけないように1秒待機する
                time.sleep(1)
                # スクレイピング実行
                self.target_data = urlopen(url).read()
                # 保存するファイルパスを指定

                self.save_temp_file("horse_html")
                self.obtained_last_key = horse_id
            except Exception as e:
                print("Error at {}: {}".format(horse_id, e))
                break
        self.copy_files()

    def create_horse_results_table(self):
        #
        # horseページのhtmlを受け取って、馬の過去成績のDataFrameに変換する関数。
        #

        data_index = 1
        horse_id_file_list = self.get_file_list(self.from_local_location)

        print("scraping horse_results_table")
        horse_results = {}
        for horse_html in tqdm(horse_id_file_list):
            horse_html_path = os.path.join(self.from_local_location, horse_html)
            with open(horse_html_path, "rb") as f:
                try:
                    # 保存してあるbinファイルを読み込む
                    html = f.read()

                    df = pd.read_html(html)[3]
                    # 受賞歴がある馬の場合、3番目に受賞歴テーブルが来るため、4番目のデータを取得する
                    if df.columns[0] == "受賞歴":
                        df = pd.read_html(html)[4]

                    # 新馬の競走馬レビューが付いた場合、
                    # 列名に0が付与されるため、次のhtmlへ飛ばす
                    if df.columns[0] == 0:
                        print("horse_results empty case1 {}".format(horse_html))
                        continue

                    horse_id = re.findall(r"(\d+).bin", horse_html)[0]

                    df.index = [horse_id] * len(df)
                    horse_results[horse_id] = df

                # 競走データが無い場合（新馬）を飛ばす
                except IndexError:
                    print("horse_results empty case2 {}".format(horse_html))
                    continue

                except Exception as e:
                    print("Error at {}: {}".format(horse_html, e))
                    break

            # pd.DataFrame型にして一つのデータにまとめる
            horse_results_df = pd.concat([horse_results[key] for key in horse_results])
            # 列名に半角スペースがあれば除去する

            if data_index % self.batch_size == 0:
                self.target_data = horse_results_df.rename(columns=lambda x: x.replace(" ", ""))
                self.save_temp_file("horse_results_table")
                self.obtained_last_key = horse_id
                horse_results = {}
                df = []
            else:
                data_index += 1
        self.target_data = horse_results_df.rename(columns=lambda x: x.replace(" ", ""))
        self.save_temp_file("horse_results_table")
        self.obtained_last_key = horse_id
        self.transfer_temp_file()

    def scrape_html_ped(self):
        """
        netkeiba.comのhorse/pedページのhtmlをスクレイピングしてdata/html/pedに保存する関数。

        """

        waiting_time = 10
        driver = prepare_chrome_driver()
        horse_id_list = self.load_file_pkl()

        for horse_id in tqdm(horse_id_list):
            try:
                self.processing_id = horse_id
                # horse_idからurlを作る
                url = self.from_location + horse_id
                wait = WebDriverWait(driver, waiting_time)
                wait.until(EC.presence_of_all_elements_located)

                # 相手サーバーに負担をかけないように1秒待機する
                time.sleep(1)
                self.target_data = urlopen(url).read()
                # 保存するファイルパスを指定

                self.save_temp_file("ped_html")
                self.obtained_last_key = horse_id

            except Exception as e:
                print("Error at {}: {}".format(horse_id, e))
                break
        self.copy_files()

    def scrape_peds_list(self):
        """
        horse/pedページのhtmlを受け取って、血統のDataFrameに変換する関数。
        """

        ped_html_list = self.get_file_list(self.from_local_location)

        data_index = 1
        peds = {}
        print("preparing peds table")

        for ped_html in tqdm(ped_html_list):
            ped_html_path = os.path.join(self.from_local_location, ped_html)
            # print("ped_html_path", ped_html_path)
            try:
                with open(ped_html_path, "rb") as f:
                    # 保存してあるbinファイルを読み込む
                    html = f.read()
                    # horse_idを取得
                    horse_id = re.findall(r"(\d+).bin", ped_html)[0]
                    # htmlをsoupオブジェクトに変換
                    soup = BeautifulSoup(html, "lxml")
                    peds_id_list = []
                    # 血統データからhorse_idを取得する
                    horse_a_list = soup.find("table", attrs={"summary": "5代血統表"}).find_all(
                        "a", attrs={"href": re.compile(r"^/horse/\w{10}")}
                    )
                    for a in horse_a_list:
                        # 血統データのhorse_idを抜き出す
                        work_peds_id = re.findall(r"horse\W(\w{10})", a["href"])[0]
                        peds_id_list.append(work_peds_id)
                    peds[horse_id] = peds_id_list

                    # pd.DataFrame型にして一つのデータにまとめて、列と行の入れ替えして、列名をpeds_0, ..., peds_61にする
                    peds_df = pd.DataFrame.from_dict(peds, orient="index").add_prefix("peds_")

            except Exception as e:
                print("Error at {}: {}".format(ped_html, e))
                break
            if data_index % self.batch_size == 0:
                self.target_data = peds_df
                self.save_temp_file("peds_list")
                peds = {}
                self.obtained_last_key = ped_html
            data_index += 1
        self.target_data = peds_df
        self.save_temp_file("peds_list")
        self.obtained_last_key = ped_html
        self.transfer_temp_file()

    def scrape_schedule(self):
        """
        開催日をyyyymmddの文字列形式で指定すると、レースidとレース時刻の一覧が返ってくる関数。
        ChromeDriverは要素を取得し終わらないうちに先に進んでしまうことがあるので、
        要素が見つかるまで(ロードされるまで)の待機時間をwaiting_timeで指定。
        """

        date_range = pd.date_range(start=self.from_date, end=self.to_date, freq="D")
        waiting_time = 10
        # ループの外で一度だけドライバーを生成
        driver = prepare_chrome_driver()
        tentative_store = []
        for kaisai_date in tqdm(date_range):
            race_id_list = []
            race_time_list = []
            tentative_list = []
            # ループごとに初期化
            try:
                kaisai_date_str = str(kaisai_date.strftime("%Y%m%d"))
                # print("kaisai_date_str", kaisai_date_str)
                query = ["kaisai_date=" + kaisai_date_str]

                url = self.from_location + "?" + "&".join(query)
                print("scraping: {}".format(url))
                driver.get(url)
                driver.implicitly_wait(waiting_time)

                a_list = driver.find_element(By.CLASS_NAME, "RaceList_Box").find_elements(By.TAG_NAME, "a")
                span_list = driver.find_element(By.CLASS_NAME, "RaceList_Box")
                for a in a_list:
                    race_id = re.findall(
                        r"(?<=shutuba.html\?race_id=)\d+|(?<=result.html\?race_id=)\d+", a.get_attribute("href")
                    )
                    if len(race_id) > 0:
                        race_id_list.append(race_id[0])

                for item in span_list.text.split("\n"):
                    if ":" in item:
                        race_time_list.append(item.split(" ")[0])

                tentative_list = [
                    (kaisai_date_str, race_time, race_id) for race_time, race_id in zip(race_time_list, race_id_list)
                ]
                tentative_store.extend(tentative_list)
                self.obtained_last_key = kaisai_date

            except Exception as e:
                print(f"Error at {kaisai_date}: {e}")
                break
        tentative_store = sorted(tentative_store, key=lambda x: (x[0], x[1]))
        self.target_data = [f"{race_time},{race_id}" for kaisai_date_str, race_time, race_id in tentative_store]
        self.obtained_last_key = kaisai_date
        self.save_temp_file("scheduled_race")
        # ループの外でドライバーをクローズ
        driver.close()
        driver.quit()
        self.transfer_temp_file()

    def scrape_scheduled_race_html(self):
        """
        netkeiba.comのraceページのhtmlをスクレイピングしてscheduled_raceに保存する関数。

        """

        waiting_time = 10
        time_race_id_list = self.load_file_pkl()
        # 時刻とレースidの組みあわせからレースidだけを抽出
        race_id_list = [element.split(",")[1] for element in time_race_id_list]
        driver = prepare_chrome_driver()

        for race_id in tqdm(race_id_list):
            try:
                self.processing_id = race_id
                query = ["?race_id=" + str(race_id)]
                url = self.from_location + query[0]

                driver.get(url)
                wait = WebDriverWait(driver, waiting_time)
                wait.until(EC.presence_of_all_elements_located)
                # スクレイピング実行
                self.target_data = urlopen(url).read()
                self.save_temp_file("scheduled_race_html")
                self.obtained_last_key = race_id

            except Exception as e:
                print("Error at {}: {}".format(race_id, e))
                break
        self.copy_files()

    ############################################################################################################
    def scrape_latest_info(self):
        """
        当日の出馬表をスクレイピング。
        dateはyyyy/mm/ddの形式。
        """

        time_race_id_list = self.load_file_pkl()
        # 時刻とレースidの組みあわせからレースidだけを抽出
        race_id_list = [element.split(",")[1] for element in time_race_id_list]
        # 取得し終わらないうちに先に進んでしまうのを防ぐため、暗黙的な待機（デフォルト10秒）

        date = self.from_date
        waiting_time = 10
        driver = prepare_chrome_driver()
        df = pd.DataFrame()
        for race_id in tqdm(race_id_list):
            # print("race_id_list", race_id_list)

            # print("race_id", race_id)
            try:
                query = "?race_id=" + race_id
                url = self.from_location + query
                # print("url", url)
                driver.get(url)
                wait = WebDriverWait(driver, waiting_time)
                wait.until(EC.presence_of_all_elements_located)
                # メインのテーブルの取得
                for tr in driver.find_elements(By.CLASS_NAME, "HorseList"):
                    row = []
                    for td in tr.find_elements(By.TAG_NAME, "td"):
                        if td.get_attribute("class") in ["HorseInfo"]:
                            href = td.find_element(By.TAG_NAME, "a").get_attribute("href")
                            row.append(re.findall(r"horse/(\d*)", href)[0])
                        elif td.get_attribute("class") in ["Jockey"]:
                            href = td.find_element(By.TAG_NAME, "a").get_attribute("href")
                            row.append(re.findall(r"jockey/result/recent/(\w*)", href)[0])
                        elif td.get_attribute("class") in ["Trainer"]:
                            href = td.find_element(By.TAG_NAME, "a").get_attribute("href")
                            row.append(re.findall(r"trainer/result/recent/(\w*)", href)[0])
                        row.append(td.text)
                    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)

                # レース結果テーブルと列を揃える
                df = df[[0, 1, 5, 6, 12, 13, 11, 3, 7, 9]]
                df.columns = [
                    Cols.WAKUBAN,
                    Cols.UMABAN,
                    Cols.SEX_AGE,
                    Cols.KINRYO,
                    Cols.TANSHO_ODDS,
                    Cols.POPULARITY,
                    Cols.WEIGHT_AND_DIFF,
                    "horse_id",
                    "jockey_id",
                    "trainer_id",
                ]
                df.index = [race_id] * len(df)

                # レース情報の取得
                texts = driver.find_element(By.CLASS_NAME, "RaceList_Item02").text
                texts = re.findall(r"\w+", texts)
                print("texts", texts)
                # 障害レースフラグを初期化
                hurdle_race_flg = False
                for text in texts:
                    if "m" in text:
                        # 20211212：[0]→[-1]に修正
                        df["course_len"] = [int(re.findall(r"\d+", text)[-1])] * len(df)
                    if text in Master.WEATHER_LIST:
                        df["weather"] = [text] * len(df)
                    if text in Master.GROUND_STATE_LIST:
                        df["ground_state"] = [text] * len(df)
                    if "稍" in text:
                        df["ground_state"] = [Master.GROUND_STATE_LIST[1]] * len(df)
                    if "不" in text:
                        df["ground_state"] = [Master.GROUND_STATE_LIST[3]] * len(df)
                    if "芝" in text:
                        df["race_type"] = [list(Master.RACE_TYPE_DICT.values())[0]] * len(df)
                    if "ダ" in text:
                        df["race_type"] = [list(Master.RACE_TYPE_DICT.values())[1]] * len(df)
                    if "障" in text:
                        df["race_type"] = [list(Master.RACE_TYPE_DICT.values())[2]] * len(df)
                        hurdle_race_flg = True
                    if "右" in text:
                        df["around"] = [Master.AROUND_LIST[0]] * len(df)
                    if "左" in text:
                        df["around"] = [Master.AROUND_LIST[1]] * len(df)
                    if "直線" in text:
                        df["around"] = [Master.AROUND_LIST[2]] * len(df)
                    if "新馬" in text:
                        df["race_class"] = [Master.RACE_CLASS_LIST[0]] * len(df)
                    if "未勝利" in text:
                        df["race_class"] = [Master.RACE_CLASS_LIST[1]] * len(df)
                    if "１勝クラス" in text:
                        df["race_class"] = [Master.RACE_CLASS_LIST[2]] * len(df)
                    if "２勝クラス" in text:
                        df["race_class"] = [Master.RACE_CLASS_LIST[3]] * len(df)
                    if "３勝クラス" in text:
                        df["race_class"] = [Master.RACE_CLASS_LIST[4]] * len(df)
                    if "オープン" in text:
                        df["race_class"] = [Master.RACE_CLASS_LIST[5]] * len(df)

                # グレードレース情報の取得
                if len(driver.find_elements(By.CLASS_NAME, "Icon_GradeType3")) > 0:
                    df["race_class"] = [Master.RACE_CLASS_LIST[6]] * len(df)
                elif len(driver.find_elements(By.CLASS_NAME, "Icon_GradeType2")) > 0:
                    df["race_class"] = [Master.RACE_CLASS_LIST[7]] * len(df)
                elif len(driver.find_elements(By.CLASS_NAME, "Icon_GradeType1")) > 0:
                    df["race_class"] = [Master.RACE_CLASS_LIST[8]] * len(df)

                # 障害レースの場合
                if hurdle_race_flg:
                    df["around"] = [Master.AROUND_LIST[3]] * len(df)
                    df["race_class"] = [Master.RACE_CLASS_LIST[9]] * len(df)

                df["date"] = [date] * len(df)
                print("df", df)
                # 取消された出走馬を削除
                df = df[df[Cols.WEIGHT_AND_DIFF] != "--"]
                self.target_data = df
                self.save_temp_file("tentative_info")
                self.obtained_last_key = race_id
            except Exception as e:
                print("Error at {}: {}".format(race_id, e))
                break
        driver.close()
        driver.quit()

    # この関数はまだ（不要かも）
    def scrape_scheduled_horse(self):
        """
        当日出走するhorse_id一覧を取得
        """

        waiting_time = 10
        driver = prepare_chrome_driver()
        scheduled_race_id_list = self.load_file_pkl()
        print("scheduled_race_id_list ", scheduled_race_id_list)
        scheduled_horse_id_list = []
        # Convert the string list to a list of lists
        # Convert the string list to a list of lists using json
        # Safely evaluate the string as a Python literal
        # parsed_scheduled_race_id_list = ast.literal_eval(scheduled_race_id_list[0][0])

        # Flatten the nested list
        # flat_scheduled_race_id_list = [item[0] for item in parsed_scheduled_race_id_list]

        # print("flat_race_id_list ", flat_scheduled_race_id_list)
        for race_id in tqdm(scheduled_race_id_list):
            scheduled_race_id = race_id[:12]
            # print("scheduled_race_id ", scheduled_race_id)
            query = "?race_id=" + scheduled_race_id

            url = self.from_location + query
            wait = WebDriverWait(driver, waiting_time)
            wait.until(EC.presence_of_all_elements_located)
            time.sleep(1)
            print("url", url)
            html = urlopen(url)
            soup = BeautifulSoup(html, "lxml", from_encoding="utf-8")
            horse_td_list = soup.find_all("td", attrs={"class": "HorseInfo"})
            # print("horse_td_list", horse_td_list)
            for td in horse_td_list:
                scheduled_horse_id = re.findall(r"\d+", td.find("a")["href"])[0]
                scheduled_horse_id_list.append(scheduled_horse_id)
        self.target_data = scheduled_horse_id_list
        self.save_temp_file("scheduled_horse")
        self.obtained_last_key = scheduled_race_id
        self.transfer_temp_file()

    # この関数はまだ

    def create_active_race_id_list(minus_time=-50):
        """
        馬体重の発表されたレースidとレース時刻の一覧が返ってくる関数。
        馬体重の発表時刻は、引数で指定されたminus_timeをレース時刻から引いた時刻で算出します。
        """
        # 現在時刻を取得
        now_date = datetime.datetime.now().date().strftime("%Y%m%d")
        hhmm = datetime.datetime.now().strftime("%H:%M")
        print(now_date, hhmm)

        # レースidとレース時刻の一覧を取得
        race_id_list, race_time_list = scrape_race_id_race_time_list(now_date)

        # 現在時刻マイナス馬体重時刻を取得
        t_delta30 = datetime.timedelta(hours=9, minutes=minus_time)
        JST30 = datetime.timezone(t_delta30, "JST")
        now30 = datetime.datetime.now(JST30)
        hhmm_minus_time = now30.strftime("%H:%M")

        target_race_id_list = []
        target_race_time_list = []
        from_time = "09:15"

        for race_id, race_time in zip(race_id_list, race_time_list):
            # レース時刻より馬体重発表時刻を算出
            dt1 = datetime.datetime(
                int(now_date[:4]), int(now_date[4:6]), int(now_date[6:8]), int(race_time[0:2]), int(race_time[3:5])
            )
            dt2 = dt1 + datetime.timedelta(minutes=minus_time)
            announce_weight_time = dt2.strftime("%H:%M")

            # 1Rの場合は、前回のレース時刻を馬体重発表時刻に設定
            if "01" == race_id_list[10:12]:
                from_time = announce_weight_time

            # 前回のレース時刻 ＜ 現在時刻 ＜ レース時刻
            if from_time < hhmm < race_time:
                target_race_id_list.append(race_id)
                target_race_time_list.append(race_time)
            # 現在時刻マイナス馬体重時刻 ＜ 馬体重発表時刻 ＜＝ 現在時刻
            elif hhmm_minus_time < announce_weight_time <= hhmm:
                target_race_id_list.append(race_id)
                target_race_time_list.append(race_time)
            # 前回のレース時刻を退避
            from_time = race_time

        return target_race_id_list, target_race_time_list
