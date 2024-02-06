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
                print("Error at {}: {}".format(race_id, e))
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
                print("Error at {}: {}".format(race_id, e))
                break

            data_index += 1
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
        # skip対象ではないゴミファイルの掃除
        if not self.skip:
            self.delete_files()

        race_id_list = self.load_file_pkl()

        data_index = 1
        for race_id in tqdm(race_id_list):
            try:
                self.processing_id = race_id
                # print("self.processing_id", self.processing_id)
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

            except Exception as e:
                print("Error at {}: {}".format(race_id, e))

            data_index += 1
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
        data_index = 1
        horse_id_list = self.load_file_pkl()
        # "print("horse_id_list", horse_id_list)
        for horse_id in tqdm(horse_id_list):
            try:
                self.processing_id = horse_id
                # print("self.processing_id", self.processing_id)
                # horse_idからurlを作る
                url = self.from_location + horse_id
                # 相手サーバーに負担をかけないように1秒待機する
                time.sleep(1)
                # スクレイピング実行
                self.target_data = urlopen(url).read()
                # 保存するファイルパスを指定

                if data_index % self.batch_size == 0:
                    self.save_temp_file("horse_html")
                    self.obtained_last_key = horse_id
            except Exception as e:
                print("Error at {}: {}".format(horse_id, e))
            data_index += 1

        self.save_temp_file("horse_html")
        self.obtained_last_key = horse_id
        self.copy_files()

    def create_horse_results_table(self):
        #
        # horseページのhtmlを受け取って、馬の過去成績のDataFrameに変換する関数。
        #
        # skip対象ではないゴミファイルの掃除
        if not self.skip:
            self.delete_files()
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
                    continue

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
        self.copy_files()

    def scrape_html_ped(self):
        """
        netkeiba.comのhorse/pedページのhtmlをスクレイピングしてdata/html/pedに保存する関数。

        """
        if not self.skip:
            self.delete_files()

        horse_id_list = self.load_file_pkl()
        data_index = 1
        for horse_id in tqdm(horse_id_list):
            try:
                self.processing_id = horse_id
                # horse_idからurlを作る
                url = self.from_location + horse_id
                # 相手サーバーに負担をかけないように1秒待機する
                time.sleep(1)
                self.target_data = urlopen(url).read()
                # 保存するファイルパスを指定

                if data_index % self.batch_size == 0:
                    self.save_temp_file("ped_html")
                    self.obtained_last_key = horse_id

            except Exception as e:
                print("Error at {}: {}".format(horse_id, e))

            data_index += 1
        self.save_temp_file("ped_html")
        self.obtained_last_key = horse_id
        self.copy_files()

    def scrape_peds_list(self):
        """
        horse/pedページのhtmlを受け取って、血統のDataFrameに変換する関数。
        """
        # skip対象ではないゴミファイルの掃除
        if not self.skip:
            self.delete_files()

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

            if data_index % self.batch_size == 0:
                self.target_data = peds_df
                self.save_temp_file("peds_list")
                peds = {}
                self.obtained_last_key = ped_html
            data_index += 1
        self.target_data = peds_df
        self.save_temp_file("peds_list")
        self.obtained_last_key = ped_html

    def scrape_schedule(self):
        """
        開催日をyyyymmddの文字列形式で指定すると、レースidとレース時刻の一覧が返ってくる関数。
        ChromeDriverは要素を取得し終わらないうちに先に進んでしまうことがあるので、
        要素が見つかるまで(ロードされるまで)の待機時間をwaiting_timeで指定。
        """
        # skip対象ではないゴミファイルの掃除
        if not self.skip:
            self.delete_files()

        kaisai_date = 20240210
        race_id_list = []
        race_time_list = []
        driver = prepare_chrome_driver()
        waiting_time = 10
        # 取得し終わらないうちに先に進んでしまうのを防ぐため、暗黙的な待機（デフォルト10秒）
        driver.implicitly_wait(waiting_time)
        print("getting race_id_list")

        try:
            query = ["kaisai_date=" + str(kaisai_date)]
            url = self.from_location + "?" + "&".join(query)
            print("scraping: {}".format(url))
            driver.get(url)

            a_list = driver.find_element(By.CLASS_NAME, "RaceList_Box").find_elements(By.TAG_NAME, "a")
            span_list = driver.find_element(By.CLASS_NAME, "RaceList_Box")

            time.sleep(1)

            for a in a_list:
                race_id = re.findall(
                    r"(?<=shutuba.html\?race_id=)\d+|(?<=result.html\?race_id=)\d+", a.get_attribute("href")
                )
                if len(race_id) > 0:
                    race_id_list.append(race_id[0].split(" "))

            for item in span_list.text.split("\n"):
                if ":" in item:
                    race_time_list.append(item.split(" ")[0])

        except Exception as e:
            print("Error at {}: {}".format(kaisai_date, e))
        finally:
            driver.close()
            driver.quit()
            tentative_list = [f"{race_time} {race_id} " for race_time, race_id in zip(race_time_list, race_id_list)]
            self.target_data = sorted(tentative_list, key=lambda x: datetime.strptime(x.split()[0], "%H:%M").time())
            self.save_temp_file("scheduled_race")
            self.obtained_last_key = kaisai_date
            self.transfer_temp_file()

    # 不要かも

    ############################################################################################################
    def scrape_scheduled_items(self):
        """
        race_htmlを受け取って、レース結果テーブルに変換する関数。
        """
        # skip対象ではないゴミファイルの掃除
        if not self.skip:
            self.delete_files()

        race_html_list = self.get_file_list(self.from_local_location)

        data_index = 1

        print("scraping scheduled items")
        race_results = {}
        for race_html in tqdm(race_html_list):
            race_html_path = os.path.join(self.from_local_location, race_html)
            with open(race_html_path, "rb") as f:
                try:
                    # 保存してあるbinファイルを読み込む
                    html = f.read()

                    # メインとなるレース結果テーブルデータを取得
                    df = pd.read_html(html)[0]

                    # htmlをsoupオブジェクトに変換
                    soup = BeautifulSoup(html, "lxml")
                    # 馬IDをスクレイピング
                    horse_id_list = []
                    horse_a_list = soup.find("table", attrs={"summary": "レース結果"}).find_all(
                        "a", attrs={"href": re.compile("^/horse")}
                    )
                    for a in horse_a_list:
                        horse_id = re.findall(r"\d+", a["href"])
                        horse_id_list.append(horse_id[0])
                    df["horse_id"] = horse_id_list

                    # 騎手IDをスクレイピング
                    jockey_id_list = []
                    jockey_a_list = soup.find("table", attrs={"summary": "レース結果"}).find_all(
                        "a", attrs={"href": re.compile("^/jockey")}
                    )
                    for a in jockey_a_list:
                        #'jockey/result/recent/'より後ろの英数字(及びアンダーバー)を抽出
                        jockey_id = re.findall(r"jockey/result/recent/(\w*)", a["href"])
                        jockey_id_list.append(jockey_id[0])
                    df["jockey_id"] = jockey_id_list

                    # 調教師IDをスクレイピング
                    trainer_id_list = []
                    trainer_a_list = soup.find("table", attrs={"summary": "レース結果"}).find_all(
                        "a", attrs={"href": re.compile("^/trainer")}
                    )
                    for a in trainer_a_list:
                        #'trainer/result/recent/'より後ろの英数字(及びアンダーバー)を抽出
                        trainer_id = re.findall(r"trainer/result/recent/(\w*)", a["href"])
                        trainer_id_list.append(trainer_id[0])
                    df["trainer_id"] = trainer_id_list

                    # 馬主IDをスクレイピング
                    owner_id_list = []
                    owner_a_list = soup.find("table", attrs={"summary": "レース結果"}).find_all(
                        "a", attrs={"href": re.compile("^/owner")}
                    )
                    for a in owner_a_list:
                        #'owner/result/recent/'より後ろの英数字(及びアンダーバー)を抽出
                        owner_id = re.findall(r"owner/result/recent/(\w*)", a["href"])
                        owner_id_list.append(owner_id[0])
                    df["owner_id"] = owner_id_list

                    # インデックスをrace_idにする
                    race_id = re.findall(r"race\(\d+).bin", race_html_path)[0]
                    df.index = [race_id] * len(df)
                    if not df.empty:
                        race_results[race_id] = df

                    # for key in race_results:
                    #    print("Columns for race_id {}: {}".format(key, race_results[key].columns))
                    data_index += 1
                    self.obtained_last_key = race_html
                except Exception as e:
                    print("Error at {}: {}".format(race_html_path, e))

        # pd.DataFrame型にして一つのデータにまとめる
        race_results_df = pd.concat([race_results[key] for key in race_results])

        # 列名に半角スペースがあれば除去する
        self.target_data = race_results_df.rename(columns=lambda x: x.replace(" ", ""))
        self.save_temp_file("race_results_table")
        self.copy_files()

    # この関数はまだ（不要かも）
    def scrape_scheduled_horse(self):
        """
        当日出走するhorse_id一覧を取得
        """
        if not self.skip:
            self.delete_files()

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

    def scrape_scheduled_race_html(self):
        """
        netkeiba.comのraceページのhtmlをスクレイピングしてdata/html/raceに保存する関数。
        skip=Trueにすると、すでにhtmlが存在する場合はスキップされ、Falseにすると上書きされる。
        返り値：新しくスクレイピングしたhtmlのファイルパス
        """
        # skip対象ではないゴミファイルの掃除
        if not self.skip:
            self.delete_files()

        race_id_list = self.load_file_pkl()
        driver = prepare_chrome_driver()

        for race_id_time in tqdm(race_id_list):
            try:
                race_id = race_id_time[:12]
                self.processing_id = race_id

                query = ["?race_id=" + str(race_id)]

                url = self.from_location + query[0]
                # print("url", url)
                # print("scraping: {}".format(url))
                driver.get(url)
                # スクレイピング実行
                self.target_data = urlopen(url).read()
                # 保存するファイルパスを指定
                # print("self.target_data", self.target_data)

                self.save_temp_file("scheduled_items")
                self.obtained_last_key = race_id

            except Exception as e:
                print("Error at {}: {}".format(race_id, e))
        self.copy_files()
