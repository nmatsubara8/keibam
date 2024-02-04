import datetime
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
        data_index = +1
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
            data_index = +1
        self.obtained_last_key = horse_id
        self.copy_files()

    # この関数はまだ
    def scrape_html_ped(horse_id_list: list, skip: bool = True):
        """
        netkeiba.comのhorse/pedページのhtmlをスクレイピングしてdata/html/pedに保存する関数。
        skip=Trueにすると、すでにhtmlが存在する場合はスキップされ、Falseにすると上書きされる。
        返り値：新しくスクレイピングしたhtmlのファイルパス
        """
        updated_html_path_list = []
        for horse_id in tqdm(horse_id_list):
            # 保存するファイル名
            filename = os.path.join(LocalPaths.HTML_PED_DIR, horse_id + ".bin")
            # skipがTrueで、かつbinファイルがすでに存在する場合は飛ばす
            if skip and os.path.isfile(filename):
                print("horse_id {} skipped".format(horse_id))
            else:
                # horse_idからurlを作る
                url = UrlPaths.PED_URL + horse_id
                # 相手サーバーに負担をかけないように1秒待機する
                time.sleep(1)
                # スクレイピング実行
                html = urlopen(url).read()
                # 保存するファイルパスを指定
                with open(filename, "wb") as f:
                    # 保存
                    f.write(html)
                updated_html_path_list.append(filename)
        return updated_html_path_list

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

    # この関数はまだ
    def scrape_shutuba_table(race_id: str, date: str, file_path: str):
        """
        当日の出馬表をスクレイピング。
        dateはyyyy/mm/ddの形式。
        """
        driver = prepare_chrome_driver()
        # 取得し終わらないうちに先に進んでしまうのを防ぐため、暗黙的な待機（デフォルト10秒）
        driver.implicitly_wait(10)
        query = "?race_id=" + race_id
        url = UrlPaths.SHUTUBA_TABLE + query
        df = pd.DataFrame()
        try:
            driver.get(url)

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
        except Exception as e:
            print(e)
        finally:
            driver.close()
            driver.quit()

        # 取消された出走馬を削除
        df = df[df[Cols.WEIGHT_AND_DIFF] != "--"]
        df.to_pickle(file_path)

    # この関数はまだ
    def scrape_horse_id_list(race_id_list: list) -> list:
        """
        当日出走するhorse_id一覧を取得
        """
        print("sraping horse_id_list")
        horse_id_list = []
        for race_id in tqdm(race_id_list):
            query = "?race_id=" + race_id
            url = UrlPaths.SHUTUBA_TABLE + query
            html = urlopen(url)
            soup = BeautifulSoup(html, "lxml", from_encoding="utf-8")
            horse_td_list = soup.find_all("td", attrs={"class": "HorseInfo"})
            for td in horse_td_list:
                horse_id = re.findall(r"\d+", td.find("a")["href"])[0]
                horse_id_list.append(horse_id)
        return horse_id_list
