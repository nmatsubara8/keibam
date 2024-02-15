import os
import re
import time
from urllib.request import urlopen

import pandas as pd
from bs4 import BeautifulSoup
from numpy import NaN
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from tqdm import tqdm

from src.constants._master import Master
from src.preparing.prepare_chrome_driver import prepare_chrome_driver


def scrape_scheduled_race_html(self, ref_id):
    # 時刻とレースidの組みあわせからレースidだけを抽出
    race_id_list = [element.split(",")[1] for element in time_race_id_list]

    query = ["?race_id=" + str(ref_id)]
    url = self.from_location + query[0]
    return get_soup(url)[0].read()


def create_raw_race_info(target_bin_file_path):
    # print(f"target_bin_file_path:{target_bin_file_path}")
    with open(target_bin_file_path, "rb") as f:
        # 保存してあるbinファイルを読み込む
        html = f.read()

        # htmlをsoupオブジェクトに変換
        soup = BeautifulSoup(html, "lxml")

        # 天候、レースの種類、コースの長さ、馬場の状態、日付、回り、レースクラスをスクレイピング
        texts = (
            soup.find("div", attrs={"class": "data_intro"}).find_all("p")[0].text
            + soup.find("div", attrs={"class": "data_intro"}).find_all("p")[1].text
        )

        info = re.findall(r"\w+", texts)
        print(f"info:{info}")
        df = pd.DataFrame()
        race_id = re.findall(r"\d+", target_bin_file_path)[0]

        # 障害レースフラグを初期化
        hurdle_race_flg = False
        for text in info:
            if text in ["芝", "ダート", "障害"]:
                df["race_type"] = [text]
            # もし、textが任意の文字列＋3桁か4桁の数字+ "m"　（例えば、1200m ）の様に表現されている場合に、
            # その数字部分の文字を抽出し、整数化の上、df["course_len"]に格納する処理をここに入れたい
            # 正規表現パターン
            pattern = r"([0-9]{3})m|([0-9]{4})m"
            # 正規表現に一致する部分を抽出
            matches = re.findall(pattern, text)
            if matches:
                for match in matches:
                    # キャプチャグループから数字部分を取得
                    extracted_number = match[0] if match[0] else match[1]
                df["course_len"] = [int(extracted_number)]

            if "右" in text:
                df["around"] = [Master.AROUND_LIST[0]]
            if "左" in text:
                df["around"] = [Master.AROUND_LIST[1]]
            if "直線" in text:
                df["around"] = [Master.AROUND_LIST[2]]
            if "障害" in text:
                df["around"] = [Master.AROUND_LIST[3]]
                hurdle_race_flg = True

            if text in Master.GROUND_STATE_LIST:
                df["ground_state"] = [text]
            if text in Master.WEATHER_LIST:
                df["weather"] = [text]
            if "年" in text:
                df["date"] = [text]

            if "新馬" in text:
                df["race_class"] = [Master.RACE_CLASS_LIST[0]]
            if "未勝利" in text:
                df["race_class"] = [Master.RACE_CLASS_LIST[1]]
            if ("1勝クラス" in text) or ("500万下" in text):
                df["race_class"] = [Master.RACE_CLASS_LIST[2]]
            if ("2勝クラス" in text) or ("1000万下" in text):
                df["race_class"] = [Master.RACE_CLASS_LIST[3]]
            if ("3勝クラス" in text) or ("1600万下" in text):
                df["race_class"] = [Master.RACE_CLASS_LIST[4]]
            if "オープン" in text:
                df["race_class"] = [Master.RACE_CLASS_LIST[5]]
            if hurdle_race_flg:
                # df["around"] = [Master.AROUND_LIST[3]]
                # df["race_type"] = ["障害"]
                df["race_class"] = [Master.RACE_CLASS_LIST[9]]
                hurdle_race_flg = False
                # 障害レースの場合
        # if hurdle_race_flg:
        # df["around"] = [Master.AROUND_LIST[3]]
        # df["race_class"] = [Master.RACE_CLASS_LIST[9]]
        # hurdle_race_flg = False

        # グレードレース情報の取得
        grade_text = soup.find("div", attrs={"class": "data_intro"}).find_all("h1")[0].text
        if "G3" in grade_text:
            df["race_class"] = [Master.RACE_CLASS_LIST[6]] * len(df)
        elif "G2" in grade_text:
            df["race_class"] = [Master.RACE_CLASS_LIST[7]] * len(df)
        elif "G1" in grade_text:
            df["race_class"] = [Master.RACE_CLASS_LIST[8]] * len(df)

        df["race_id"] = race_id

    return df


def create_tmp_race_info(target_bin_file_path):
    with open(target_bin_file_path, "rb") as f:
        html = f.read()
        soup = BeautifulSoup(html, "lxml")
        df = pd.DataFrame()
        # 天候、レースの種類、コースの長さ、馬場の状態、日付、回り、レースクラスをスクレイピング
        texts = (
            soup.find("div", attrs={"class": "data_intro"}).find_all("p")[0].text
            + soup.find("div", attrs={"class": "data_intro"}).find_all("p")[1].text
        )
        info = re.findall(r"\w+", texts)
        length = len(info)

        # インデックスをrace_idにする
        race_id = re.findall(r"\d+", target_bin_file_path)[0]
        df.index = [race_id] * length
        df["id"] = range(1, length + 1)
        df["info"] = info
        df["race_id"] = race_id
        df["id"] = df["id"].astype(int)

    return df


def storing_process(self):
    df = []
    df_sorted = []
    df_supressed = []
    self_path = os.path.join(self.to_temp_location, self.temp_save_file_name)
    df = pd.read_csv(self_path)
    df_copy = df.copy()
    df_copy.iloc[:, -1] = df_copy.iloc[:, -1].astype(int)
    df_sorted = df_copy.iloc[:, -1].sort_values()
    df_supressed = df_sorted.drop_duplicates()
    self.target_data = df_supressed.reset_index(drop=True)
    self.delete_files_tmp()
    self.save_temp_file(self.alias)
    self.target_data = df
    if self.get_filetype() != "html":
        self.transfer_temp_file()


################################# Done ####################################
def process_pkl_file(self, process_function):
    """
    pklファイルを受け取って、テーブルに変換する関数。
    process_functionを入れ替えながら汎用的に使う共通モジュール
    対象ファイルや処理のバッチサイズなどを読み取り、セットの上、処理する
    """
    if self.alias == "kaisai_date_list":
        # yyyy-mmの形式でfrom_とto_を指定すると、間のレース開催日一覧yyyy-mm-ddが返ってくる関数.to_の月は含まないので注意。
        df = pd.DataFrame({"kaisai_data": []})
        target_all_files = pd.date_range(start=self.from_date, end=self.to_date, freq="ME").astype(str)
    else:
        df = self.load_file_pkl()
        target_all_files = df.iloc[:, 0]

        # print("target_pkl_fileはここから+''")
        print(target_all_files.head())

    total_batches = (len(target_all_files) + self.batch_size - 1) // self.batch_size  # バッチ数の計算
    total_files = len(target_all_files)  # 処理対象の全データ数
    print(f"# of input files: {total_files}")
    print(f"# of total_batches: {total_batches}")
    processed_files = 0  # 処理済みのファイル数
    # print(f"start {self.alias} processing")
    # target_data_name = {}

    # tqdmインスタンスの作成
    pbar = tqdm(total=total_files, desc="Processing Batches", unit="%", unit_scale=True, leave=False)

    # ドライバーのインスタンス化
    driver = prepare_chrome_driver()
    # 取得し終わらないうちに先に進んでしまうのを防ぐため、暗黙的な待機（デフォルト10秒）
    waiting_time = 30
    driver.implicitly_wait(waiting_time)

    for batch_index in range(total_batches):
        start_index = batch_index * self.batch_size
        end_index = min((batch_index + 1) * self.batch_size, len(target_all_files))
        batch_target_all_files = target_all_files[start_index:end_index]
        # print("ref_idの確認:", batch_target_all_files[0:2])
        batch_data = []
        for ref_id in batch_target_all_files:  #
            try:
                # print(f"ref_id:{ref_id}")
                # self.processing_id = ref_id
                return_data = process_function(self, ref_id, driver, waiting_time)

                time.sleep(1)
                batch_data.append(return_data)
            # print(f"temp_df:{temp_df}")
            except Exception as e:
                print("Error at {}: {}:{}".format(processed_files, ref_id, e))
                self.obtained_last_key = ref_id
                break

            processed_files += 1
            pbar.update(1)  # 処理済みのファイル数を1増やす
            # temp_df = pd.concat([temp_df[key] for key in temp_df])
        df = pd.concat(batch_data)

        self.obtained_last_key = ref_id
        # print(f"batch_df:{batch_df}")  # バッチごとのDataFrameを結合
        self.target_data = df
        self.save_temp_file(self.alias)
    storing_process(self)

    print(f"# of processed files: {processed_files}")
    # ドライバーのクローズ
    driver.close()
    driver.quit()


def get_kaisai_date_list(self, ref_id, driver, waiting_time):
    match = re.match(r"^(\d{4})-(\d{2})-\d{2}$", ref_id)
    if match:
        year = match.group(1)
        month = match.group(2)
        print("Year:", year)
        print("Month:", month)
    else:
        print("Invalid date format")

    # 開催日一覧を入れるリスト
    kaisai_date_list = []
    # df = pd.DataFrame(columns=["kaisai_date"])

    # 取得したdate_rangeから、スクレイピング対象urlを作成する。
    # urlは例えば、https://race.netkeiba.com/top/calendar.html?year=2022&month=7 のような構造になっている。
    ref_id = "year=" + str(year) + "&month=" + str(month)
    ref_ym = str(year) + str(month)

    url = str(self.from_location) + "?" + ref_id
    # print(f"url:{url}")
    soup = get_soup(url, driver, waiting_time)
    a_list = soup.find("table", class_="Calendar_Table").find_all("a")
    for a in a_list:
        kaisai_date_list.append(re.findall(r"(?<=kaisai_date=)\d+", a["href"])[0])
    # print(f"kaisai_date_list:{kaisai_date_list}")
    # DataFrameを作成し、インデックスをリセットして整形する
    df = pd.DataFrame({"kaisai_data": kaisai_date_list}, index=[ref_ym] * len(kaisai_date_list))
    # print(f"df:{df}")

    return df


################################# Done ####################################
def get_soup(url, driver, waiting_time):
    driver.get(url)
    wait = WebDriverWait(driver, waiting_time)
    wait.until(EC.presence_of_all_elements_located)
    html = urlopen(url)
    soup = BeautifulSoup(html, "lxml")
    return soup


################################# Done ####################################
def get_raw_horse_id_list(self, ref_id, driver, waiting_time):
    # この例ではtarget=horse ref_id=race_id
    target_id_list = []
    target_ids = []
    target_ids = []

    url = str(self.from_location) + str(ref_id)
    soup = get_soup(url, driver, waiting_time)

    target_td_list = soup.find_all("td", attrs={"class": "txt_l"})

    target_ids = [a["href"] for td in target_td_list if (a := td.find("a")) and "/horse/" in a["href"]]
    target_id = [re.search(r"/horse/(\d+)/", href).group(1) for href in target_ids]
    for id in range(len(target_id)):
        target_id_list.append(target_id[id])
    # print(f"target_id_list:{target_id_list}")
    df = pd.DataFrame({"horse_id": target_id_list}, index=[ref_id] * len(target_id_list))
    # print(f"df:{df}")
    return df


################################# Done ####################################
def scrape_race_id_list(self, ref_id, driver, waiting_time):
    # query = ["kaisai_date=" + str(ref_id)]
    url = f"{self.from_location}?kaisai_date={ref_id}"
    # print(f"url:{url}")
    # df = pd.DataFrame()
    race_id_list = []
    max_attempt = 5
    try:
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

                # インデックスをhorse_idにする
        # DataFrameを作成し、インデックスをref_idに設定する
        df = pd.DataFrame({"race_id": race_id_list}, index=[ref_id] * len(race_id_list))
    except Exception as e:
        print("Error at {}: {}".format(ref_id, e))
        print("error / obtained_last_key: ", self.obtained_last_key)

    return df


################################# Done ####################################
def scrape_html_race(self, ref_id):
    url = self.from_location + ref_id
    html = urlopen(url).read()
    return html


################################# Done ####################################
def scrape_html_horse(self, ref_id):
    url = self.from_location + ref_id
    html = urlopen(url).read()
    return html


################################# Done ####################################
def scrape_html_ped(self, ref_id):
    url = self.from_location + ref_id
    html = urlopen(url).read()
    return html


################################# Done ####################################
def process_bin_file(self, process_function):
    """
    binファイルを受け取って、テーブルに変換する関数。
    process_functionを入れ替えながら汎用的に使う共通モジュール
    対象ファイルや処理のバッチサイズなどを読み取り、セットの上、処理する
    """

    target_bin_files = sorted(self.get_file_list(self.from_local_location))
    total_batches = (len(target_bin_files) + self.batch_size - 1) // self.batch_size  # バッチ数の計算
    total_files = len(target_bin_files)  # 処理対象の全データ数
    print(f"# of input files: {total_files}")
    processed_files = 0  # 処理済みのファイル数

    print(f"start {self.alias} processing")

    # tqdmインスタンスの作成
    pbar = tqdm(total=total_files, desc="Processing Batches", unit="%", unit_scale=True, leave=False)

    for batch_index in range(total_batches):
        start_index = batch_index * self.batch_size
        end_index = min((batch_index + 1) * self.batch_size, len(target_bin_files))
        batch_target_bin_files = target_bin_files[start_index:end_index]

        for target_bin_file in batch_target_bin_files:  # race_html binファイル
            target_bin_file_path = os.path.join(self.from_local_location, target_bin_file)

            self.target_data = pd.DataFrame()
            try:
                self.target_data = process_function(target_bin_file_path)  # , target_data_name)
                time.sleep(1)
            except Exception as e:
                print("Error at {}: {}".format(target_bin_file_path, e))
                break

            processed_files += 1
            pbar.update(1)  # 処理済みのファイル数を1増やす
            # temp_df = pd.concat([temp_df[key] for key in temp_df])

            # if self.alias == "race_results_table":
            #    self.target_data = trim_function(temp_df)

            self.save_temp_file(self.alias)
            # target_data_name = {}  # バッチ処理が完了したので辞書をクリア

        self.obtained_last_key = target_bin_files[-1]
        self.transfer_temp_file()
    self.copy_files()

    print(f"# of processed files: {processed_files}")


################################# Done ####################################
def trim_function(df):
    """
    process_bin_file()のヘルパー関数

    process_functionとして呼び出したエイリアスによって異なる後処理を定義している
    """
    # 列名に半角スペースがあれば除去する
    trimmed_df = df.columns = df.columns.str.replace(r"\s+", "")
    return trimmed_df


################################# Done ####################################
def create_raw_race_results(target_bin_file_path):
    with open(target_bin_file_path, "rb") as f:
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
        race_id = re.findall(r"\d+", target_bin_file_path)[0]
        df.index = [race_id] * len(df)
        df[race_id] = df.index
        df.columns = df.columns.str.replace(" ", "")

        # last_column_name = df.columns[-1]
        # df = df.rename(columns={last_column_name: "race_id"})
        # race_id_column = df.pop("race_id")
        # df.insert(0, "race_id", race_id_column)
        ###### df.drop(df.columns[0], axis=1, inplace=True)

    return df


################################# Done ####################################
def create_raw_horse_results(target_bin_file_path):
    with open(target_bin_file_path, "rb") as f:
        # 保存してあるbinファイルを読み込む
        html = f.read()

        df = pd.read_html(html)[3]
        # 受賞歴がある馬の場合、3番目に受賞歴テーブルが来るため、4番目のデータを取得する
        if df.columns[0] == "受賞歴":
            df = pd.read_html(html)[4]

        # 新馬の競走馬レビューが付いた場合、
        # 列名に0が付与されるため、次のhtmlへ飛ばす
        if df.columns[0] == 0:
            print("horse_results empty case1 {}".format(target_bin_file_path))
            # continue

        # インデックスをhorse_idにする
        horse_id = re.findall(r"\d+", target_bin_file_path)[0]
        df.index = [horse_id] * len(df)
        df[horse_id] = df.index

    return df


################################# Done ####################################
def create_raw_horse_info(target_bin_file_path):
    with open(target_bin_file_path, "rb") as f:
        # 保存してあるbinファイルを読み込む
        html = f.read()

        # 馬の基本情報を取得
        df = pd.read_html(html)[1].set_index(0).T

        # htmlをsoupオブジェクトに変換
        soup = BeautifulSoup(html, "lxml")

        # 調教師IDをスクレイピング
        try:
            trainer_a_list = soup.find("table", attrs={"summary": "のプロフィール"}).find_all(
                "a", attrs={"href": re.compile("^/trainer")}
            )
            trainer_id = re.findall(r"trainer/(\w*)", trainer_a_list[0]["href"])[0]
        except IndexError:
            # 調教師IDを取得できない場合
            # print('trainer_id empty {}'.format(race_html))
            trainer_id = NaN
        df["trainer_id"] = trainer_id

        # 馬主IDをスクレイピング
        try:
            owner_a_list = soup.find("table", attrs={"summary": "のプロフィール"}).find_all(
                "a", attrs={"href": re.compile("^/owner")}
            )
            owner_id = re.findall(r"owner/(\w*)", owner_a_list[0]["href"])[0]
        except IndexError:
            # 馬主IDを取得できない場合
            # print('owner_id empty {}'.format(race_html))
            owner_id = NaN
        df["owner_id"] = owner_id

        # 生産者IDをスクレイピング
        try:
            breeder_a_list = soup.find("table", attrs={"summary": "のプロフィール"}).find_all(
                "a", attrs={"href": re.compile("^/breeder")}
            )
            breeder_id = re.findall(r"breeder/(\w*)", breeder_a_list[0]["href"])[0]
        except IndexError:
            # 生産者IDを取得できない場合
            # print('breeder_id empty {}'.format(race_html))
            breeder_id = NaN
        df["breeder_id"] = breeder_id

        # インデックスをhorse_idにする
        horse_id = re.findall(r"\d+", target_bin_file_path)[0]
        df.index = [horse_id] * len(df)
        df[horse_id] = df.index
    return df


################################# Done ####################################
def create_raw_horse_ped(target_bin_file_path):
    with open(target_bin_file_path, "rb") as f:
        # 保存してあるbinファイルを読み込む
        html = f.read()
        # horse_idを取得
        horse_id = re.findall(r"(\d+)", target_bin_file_path)[0]
        # htmlをsoupオブジェクトに変換

        soup = BeautifulSoup(html, "lxml")
        df = pd.DataFrame()
        peds_id_list = []
        # 血統データからhorse_idを取得する
        horse_a_list = soup.find("table", attrs={"summary": "5代血統表"}).find_all(
            "a", attrs={"href": re.compile(r"^/horse/\w{10}")}
        )

        for a in horse_a_list:
            # 血統データのhorse_idを抜き出す
            work_peds_id = re.findall(r"horse\W(\w{10})", a["href"])[0]
            peds_id_list.append(work_peds_id)

        df[horse_id] = peds_id_list

        # pd.DataFrame型にして一つのデータにまとめて、列と行の入れ替えして、列名をpeds_0, ..., peds_61にする
        df = df.transpose()
        df.columns = ["peds_" + str(i) for i in range(len(df.columns))]
        # print("df", df)

    return df


################################# Done ####################################
def create_raw_race_return(target_bin_file_path):
    with open(target_bin_file_path, "rb") as f:
        # 保存してあるbinファイルを読み込む
        html = f.read()
        html = html.replace(b"<br />", b"br")
        dfs = pd.read_html(html)

        # dfsの1番目に単勝〜馬連、2番目にワイド〜三連単がある
        df = pd.concat([dfs[1], dfs[2]])

        # インデックスをrace_idにする
        race_id = re.findall(r"\d+", target_bin_file_path)[0]
        df.index = [race_id] * len(df)
        df[race_id] = df.index

    return df
