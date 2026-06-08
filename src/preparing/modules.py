import logging
import os
import re
import time

import pandas as pd

from src.constants._master import Master

NaN = float("nan")

logger = logging.getLogger(__name__)


def scrape_scheduled_race_html(self, ref_id):
    # 時刻とレースidの組みあわせからレースidだけを抽出
    # race_id_list = [element.split(",")[1] for element in time_race_id_list]

    query = ["?race_id=" + str(ref_id)]
    url = self.from_location + query[0]
    return get_soup(url)[0].read()


def storing_process(self):
    df = []
    df_sorted = []
    df_supressed = []
    self_path = os.path.join(self.to_temp_location, self.temp_save_file_name)
    df = pd.read_csv(self_path)
    df_copy = df.copy()
    df_copy.iloc[:, -1] = df_copy.iloc[:, -1].astype(int)
    # 最後の列にstrip()メソッドを適用する
    df_copy.iloc[:, -1] = df_copy.iloc[:, -1].apply(lambda x: x.strip() if isinstance(x, str) else x)
    df_sorted = df_copy.iloc[:, -1].sort_values()
    df_supressed = df_sorted.drop_duplicates()
    self.target_data = df_supressed.reset_index(drop=True)

    self.delete_files_tmp()
    self.save_temp_file(self.alias)
    self.target_data = df

    self.transfer_temp_file()


################################# Done ####################################
def process_pkl_file(self, process_function):
    """
    pklファイルを受け取って、テーブルに変換する関数。
    process_functionを入れ替えながら汎用的に使う共通モジュール
    対象ファイルや処理のバッチサイズなどを読み取り、セットの上、処理する
    """
    if self.alias == "kaisai_date_list":
        # yyyy-mmの形式でfrom_とto_を指定すると、間のレース開催日一覧yyyy-mm-ddが返ってくる関数。
        # to_の月は含まないので注意。
        df = pd.DataFrame({"kaisai_data": []})
        target_all_files = pd.date_range(start=self.from_date, end=self.to_date, freq="ME").astype(str)
    else:
        df = self.load_file_pkl()
        # ID（日付/race_id 等）は最後の列に入る（load_file_pkl も iloc[:, -1] を
        # int 変換して扱う）。先頭列は "Unnamed: 0" などの連番のことがあるため
        # iloc[:, -1] を使う。
        target_all_files = df.iloc[:, -1]

        # print("target_pkl_fileはここから+''")
        logger.debug("%s", target_all_files.head())

    total_batches = (len(target_all_files) + self.batch_size - 1) // self.batch_size  # バッチ数の計算
    total_files = len(target_all_files)  # 処理対象の全データ数
    filetype = self.get_filetype()
    logger.info("filetype:%s", filetype)
    logger.info("# of input files: %s", total_files)
    logger.info("# of total_batches: %s", total_batches)
    processed_files = 0  # 処理済みのファイル数
    # print(f"start {self.alias} processing")
    # target_data_name = {}

    # tqdmインスタンスの作成（Jupyter ではグラフィカルバー、端末ではテキストバーを自動選択）
    from tqdm.auto import tqdm
    pbar = tqdm(total=total_files, desc=f"{self.alias} 取得", unit="件", leave=True)

    # スクレイパーのインスタンス化（Playwright 全面移行）。
    # process_function には従来の driver 引数位置で AbstractScraper を渡す
    # （同期パイプライン互換のため fetch_sync 境界を内部で使う）。
    from src.preparing._scraper import PlaywrightScraper
    driver = PlaywrightScraper()
    # ブラウザをループ全体で 1 度だけ起動して使い回す（毎回の起動・終了を避け高速化）。
    driver.open_sync()
    # Playwright は wait_for_selector / domcontentloaded で描画完了を判定するため
    # implicitly_wait 相当は不要。waiting_time は後方互換のためのプレースホルダ。
    waiting_time = 30

    for batch_index in range(total_batches):
        start_index = batch_index * self.batch_size
        end_index = min((batch_index + 1) * self.batch_size, len(target_all_files))
        batch_target_all_files = target_all_files[start_index:end_index]
        # print("ref_idの確認:", batch_target_all_files[0:2])
        batch_data = []
        return_data = None  # 例外で process_function が完了しない場合への初期化
        for ref_id in batch_target_all_files:  #
            try:
                # print(f"ref_id:{ref_id}")
                self.processing_id = ref_id
                return_data = process_function(self, ref_id, driver, waiting_time)

                time.sleep(0.2)  # サーバ負荷軽減の最小待機
                batch_data.append(return_data)
            # print(f"temp_df:{temp_df}")
            except Exception as e:
                logger.error("Error at %s: %s:%s", processed_files + 1, ref_id, e)
                self.obtained_last_key = ref_id
                break

            processed_files += 1
            pbar.update(1)  # 処理済みのファイル数を1増やす
            # temp_df = pd.concat([temp_df[key] for key in temp_df])
            self.obtained_last_key = ref_id

        # print(f"直前 filetype:{filetype}")
        if filetype != "bin":
            if batch_data:
                df = pd.concat(batch_data)
                self.target_data = df
                self.save_temp_file(self.alias)
        else:
            if return_data is not None:
                self.processing_id = ref_id
                self.target_data = return_data
                self.save_temp_file(self.alias)

    if filetype != "bin":
        storing_process(self)

    # ループ全体で使い回したブラウザを終了する。
    driver.close_sync()

    logger.info("# of processed files: %s", processed_files)


def get_kaisai_date_list(self, ref_id, driver, waiting_time):
    match = re.match(r"^(\d{4})-(\d{2})-\d{2}$", ref_id)
    if match:
        year = match.group(1)
        month = match.group(2)
        # print("Year:", year)
        # print("Month:", month)
    else:
        logger.warning("Invalid date format")

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
def get_soup(url, driver, waiting_time=None):
    """URL の HTML を取得して BeautifulSoup を返す（Playwright 同期ブリッジ）。

    driver は AbstractScraper（PlaywrightScraper）。JS 描画ページでも
    domcontentloaded まで待って HTML を取得する。waiting_time は後方互換の未使用引数。
    """
    from bs4 import BeautifulSoup

    html = driver.fetch_sync(url)
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
def scrape_race_id_list(self, ref_id, driver, waiting_time=None):
    """開催日のレース一覧ページから race_id を抽出する（Playwright + bs4）。

    driver は AbstractScraper。RaceList_Box が JS 描画されるまで待って HTML を取得し、
    アンカーの href から race_id を正規表現で抜き出す（selenium の find_element を廃止）。
    """
    from bs4 import BeautifulSoup

    url = f"{self.from_location}?kaisai_date={ref_id}"
    # リクエストした開催日の年（race_id 先頭4桁）。ページ読込失敗時に netkeiba が
    # 現在のデフォルトレース一覧を返すことがあるため、年が一致する race_id だけ残す。
    expected_year = str(ref_id)[:4]
    race_id_list = []
    df = pd.DataFrame({"race_id": []}, index=[])
    try:
        # RaceList_Box の描画完了を待ってから HTML を取得
        html = driver.fetch_sync(url, wait_selector=".RaceList_Box")
        soup = BeautifulSoup(html, "lxml")
        box = soup.find(class_="RaceList_Box")
        a_list = box.find_all("a") if box is not None else []
        for a in a_list:
            href = a.get("href", "")
            race_id = re.findall(
                r"(?<=shutuba.html\?race_id=)\d+|(?<=result.html\?race_id=)\d+", href
            )
            # 年が一致しない race_id（=フォールバックで返った現在レース）は除外
            if len(race_id) > 0 and race_id[0][:4] == expected_year:
                race_id_list.append(race_id[0])
        df = pd.DataFrame({"race_id": race_id_list}, index=[ref_id] * len(race_id_list))
    except Exception as e:
        logger.error("Error at %s: %s", ref_id, e)
        logger.error("error / obtained_last_key: %s", self.obtained_last_key)

    return df


################################# Done ####################################
def scrape_html(self, ref_id, driver, waiting_time):
    """from_location + ref_id の静的 HTML を取得する（race/horse/ped 共通）。

    db.netkeiba.com は素の urlopen（UA なし）に HTTP 400 を返すため、
    PlaywrightScraper（driver）経由で取得する。JS 描画不要なページなので
    domcontentloaded で十分。戻り値は bin ファイル書き込み用に UTF-8 bytes。
    """
    url = str(self.from_location) + str(ref_id)
    html_str = driver.fetch_sync(url)
    return html_str.encode("utf-8")


# race/horse/ped は同一実装。後方互換のため import 可能な別名として公開する
# （url_loader.py が名前で import し process_function として渡すため）。
scrape_html_race = scrape_html
scrape_html_horse = scrape_html
scrape_html_ped = scrape_html


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
    logger.info("# of input files: %s", total_files)
    processed_files = 0  # 処理済みのファイル数

    logger.info("start %s processing", self.alias)

    # tqdmインスタンスの作成（Jupyter ではグラフィカルバー、端末ではテキストバーを自動選択）
    from tqdm.auto import tqdm
    pbar = tqdm(total=total_files, desc=f"{self.alias} 取得", unit="件", leave=True)

    for batch_index in range(total_batches):
        start_index = batch_index * self.batch_size
        end_index = min((batch_index + 1) * self.batch_size, len(target_bin_files))
        batch_target_bin_files = target_bin_files[start_index:end_index]

        for target_bin_file in batch_target_bin_files:  # race_html binファイル
            target_bin_file_path = os.path.join(self.from_local_location, target_bin_file)

            self.target_data = pd.DataFrame()
            try:
                self.target_data = process_function(target_bin_file_path)  # , target_data_name)
                # time.sleep(1)
            except Exception as e:
                logger.error("Error at %s: %s", target_bin_file_path, e)
                continue

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

    logger.info("# of processed files: %s", processed_files)


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
    from bs4 import BeautifulSoup
    race_results = {}
    with open(target_bin_file_path, "rb") as f:
        # 保存してあるbinファイルを読み込む
        html = f.read()

        # メインとなるレース結果テーブルデータを取得
        df = pd.read_html(html.decode("utf-8", errors="replace"))[0]

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
        df["horse_id"].astype(int)

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
        df["jockey_id"].astype(str)

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
        df["trainer_id"].astype(str)

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
        df["owner_id"].astype(str)

        race_id = re.findall(r"\d+", target_bin_file_path)[0]
        df["race_id"] = race_id
        df["race_id"].astype(int)

        # インデックスをrace_idにする
        df.index = [race_id] * len(df)
        # df.set_index("race_id", inplace=True)  # この行を削除

        race_results[race_id] = df
        # 各レースの結果データフレームを結合して race_results_df を生成
        race_results_df = pd.concat([race_results[key] for key in race_results])
        race_results_df.set_index("race_id", inplace=True)
        race_results_df = race_results_df.rename(columns=lambda x: x.replace(" ", ""))

    return race_results_df


# パターンにマッチする部分を抽出する関数を定義
def convert_string(value):
    # 正規表現パターンを定義
    pattern = r"\d{0,2}([^\d]+)\d{0,2}"
    match = re.search(pattern, value)
    if match:
        return match.group(1)  # マッチした部分の文字列を返す
    else:
        return value  # マッチしなかった場合は元の値を返す


################################# Done ####################################
def create_raw_horse_results(target_bin_file_path):
    horse_result = {}
    with open(target_bin_file_path, "rb") as f:
        # 保存してあるbinファイルを読み込む
        html = f.read()

        html_str = html.decode("utf-8", errors="replace")
        df = pd.read_html(html_str)[3]
        # 受賞歴がある馬の場合、3番目に受賞歴テーブルが来るため、4番目のデータを取得する
        if df.columns[0] == "受賞歴":
            df = pd.read_html(html_str)[4]
            # print(f"test df:{df.iloc[:,1]}")

        # 新馬の競走馬レビューが付いた場合、
        # 列名に0が付与されるため、次のhtmlへ飛ばす
        if df.columns[0] == 0:
            logger.warning("horse_results empty case1 %s", target_bin_file_path)
            # continue

        # インデックスをhorse_idにする
        horse_id = re.findall(r"\d+", target_bin_file_path)[0]
        df.index = [horse_id] * len(df)
        df["horse_id"] = df.index
        df["horse_id"].astype(int)
        # "R"列の値が数値を表す文字列であるかを判定し、数値を表す文字列の場合にintに変換する
        for index, value in df["R"].items():
            if isinstance(value, str) and value.isdigit():
                df.at[index, "R"] = int(value)

        # "R"列のデータ型をintに変換する
        df["R"] = pd.to_numeric(df["R"], errors="coerce").astype(float).astype("Int64")

        df.columns = df.columns.str.replace(" ", "")
        df.iloc[:, 1] = df.iloc[:, 1].apply(convert_string)
        horse_result[horse_id] = df
        # 各レースの結果データフレームを結合して race_results_df を生成
        horse_result_df = pd.concat([horse_result[key] for key in horse_result])
        horse_result_df.set_index("horse_id", inplace=True)

    return horse_result_df


################################# Done ####################################
def create_raw_horse_info(target_bin_file_path):
    from bs4 import BeautifulSoup
    horse_info = {}
    with open(target_bin_file_path, "rb") as f:
        # 保存してあるbinファイルを読み込む
        html = f.read()

        # 馬の基本情報を取得
        df = pd.read_html(html.decode("utf-8", errors="replace"))[1].set_index(0).T

        # htmlをsoupオブジェクトに変換
        soup = BeautifulSoup(html, "lxml")
        # 列に "募集情報" が含まれているかを調べる
        funding_info = df.apply(lambda x: x.str.contains("募集情報")).any()

        # 列に "募集情報" がある場合、その列の値を "募集情報" 列に代入する

        if funding_info.any():
            df["募集情報"] = df.loc[:, funding_info].values.flatten()
        else:
            # 列に "募集情報" が含まれていない場合、"募集情報" 列に NaN を入れる
            df["募集情報"] = NaN

        # print(f"soup:{soup}")
        # user_input = input()
        # if user_input == " ":
        #    pass

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
        # df["owner_id"] = df["owner_id"].astype(str)

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
        df["horse_id"] = df.index
        df["horse_id"].astype(int)

        horse_info[horse_id] = df
        # 各レースの結果データフレームを結合して race_results_df を生成
        horse_info_df = pd.concat([horse_info[key] for key in horse_info])
        horse_info_df.set_index("horse_id", inplace=True)

    return horse_info_df


################################# Done ####################################
def create_raw_horse_ped(target_bin_file_path):
    from bs4 import BeautifulSoup
    horse_ped = {}
    with open(target_bin_file_path, "rb") as f:
        # 保存してあるbinファイルを読み込む
        html = f.read()
        # horse_idを取得

        # htmlをsoupオブジェクトに変換
        horse_id = re.findall(r"\d+", target_bin_file_path)[0]
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
        df["horse_id"] = horse_id
        df["horse_id"].astype(int)
        # print("df", df)
        df["horse_id"] = df.index

        horse_ped[horse_id] = df
        # 各レースの結果データフレームを結合して race_results_df を生成
        horse_ped_df = pd.concat([horse_ped[key] for key in horse_ped])
        horse_ped_df.set_index("horse_id", inplace=True)

    return horse_ped_df


################################# Done ####################################
def create_raw_race_return(target_bin_file_path):
    race_return = {}
    with open(target_bin_file_path, "rb") as f:
        # 保存してあるbinファイルを読み込む
        html = f.read()
        html_str = html.replace(b"<br />", b"br").decode("utf-8", errors="replace")
        dfs = pd.read_html(html_str)

        # dfsの1番目に単勝〜馬連、2番目にワイド〜三連単がある
        df = pd.concat([dfs[1], dfs[2]])
        # 列名を整数に変更

        # インデックスをrace_idにする
        race_id = re.findall(r"\d+", target_bin_file_path)[0]
        df.index = [race_id] * len(df)
        # df["race_id"].astype(int)
        df["race_id"] = df.index
        # race_id列を除外して、他の列名のみを整数に変換する辞書を作成
        new_columns = {col: int(col) for col in df.columns if col != "race_id"}
        # 列名を変換
        df.rename(columns=new_columns, inplace=True)
        race_return[race_id] = df
        # 各レースの結果データフレームを結合して race_results_df を生成
        race_return_df = pd.concat([race_return[key] for key in race_return])
        race_return_df.set_index("race_id", inplace=True)

    return race_return_df


def dart_checker(text1):
    if text1.split("/")[0].strip()[:2] == "障芝":
        dart = True
    else:
        dart = False
    return dart


def count_ground_state(text1):
    return text1.split("/")[2].count(":")


def create_raw_race_info(target_bin_file_path):
    from bs4 import BeautifulSoup
    # print(f"target_bin_file_path:{target_bin_file_path}")
    with open(target_bin_file_path, "rb") as f:
        # 保存してあるbinファイルを読み込む
        html = f.read()

        # htmlをsoupオブジェクトに変換
        soup = BeautifulSoup(html, "lxml")

        # 天候、レースの種類、コースの長さ、馬場の状態、日付、回り、レースクラスをスクレイピング
        text1 = soup.find("div", attrs={"class": "data_intro"}).find_all("p")[0].text
        text2 = soup.find("div", attrs={"class": "data_intro"}).find_all("p")[1].text
        # print(f"text1:{text1}")
        # print(f"text2:{text2}")
        race_id = str(re.findall(r"\d+", target_bin_file_path)[0])
        # print(f"race_id :{race_id}")
        # テキスト情報を解析してDataFrameに変換
        race_distance = re.search(r"\d+", text1.split("/")[0]).group()
        weather = text1.split("/")[1].split(":")[1].strip()

        if weather in Master.WEATHER_LIST:
            pass
        else:
            logger.warning("unknown weather definition appeared:%s", race_id)

        race_type = text1.split("/")[2].split(":")[0].strip()
        # 発走時刻
        start_time = text1.split("/")[-1].split(":")[1:3]
        start_time = ":".join(start_time).strip().split("\n\n")[0]

        race_date = text2.split(" ")[0]
        race_name = text2.split(" ")[1]
        dart = dart_checker(text1)
        # print(f"dart:{dart}")
        # print(f"count:{count_ground_state(text1)}")
        # test = text1.split("/")[2].split(":")[2]
        # print(f"gs2:{test}")

        for around in Master.AROUND_LIST:
            if around in text1.split("/")[0]:
                around_info = around
            else:
                around_info = None

        # 開催日数と開催回数を取得
        race_day_count = re.search(r"\d+", race_name.split("日目")[0]).group()  # 開催日数
        race_round_count = re.search(r"\d+", race_name.split("回")[0]).group()  # 開催回数

        # 開催場所を取得
        # place_id = None
        for key, value in Master.PLACE_DICT.items():
            if key in race_name:
                place_id = value
                place_name = key
        # 未知の馬場状態でも DataFrame 構築（後段 768-769 行）まで到達できるよう、
        # スクレイプ生値をそのまま使うフォールバックを置く（既存は NameError でクラッシュ）。
        ground_state1 = NaN
        ground_state2 = NaN
        if count_ground_state(text1) == 1:
            temp_ground_state0 = text1.split("/")[2].split(":")[1].strip()
            # 既知/未知に関わらず生値を採用。未知時のみ警告を残す。
            ground_state1 = temp_ground_state0
            ground_state2 = temp_ground_state0
            if temp_ground_state0 not in Master.GROUND_STATE_LIST:
                logger.warning(
                    "unknown GROUND_STATE definition appeared1:%s%s", race_id, temp_ground_state0
                )
        elif dart_checker(text1) and count_ground_state(text1) == 2:
            temp_ground_state1 = text1.split("/")[2].split(":")[1].split()[0].strip()
            ground_state1 = temp_ground_state1
            if temp_ground_state1 not in Master.GROUND_STATE_LIST:
                logger.warning("unknown GROUND_STATE definition appeared2:%s%s", race_id, temp_ground_state1)
            temp_ground_state2 = text1.split("/")[2].split(":")[2].strip()
            ground_state2 = temp_ground_state2
            if temp_ground_state2 not in Master.GROUND_STATE_LIST:
                logger.warning("unknown GROUND_STATE definition appeared3:%s%s", race_id, temp_ground_state2)
        # 不要な部分を削除
        # レース条件から年齢、性別、レースクラスを削除
        # 馬齢を取得
        race_condition = text2.split(" ")[2]

        # レース条件に基づいてフラグを設定
        race_flags = {}
        if race_condition is not None:
            # 性別を取得
            sex_info = None
            for sex in Master.SEX_LIST:
                if sex in race_condition:
                    sex_info = sex

            # レースクラスを取得
            race_class_info = None
            for race_class in Master.RACE_CLASS_LIST:
                if race_class in race_condition:
                    race_class_info = race_class
            if race_class_info is None:
                logger.warning("unknown race_class definition appeared:%s", race_id)
            # 向きを取得

            if (around_info is None) and (("障害" in race_class_info or race_condition) or dart):
                around_info = "直線"

            for key, value in Master.RACE_CONDITION_DICT.items():
                if key in race_condition:
                    race_flags[value] = 1
                    race_condition = race_condition.replace(key, "").strip()
                else:
                    race_flags[value] = 0

        if "歳以上" in race_condition:
            age = re.search(r"\d+", race_condition.split("歳以上")[0]).group() + "+"

        else:
            age = re.search(r"\d+", race_condition.split("歳")[0]).group()
        if race_condition is not None:
            # ageの処理を修正
            if age is not None and age != "":
                if "+" in age:
                    race_condition = race_condition.replace(age[:-1], "").replace("歳以上", "").strip()
                else:
                    race_condition = race_condition.replace(age, "").replace("歳", "").strip()

            if sex_info is not None and sex_info != "":
                race_condition = race_condition.replace(sex_info, "").strip()
            if race_class_info is not None and race_class_info != "":
                race_condition = race_condition.replace(race_class_info, "").strip()
            if race_condition is not None and race_condition != "":
                race_condition = race_condition.replace("()", "").strip()
                race_condition = race_condition.replace("[]", "").strip()
            if race_condition is not None and race_condition != "":
                race_condition = race_condition.strip()

        # DataFrame作成歳
        df = pd.DataFrame(
            {
                "race_id": [race_id],
                "place_id": [place_id],
                "place": [place_name],
                "days": [race_day_count],  # 開催日数を追加
                "times": [race_round_count],  # 開催回数を追加
                "date": [race_date],
                "time": [start_time],
                "race_type": [race_type],
                "around": [around_info],
                "course_len": [race_distance],
                "weather": [weather],
                "ground_state1": [ground_state1],
                "ground_state2": [ground_state2],
                "age": [age],
                "sex": [sex_info],
                "race_class": [race_class_info],
                "race_condition": [race_condition],
                **race_flags,
            }
        )
    df["race_id"].astype(int)
    df.set_index("race_id", inplace=True)
    return df


r"""
def create_tmp_race_info(target_bin_file_path):
    with open(target_bin_file_path, "rb") as f:
        html = f.read()
        soup = BeautifulSoup(html, "lxml")

        # 天候、レースの種類、コースの長さ、馬場の状態、日付、回り、レースクラスをスクレイピング
        text1 = soup.find("div", attrs={"class": "data_intro"}).find_all("p")[0].text
        text2 = soup.find("div", attrs={"class": "data_intro"}).find_all("p")[1].text
        logger.debug("text1:%s", text1)
        logger.debug("text2:%s", text2)

        # テキスト情報を解析してDataFrameに変換
        race_distance = re.search(r"\d+", text1.split("/")[0]).group()
        weather = text1.split("/")[1].split(":")[1].strip()

        if weather in Master.WEATHER_LIST:
            pass
        else:
            logger.warning("unknown weather definition appeared")

        race_type = text1.split("/")[2].split(":")[0].strip()
        # 向きを取得

        around_info = None
        for around in Master.AROUND_LIST:
            if around in text1.split(" ")[0]:
                around_info = around
        if around_info is None:
            logger.warning("unknown around definition appeared")

        ground_state1 = text1.split("/")[2].split(":")[1].strip()
        if ground_state1 in Master.GROUND_STATE_LIST:
            pass
        else:
            logger.warning("unknown GROUND_STATE definition appeared")

        start_time = text1.split("/")[-1].split(":")[1:3]
        start_time = ":".join(start_time).strip()
        race_date = text2.split(" ")[0]
        race_name = text2.split(" ")[1]

        # 開催日数と開催回数を取得
        race_day_count = re.search(r"\d+", race_name.split("日目")[0]).group()  # 開催日数
        race_round_count = re.search(r"\d+", race_name.split("回")[0]).group()  # 開催回数

        # 開催場所を取得
        place_id = None
        for key, value in Master.PLACE_DICT.items():
            if key in race_name:
                place_id = value
                place_name = key
        # 馬齢を取得
        race_condition = text2.split(" ")[2]
        age = re.search(r"\d+", race_condition.split("歳")[0]).group()
        # 性別を取得
        sex_info = None
        for sex in Master.SEX_LIST:
            if sex in race_condition:
                sex_info = sex
        if sex_info is None:
            logger.warning("unknown sex definition appeared")
        # レースクラスを取得
        race_class_info = None
        for race_class in Master.RACE_CLASS_LIST:
            if race_class in race_condition:
                race_class_info = race_class
        if race_class_info is None:
            logger.warning("unknown race_class definition appeared")
        # 不要な部分を削除
        # レース条件から年齢、性別、レースクラスを削除
        race_condition = (
            race_condition.replace(age, "").replace("歳", "").replace(sex_info, "").replace(race_class_info, "").strip()
        )

        # DataFrame作成歳
        df = pd.DataFrame(
            {
                "レース名": [race_name],
                "レース場id": [place_id],
                "レース場名": [place_name],
                "開催日数": [race_day_count],  # 開催日数を追加
                "開催回数": [race_round_count],  # 開催回数を追加
                "レース開催日": [race_date],
                "発走時刻": [start_time],
                "レース種類": [race_type],
                "向き": [around_info],
                "レース距離": [race_distance],
                "天候": [weather],
                "馬場状態1": [ground_state1],
                "馬場状態2": [ground_state2],
                "馬齢": [age],
                "性別": [sex_info],
                "レースクラス": [race_class_info],
                "レース条件": [race_condition],
            }
        )

    return df



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


        # 天候、レースの種類、コースの長さ、馬場の状態、日付、回り、レースクラスをスクレイピング
        texts = (
            soup.find("div", attrs={"class": "data_intro"}).find_all("p")[0].text
            + soup.find("div", attrs={"class": "data_intro"}).find_all("p")[1].text
        )

        info = re.findall(r"\w+", texts)
        logger.debug("info:%s", info)
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
                df["around"] = [Master.AROUND_RIGHT]
            if "左" in text:
                df["around"] = [Master.AROUND_LEFT]
            if "直線" in text:
                df["around"] = [Master.AROUND_STRAIGHT]
            if "障害" in text:
                # AROUND_LIST[3] は範囲外: 既存挙動保持のため位置参照を残す。
                df["around"] = [Master.AROUND_LIST[3]]
                hurdle_race_flg = True

            if text in Master.GROUND_STATE_LIST:
                df["ground_state"] = [text]
            if text in Master.WEATHER_LIST:
                df["weather"] = [text]
            if "年" in text:
                df["date"] = [text]

            if "新馬" in text:
                df["race_class"] = [Master.RACE_CLASS_SHINBA]
            if "未勝利" in text:
                df["race_class"] = [Master.RACE_CLASS_MISHORI]
            if ("1勝クラス" in text) or ("500万下" in text):
                df["race_class"] = [Master.RACE_CLASS_1SHO]
            if ("2勝クラス" in text) or ("1000万下" in text):
                df["race_class"] = [Master.RACE_CLASS_2SHO]
            if ("3勝クラス" in text) or ("1600万下" in text):
                df["race_class"] = [Master.RACE_CLASS_3SHO]
            if "オープン" in text:
                df["race_class"] = [Master.RACE_CLASS_OPEN]
            if hurdle_race_flg:
                # 障害は race_class を上書きしない（障害判定は race_type 側で表現）。
                hurdle_race_flg = False

        # グレードレース情報の取得（grade アイコン → 対応グレードへ正しくマップ）
        grade_text = soup.find("div", attrs={"class": "data_intro"}).find_all("h1")[0].text
        if "G3" in grade_text:
            df["race_class"] = [Master.RACE_CLASS_G3] * len(df)
        elif "G2" in grade_text:
            df["race_class"] = [Master.RACE_CLASS_G2] * len(df)
        elif "G1" in grade_text:
            df["race_class"] = [Master.RACE_CLASS_G1] * len(df)

        df["race_id"] = race_id

    return df

"""
