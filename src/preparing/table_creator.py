import os
import re

import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

from src.constants._master import Master
from src.preparing.DataLoader import DataLoader


class TableCreator(DataLoader):
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

    def create_race_results_table(self):
        """
        race_htmlを受け取って、レース結果テーブルに変換する関数。
        """
        # skip対象ではないゴミファイルの掃除
        if not self.skip:
            self.delete_files()

        race_html_list = self.get_file_list(self.from_local_location)

        data_index = 1

        print("creating race_results_table")
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
                    race_id = re.findall(r"race\W(\d+).bin", race_html_path)[0]
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

    def create_race_info_table(self):
        """
        raceページのhtmlを受け取って、レース情報テーブルに変換する関数。
        """
        # skip対象ではないゴミファイルの掃除
        if not self.skip:
            self.delete_files()

        race_html_list = self.get_file_list(self.from_local_location)

        data_index = 1
        print("preparing raw race_info table")
        race_infos = {}
        for race_html in tqdm(race_html_list):
            race_html_path = os.path.join(self.from_local_location, race_html)
            with open(race_html_path, "rb") as f:
                try:
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
                    df = pd.DataFrame()
                    # 障害レースフラグを初期化
                    hurdle_race_flg = False
                    for text in info:
                        if text in ["芝", "ダート"]:
                            df["race_type"] = [text]
                        if "障" in text:
                            df["race_type"] = ["障害"]
                            hurdle_race_flg = True
                        if "m" in text:
                            # 20211212：[0]→[-1]に修正
                            df["course_len"] = [int(re.findall(r"\d+", text)[-1])]
                        if text in Master.GROUND_STATE_LIST:
                            df["ground_state"] = [text]
                        if text in Master.WEATHER_LIST:
                            df["weather"] = [text]
                        if "年" in text:
                            df["date"] = [text]
                        if "右" in text:
                            df["around"] = [Master.AROUND_LIST[0]]
                        if "左" in text:
                            df["around"] = [Master.AROUND_LIST[1]]
                        if "直線" in text:
                            df["around"] = [Master.AROUND_LIST[2]]
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

                    # グレードレース情報の取得
                    grade_text = soup.find("div", attrs={"class": "data_intro"}).find_all("h1")[0].text
                    if "G3" in grade_text:
                        df["race_class"] = [Master.RACE_CLASS_LIST[6]] * len(df)
                    elif "G2" in grade_text:
                        df["race_class"] = [Master.RACE_CLASS_LIST[7]] * len(df)
                    elif "G1" in grade_text:
                        df["race_class"] = [Master.RACE_CLASS_LIST[8]] * len(df)

                    # 障害レースの場合
                    if hurdle_race_flg:
                        df["around"] = [Master.AROUND_LIST[3]]
                        df["race_class"] = [Master.RACE_CLASS_LIST[9]]

                    # インデックスをrace_idにする
                    race_id = re.findall(r"(\d+).bin", race_html)[0]
                    df.index = [race_id] * len(df)
                    race_infos[race_id] = df
                except Exception as e:
                    print("Error at {}: {}".format(race_html_path, e))
            data_index += 1
        # pd.DataFrame型にして一つのデータにまとめる
        self.target_data = pd.concat([race_infos[key] for key in race_infos])

        self.save_temp_file("race_info_table")
        self.copy_files()

    def create_race_return_table(self):
        """
        raceページのhtmlを受け取って、払い戻しテーブルに変換する関数。
        """
        if not self.skip:
            self.delete_files()

        race_html_list = self.get_file_list(self.from_local_location)

        data_index = 1
        race_return = {}
        print("preparing raw return table")
        race_return = {}
        for race_html in tqdm(race_html_list):
            race_html_path = os.path.join(self.from_local_location, race_html)
            with open(race_html_path, "rb") as f:
                try:
                    # 保存してあるbinファイルを読み込む
                    html = f.read()

                    html = html.replace(b"<br />", b"br")
                    dfs = pd.read_html(html)

                    # dfsの1番目に単勝〜馬連、2番目にワイド〜三連単がある
                    df = pd.concat([dfs[1], dfs[2]])

                    race_id = re.findall(r"(\d+).bin", race_html)[0]
                    df.index = [race_id] * len(df)
                    race_return[race_id] = df
                except Exception as e:
                    print("error at {}".format(race_html))
                    print(e)
            data_index += 1
        # pd.DataFrame型にして一つのデータにまとめる
        self.target_data = pd.concat([race_return[key] for key in race_return])
        self.save_temp_file("race_info_table")
        self.copy_files()

    def create_horse_info_table(self):
        """
        horseページのhtmlを受け取って、馬の基本情報のDataFrameに変換する関数。
        """
        if not self.skip:
            self.delete_files()

        race_html_list = self.get_file_list(self.from_local_location)

        data_index = 1

        print("preparing horse_info table")
        horse_info_df = pd.DataFrame()
        horse_info = {}
        for race_html in tqdm(race_html_list):
            race_html_path = os.path.join(self.from_local_location, race_html)
            with open(race_html_path, "rb") as f:
                # 保存してあるbinファイルを読み込む
                html = f.read()

                # 馬の基本情報を取得
                df_info = pd.read_html(html)[1].set_index(0).T

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
                df_info["trainer_id"] = trainer_id

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
                df_info["owner_id"] = owner_id

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
                df_info["breeder_id"] = breeder_id

                # インデックスをrace_idにする
                horse_id = re.findall(r"horse\W(\d+).bin", race_html)[0]
                df_info.index = [horse_id] * len(df_info)
                horse_info[horse_id] = df_info
            data_index += 1
        # pd.DataFrame型にして一つのデータにまとめる
        horse_info_df = pd.concat([horse_info[key] for key in horse_info])

        return horse_info_df
