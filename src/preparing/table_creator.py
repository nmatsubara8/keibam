import datetime
import os
import re

import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from tqdm import tqdm

from src.constants._master import Master
from src.constants._results_cols import ResultsCols as Cols
from src.preparing.DataLoader import DataLoader
from src.preparing.modules import create_raw_horse_info
from src.preparing.modules import create_raw_horse_ped
from src.preparing.modules import create_raw_horse_results
from src.preparing.modules import create_raw_race_info
from src.preparing.modules import create_raw_race_results
from src.preparing.modules import create_raw_race_return
from src.preparing.modules import create_tmp_race_info
from src.preparing.modules import process_bin_file


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
        race_html binファイルを受け取って、レース結果テーブルに変換する関数。
        """
        process_bin_file(self, create_raw_race_results)

    def create_tmp_for_race_info(self):
        """
        raceページのhtmlを受け取って、レース情報テーブルに変換する関数。
        """
        process_bin_file(self, create_tmp_race_info)

    def create_race_info_table(self):
        """
        raceページのhtmlを受け取って、レース情報テーブルに変換する関数。
        """
        process_bin_file(self, create_raw_race_info)

    def create_race_return_table(self):
        """
        raceページのhtmlを受け取って、払い戻しテーブルに変換する関数。
        """
        process_bin_file(self, create_raw_race_return)

    def create_horse_results_table(self):
        """
        # horseページのhtmlを受け取って、馬の過去成績のDataFrameに変換する関数。
        """
        process_bin_file(self, create_raw_horse_results)

    def create_horse_info_table(self):
        """
        horseページのhtmlを受け取って、馬の基本情報のDataFrameに変換する関数。
        """

        process_bin_file(self, create_raw_horse_info)

    def scrape_peds_list(self):
        """
        horse/pedページのhtmlを受け取って、血統のDataFrameに変換する関数。
        """
        process_bin_file(self, create_raw_horse_ped)

    ################################################################################################################
    # この関数はまだ
    def create_table_for_predict(self):
        """
        scheduled_raceに保管したhtmlをスクレイピング。
        dateはyyyy/mm/ddの形式。
        """

        race_html_list = self.get_file_list(self.from_local_location)
        # print("race_html_list", race_html_list)

        data_index = 1
        race_return = {}
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
                    print("info", info)
                    df = pd.DataFrame()

                    # メインのテーブルの取得
                    for tr in soup.find_elements(By.CLASS_NAME, "HorseList"):
                        print("tr", tr)
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
                    print("df", df)
                    df.index = [race_id] * len(df)

                    # レース情報の取得
                    texts = soup.find_element(By.CLASS_NAME, "RaceList_Item02").text
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
                    if len(soup.find_elements(By.CLASS_NAME, "Icon_GradeType3")) > 0:
                        df["race_class"] = [Master.RACE_CLASS_LIST[6]] * len(df)
                    elif len(soup.find_elements(By.CLASS_NAME, "Icon_GradeType2")) > 0:
                        df["race_class"] = [Master.RACE_CLASS_LIST[7]] * len(df)
                    elif len(soup.find_elements(By.CLASS_NAME, "Icon_GradeType1")) > 0:
                        df["race_class"] = [Master.RACE_CLASS_LIST[8]] * len(df)

                    # 障害レースの場合
                    if hurdle_race_flg:
                        df["around"] = [Master.AROUND_LIST[3]] * len(df)
                        df["race_class"] = [Master.RACE_CLASS_LIST[9]] * len(df)

                    df["date"] = [date] * len(df)
                except Exception as e:
                    print(e)
                    break
                # finally:
                #    driver.close()
                #    driver.quit()

        # 取消された出走馬を削除
        df = df[df[Cols.WEIGHT_AND_DIFF] != "--"]
        df.to_pickle(file_path)

    # この関数はまだ

    def update_horse_table(self):
        """
        horse_htmlを受け取って、結果テーブルに変換する関数。data/html/horse_resultsに保存する

        """

        horse_html_list = self.get_file_list(self.from_local_location)

        data_index = 1
        print("creating horse_results_table")
        horse_results = {}
        for horse_html in tqdm(horse_html_list):
            horse_html_path = os.path.join(self.from_local_location, horse_html)
            with open(horse_html_path, "rb") as f:
                try:
                    # 保存してあるbinファイルを読み込む
                    html = f.read()
                    # パスから正規表現でhorse_id_listを取得
                    # horse_id_list = [re.findall(r"horse\(\d+).bin", horse_html)[0] for horse_html in horse_html_path]
                    # DataFrameにしておく
                    horse_id_df = pd.DataFrame({"horse_id": horse_html_list})

                    ### 取得日マスタの更新 ###
                    print("updating master")
                    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 現在日時を取得

                    pd.DataFrame(columns=["horse_id", "updated_at"])
                    # マスタを読み込み
                    master = pd.read_csv("horse_results.pickle", dtype=object)
                    # horse_id列に新しい馬を追加
                    new_master = master.merge(horse_id_df, on="horse_id", how="outer")
                    # マスタ更新
                    new_master.loc[new_master["horse_id"].isin(horse_id_list), "updated_at"] = now
                    # 列が入れ替わってしまう場合があるので、修正しつつ保存
                    self.target_data = new_master[["horse_id", "updated_at"]]
                    self.obtained_last_key = horse_html
                except Exception as e:
                    print("Error at {}: {}".format(horse_html_path, e))
                    break

            data_index += 1
        self.save_temp_file("horse_results_table")
        self.copy_files()
