import re

import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

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
        target_data=None,
        processing_id="",
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
            processing_id,
            obtained_last_key,
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

    race_html_list = self.get_file_list()

    data_index = 1

    print("creating race_results_table")
    race_results = {}
    for race_html in tqdm(race_html_list):
        with open(race_html, "rb") as f:
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
                race_id = re.findall(r"race\W(\d+).bin", race_html)[0]
                df.index = [race_id] * len(df)

                race_results[race_id] = df
                data_index += 1
                self.obtained_last_key = race_html
            except Exception as e:
                print("error at {}".format(race_html))
                print(e)

    # pd.DataFrame型にして一つのデータにまとめる
    race_results_df = pd.concat([race_results[key] for key in race_results])

    # 列名に半角スペースがあれば除去する
    self.target_data = race_results_df.rename(columns=lambda x: x.replace(" ", ""))
    self.save_temp_file("race_results_table")
    self.transfer_temp_file()
