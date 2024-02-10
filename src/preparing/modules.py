import os
import re

import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm


def bin_file_proccesing_common_setting(self, callback):
    target_bin_files = sorted(self.get_file_list(self.from_local_location))
    total_batches = (len(target_bin_files) + self.batch_size - 1) // self.batch_size  # バッチ数の計算
    total_files = len(target_bin_files)  # 処理対象の全データ数
    processed_files = 0  # 処理済みのファイル数
    match = re.search(r"([^/]+)/$", self.to_location)
    if match:
        target_data_name = match.group(1)
    print(f"creating {target_data_name} table")
    target_data_name = {}

    # tqdmインスタンスの作成
    pbar = tqdm(total=total_files, desc="Processing Batches", position=0, unit="%", unit_scale=True, leave=True)

    for batch_index in range(total_batches):
        start_index = batch_index * self.batch_size
        end_index = min((batch_index + 1) * self.batch_size, len(target_bin_files))
        batch_target_bin_files = target_bin_files[start_index:end_index]

        for target_bin_file in batch_target_bin_files:  # race_html binファイル
            target_bin_file_path = os.path.join(self.from_local_location, target_bin_file)

            callback(target_bin_file_path)

            processed_files += 1
            pbar.update(1)  # 処理済みのファイル数を1増やす


def create_raw_horse_results(target_bin_file_path, target_data_name):
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
        race_id = re.findall(r"race\W(\d+).bin", target_bin_file_path)[0]
        df.index = [race_id] * len(df)
        target_data_name[race_id] = df

    return target_data_name


def process_bin_file(self, process_function):
    """
    race_html binファイルを受け取って、レース結果テーブルに変換する関数。
    """

    target_bin_files = sorted(self.get_file_list(self.from_local_location))
    total_batches = (len(target_bin_files) + self.batch_size - 1) // self.batch_size  # バッチ数の計算
    total_files = len(target_bin_files)  # 処理対象の全データ数
    processed_files = 0  # 処理済みのファイル数
    match = re.search(r"([^/]+)/$", self.to_location)
    if match:
        target_data_name = match.group(1)
    print(f"creating {target_data_name} table")
    target_data_name = {}

    # tqdmインスタンスの作成
    pbar = tqdm(total=total_files, desc="Processing Batches", position=0, unit="%", unit_scale=True, leave=True)

    for batch_index in range(total_batches):
        start_index = batch_index * self.batch_size
        end_index = min((batch_index + 1) * self.batch_size, len(target_bin_files))
        batch_target_bin_files = target_bin_files[start_index:end_index]

        for target_bin_file in batch_target_bin_files:  # race_html binファイル
            target_bin_file_path = os.path.join(self.from_local_location, target_bin_file)

            try:
                temp_df = process_function(target_bin_file_path, target_data_name)

            except Exception as e:
                print("Error at {}: {}".format(target_bin_file_path, e))
                break

            processed_files += 1
            pbar.update(1)  # 処理済みのファイル数を1増やす

            self.target_data = trim_function(temp_df)
            self.save_temp_file("race_results_table")
            target_data_name.clear()  # バッチ処理が完了したので辞書をクリア
            temp_df = []
        self.obtained_last_key = target_bin_files[-1]
        self.transfer_temp_file()


def trim_function(target_data_name):
    # バッチごとに途中経過を保存
    race_results_df = pd.concat([target_data_name[key] for key in target_data_name])
    # 列名に半角スペースがあれば除去する
    race_results_df = race_results_df.rename(columns=lambda x: x.replace(" ", ""))
    return race_results_df
