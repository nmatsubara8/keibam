import os
import re

import pandas as pd
from bs4 import BeautifulSoup
from numpy import NaN
from tqdm import tqdm

from src.constants._master import Master


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
    # target_data_name = {}
    match = re.search(r"([^/]+)/$", self.to_location)
    if match:
        target_data_name = match.group(1)
    print(f"creating {target_data_name} table")

    # tqdmインスタンスの作成
    pbar = tqdm(total=total_files, desc="Processing Batches", unit="%", unit_scale=True, leave=True)

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
            temp_df = pd.concat([temp_df[key] for key in temp_df])

            # if self.alias == "race_results_table":
            #    self.target_data = trim_function(temp_df)
            self.target_data = temp_df

            self.save_temp_file("race_results_table")
            target_data_name = {}  # バッチ処理が完了したので辞書をクリア
            temp_df = []
        self.obtained_last_key = target_bin_files[-1]
        self.transfer_temp_file()
    print(f"# of processed files: {processed_files}")


def trim_function(temp_df):
    """
    process_bin_file()のヘルパー関数

    process_functionとして呼び出したエイリアスによって異なる後処理を定義している
    """
    # 列名に半角スペースがあれば除去する
    results_df = temp_df.str.replace(" ", "")
    return results_df


def create_raw_race_results(target_bin_file_path, target_data_name):
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
        # print(f"race_id:{race_id}")
        df.index = [race_id] * len(df)
        # print(f"df.index:{df.index}")
        df[race_id] = df.index
        # print(f"df:{df}")
        # print("type df:", type(df))
    return df


def create_raw_race_info(target_bin_file_path, target_data_name):
    target_data_name = {}
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

        race_id = re.findall(r"race\W(\d+).bin", target_bin_file_path)[0]
        df.index = [race_id] * len(df)
        target_data_name[race_id] = df

    return target_data_name

def create_raw_horse_results(target_bin_file_path, target_data_name):
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
            print("horse_results empty case1 {}".format(horse_html))
            continue

        horse_id = re.findall(r"(\d+).bin", horse_html)[0]

        df.index = [horse_id] * len(df)
        horse_results[horse_id] = df

        # 競走データが無い場合（新馬）を飛ばす
        except IndexError:
            continue

        # インデックスをhorse_idにする
        horse_id = re.findall(r"\d+", target_bin_file_path)[0]
        df.index = [horse_id] * len(df)
        df[horse_id] = df.index
    return df


def create_raw_horse_info(target_bin_file_path, target_data_name):
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

def create_raw_horse_ped(target_bin_file_path, target_data_name):
    with open(target_bin_file_path, "rb") as f:
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
        # インデックスをhorse_idにする
        horse_id = re.findall(r"\d+", target_bin_file_path)[0]
        df.index = [horse_id] * len(df)
        df[horse_id] = df.index
    return df

def create_raw_race_return(target_bin_file_path, target_data_name):
    with open(target_bin_file_path, "rb") as f:
        # 保存してあるbinファイルを読み込む
        html = f.read()
        html = html.replace(b"<br />", b"br")
        dfs = pd.read_html(html)

        # dfsの1番目に単勝〜馬連、2番目にワイド〜三連単がある
        df = pd.concat([dfs[1], dfs[2]])

        # インデックスをrace_idにする
        race_id = re.findall(r"\d+", target_bin_file_path)[0]
        # print(f"race_id:{race_id}")
        df.index = [race_id] * len(df)
        # print(f"df.index:{df.index}")
        df[race_id] = df.index
        # print(f"df:{df}")
        # print("type df:", type(df))
    return df
