"""netkeiba の保存済み HTML（bin）から raw テーブルを生成するパーサ群。

modules.py（旧・神モジュール）から分割。bin ファイルパス→DataFrame の純粋変換。
スクレイプ取得は _scrape_pages.py に分離。後方互換のため modules.py が両者を再 export する。
"""

import io
import logging
import os
import re
import pandas as pd
from src.constants._master import Master
from src.constants._master import classify_race_class


def _re_first_int(text: str, default: str = "0") -> str:
    """text から最初の連続数字を返す。見つからなければ default を返す。

    re.search(...).group() が None.group() でクラッシュするのを防ぐ
    （古い/特殊なレース名で数字が無いケースに対応）。
    """
    m = re.search(r"\d+", text or "")
    return m.group() if m else default


NaN = float("nan")


logger = logging.getLogger(__name__)


def trim_function(df):
    """
    process_bin_file()のヘルパー関数

    process_functionとして呼び出したエイリアスによって異なる後処理を定義している
    """
    # 列名に半角スペースがあれば除去する
    trimmed_df = df.columns = df.columns.str.replace(r"\s+", "")
    return trimmed_df


def create_raw_race_results(target_bin_file_path):
    from bs4 import BeautifulSoup
    race_results = {}
    with open(target_bin_file_path, "rb") as f:
        # 保存してあるbinファイルを読み込む
        html = f.read()

        # メインとなるレース結果テーブルデータを取得。
        # 旧年代（〜1990s）のページは空テーブルを含み、全テーブル一括の read_html が
        # IndexError で落ちるため、summary 属性で対象テーブルだけを解析する
        # （summary="レース結果" は 1986〜現在まで全年代で共通）。
        df = pd.read_html(io.BytesIO(html), attrs={"summary": "レース結果"})[0]

        # htmlをsoupオブジェクトに変換
        soup = BeautifulSoup(html, "lxml")

        # 馬・騎手・調教師・馬主の ID をスクレイピング。
        # 旧年代（1970s 等）はリンクの無い行（未登録馬・データ未整備）があり、
        # テーブル一括の find_all では行数と ID 数がずれるため、行単位で対応付ける
        # （リンクが無いセルは None）。また旧 horse_id は "1972z00735" のような
        # 英字混じりがあるため \d+ ではなく URL パスから \w+ で抽出する。
        result_table = soup.find("table", attrs={"summary": "レース結果"})
        body_rows = [tr for tr in result_table.find_all("tr") if tr.find("td") is not None]
        if len(body_rows) != len(df):
            raise ValueError(
                f"レース結果テーブルの行数不一致 rows={len(body_rows)} df={len(df)}: {target_bin_file_path}"
            )

        def _row_id(tr, href_pattern, id_regex):
            a = tr.find("a", attrs={"href": re.compile(href_pattern)})
            if a is None:
                return None
            m = re.search(id_regex, a["href"])
            return m.group(1) if m else None

        df["horse_id"] = [_row_id(tr, "^/horse", r"/horse/(\w+)") for tr in body_rows]
        df["jockey_id"] = [_row_id(tr, "^/jockey", r"jockey/result/recent/(\w+)") for tr in body_rows]
        df["trainer_id"] = [_row_id(tr, "^/trainer", r"trainer/result/recent/(\w+)") for tr in body_rows]
        df["owner_id"] = [_row_id(tr, "^/owner", r"owner/result/recent/(\w+)") for tr in body_rows]

        race_id = re.findall(r"\d+", os.path.basename(target_bin_file_path))[0]
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


def convert_string(value):
    # 旧年代（〜1990s）の馬戦績は 開催 列が欠損（NaN=float）のことがあるため、
    # 文字列以外はそのまま返す（地方・海外遠征のない時代の欠損値）。
    if not isinstance(value, str):
        return value
    # 正規表現パターンを定義
    pattern = r"\d{0,2}([^\d]+)\d{0,2}"
    match = re.search(pattern, value)
    if match:
        return match.group(1)  # マッチした部分の文字列を返す
    else:
        return value  # マッチしなかった場合は元の値を返す


def create_raw_horse_results(target_bin_file_path):
    horse_result = {}
    with open(target_bin_file_path, "rb") as f:
        # 保存してあるbinファイルを読み込む
        html = f.read()

        html_str = html.decode("utf-8", errors="replace")
        df = pd.read_html(io.StringIO(html_str))[3]
        # 受賞歴がある馬の場合、3番目に受賞歴テーブルが来るため、4番目のデータを取得する
        if df.columns[0] == "受賞歴":
            df = pd.read_html(io.StringIO(html_str))[4]
            # print(f"test df:{df.iloc[:,1]}")

        # 新馬の競走馬レビューが付いた場合、
        # 列名に0が付与されるため、次のhtmlへ飛ばす
        if df.columns[0] == 0:
            logger.warning("horse_results empty case1 %s", target_bin_file_path)
            # continue

        # インデックスをhorse_idにする
        horse_id = re.findall(r"\d+", os.path.basename(target_bin_file_path))[0]
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


def create_raw_horse_info(target_bin_file_path):
    from bs4 import BeautifulSoup
    horse_info = {}
    with open(target_bin_file_path, "rb") as f:
        # 保存してあるbinファイルを読み込む
        html = f.read()

        # 馬の基本情報を取得（bin は UTF-8 で保存されているため StringIO で解析）
        html_str = html.decode("utf-8", errors="replace")
        tables = pd.read_html(io.StringIO(html_str))
        # プロフィールテーブルは「生年月日」行を持つ 2 列テーブルで特定する。
        # JS 描画後のページは 2 列テーブルが複数あり（次走予定・血統ミニ表等）、
        # 「最初の 2 列テーブル」では血統表を誤って拾うことがある。
        profile_table = None
        for t in tables:
            if t.shape[1] == 2 and (t.iloc[:, 0].astype(str) == "生年月日").any():
                profile_table = t
                break
        if profile_table is None:
            for t in tables:
                if t.shape[1] == 2 and t.iloc[:, 0].dtype == object:
                    profile_table = t
                    break
        if profile_table is None:
            if len(tables) < 2:
                raise IndexError(f"馬プロフィールテーブルが見つかりません: {target_bin_file_path}")
            profile_table = tables[1]
        df = profile_table.set_index(0).T

        # htmlをsoupオブジェクトに変換
        soup = BeautifulSoup(html_str, "lxml")
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
        horse_id = re.findall(r"\d+", os.path.basename(target_bin_file_path))[0]
        df.index = [horse_id] * len(df)
        df["horse_id"] = df.index
        df["horse_id"].astype(int)

        horse_info[horse_id] = df
        # 各レースの結果データフレームを結合して race_results_df を生成
        horse_info_df = pd.concat([horse_info[key] for key in horse_info])
        horse_info_df.set_index("horse_id", inplace=True)

    return horse_info_df


def create_raw_horse_ped(target_bin_file_path):
    from bs4 import BeautifulSoup
    horse_ped = {}
    with open(target_bin_file_path, "rb") as f:
        # 保存してあるbinファイルを読み込む
        html = f.read()
        # horse_idを取得

        # htmlをsoupオブジェクトに変換
        horse_id = re.findall(r"\d+", os.path.basename(target_bin_file_path))[0]
        html_str = html.decode("utf-8", errors="replace")
        soup = BeautifulSoup(html_str, "html.parser")
        df = pd.DataFrame()
        peds_id_list = []
        # 血統データからhorse_idを取得する
        ped_table = soup.find("table", attrs={"summary": "5代血統表"})
        if ped_table is None:
            # Try finding by class or other attributes for newer page formats
            ped_table = soup.find("table", class_="blood_table")
        if ped_table is None:
            raise ValueError(f"血統テーブルが見つかりません: {target_bin_file_path}")
        # 血統セルの馬リンクは相対（/horse/<id>）と絶対（https://db.netkeiba.com/horse/<id>/）
        # の両形式がある。各祖先には /horse/ped/<id>/・/horse/sire/<id>/ も併設される
        # ため、パスが /horse/<id> で終わる直接リンクだけを対象にする。
        ped_href_re = re.compile(r"(?:^|netkeiba\.com)/horse/(\w{10})/?$")
        horse_a_list = ped_table.find_all("a", attrs={"href": ped_href_re})

        for a in horse_a_list:
            # 血統データのhorse_idを抜き出す
            m = ped_href_re.search(a["href"])
            if m is None:
                continue
            peds_id_list.append(m.group(1))

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


def create_raw_race_return(target_bin_file_path):
    race_return = {}
    with open(target_bin_file_path, "rb") as f:
        # 保存してあるbinファイルを読み込む
        html = f.read()
        html_bytes = html.replace(b"<br />", b"br")
        # 払戻テーブルは summary="払い戻し" で特定する（1986〜現在まで全年代で共通、
        # 通常 2 表: 単勝〜馬連 / ワイド〜三連単。旧年代は馬券種が少ない）。
        # テーブル位置 (dfs[1], dfs[2]) 前提だと旧ページの空テーブルで崩れる。
        dfs = pd.read_html(io.BytesIO(html_bytes), attrs={"summary": "払い戻し"})
        df = pd.concat(dfs)
        # 列名を整数に変更

        # インデックスをrace_idにする
        race_id = re.findall(r"\d+", os.path.basename(target_bin_file_path))[0]
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
        data_intro = soup.find("div", attrs={"class": "data_intro"})
        if data_intro is None:
            raise ValueError(f"data_intro not found (cancelled race?): {target_bin_file_path}")
        text1 = data_intro.find_all("p")[0].text
        text2 = data_intro.find_all("p")[1].text
        # print(f"text1:{text1}")
        # print(f"text2:{text2}")
        race_id = str(re.findall(r"\d+", os.path.basename(target_bin_file_path))[0])
        # print(f"race_id :{race_id}")
        # netkeiba に実体のない空ページ（race_name 空・日付 1970-01-01 等）は
        # 中止/欠番レースと同様にクリーンにスキップする。各種パース（天候など）の
        # 前に判定し、空ページに対する無意味な警告や place_id 未確定クラッシュを防ぐ。
        race_date = text2.split(" ")[0]
        race_name = text2.split(" ")[1]
        if not race_name.strip():
            raise ValueError(f"empty race page (cancelled/invalid?): {target_bin_file_path}")
        # テキスト情報を解析してDataFrameに変換
        race_distance = _re_first_int(text1.split("/")[0])
        weather = text1.split("/")[1].split(":")[1].strip()

        if weather in Master.WEATHER_LIST:
            pass
        else:
            logger.warning("unknown weather definition appeared:%s", race_id)

        race_type = text1.split("/")[2].split(":")[0].strip()
        # 発走時刻
        start_time = text1.split("/")[-1].split(":")[1:3]
        start_time = ":".join(start_time).strip().split("\n\n")[0]

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
        race_day_count = _re_first_int(race_name.split("日目")[0])  # 開催日数
        race_round_count = _re_first_int(race_name.split("回")[0])  # 開催回数

        # 開催場所を取得
        # 未知の開催場所（海外・地方の表記揺れ等）でも UnboundLocalError で
        # クラッシュせず DataFrame 構築まで到達できるよう NaN で初期化する
        # （ground_state と同じ防御パターン）。未知時のみ警告を残す。
        place_id = NaN
        place_name = NaN
        for key, value in Master.PLACE_DICT.items():
            if key in race_name:
                place_id = value
                place_name = key
        if place_name is NaN:
            logger.warning("unknown place definition appeared:%s", race_id)
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

            # レースクラスを取得。グレード(G1/GⅠ/Ｇ３)・リステッド(L) は race_name に、
            # 条件戦(1勝クラス/未勝利/500万下…) は race_condition に現れる。両方を
            # classify_race_class（NFKC + 正規表現で全角・ローマ数字・(L)・旧称を吸収）に
            # かけ、グレード/L を優先して取りこぼしを防ぐ。
            race_class_info = classify_race_class(race_name) or classify_race_class(race_condition)
            if race_class_info is None:
                # 後方互換フォールバック: 旧 RACE_CLASS_LIST 部分一致 + 旧称エイリアス
                for race_class in Master.RACE_CLASS_LIST:
                    if race_class in race_condition:
                        race_class_info = race_class
                if race_class_info is None:
                    for legacy, modern in Master.RACE_CLASS_LEGACY_ALIASES.items():
                        if legacy in race_condition:
                            race_class_info = modern
                            break
            if race_class_info is None:
                logger.warning("unknown race_class definition appeared:%s", race_id)
            # 向きを取得
            if (around_info is None) and (
                ("障害" in (race_class_info or "") or race_condition) or dart
            ):
                around_info = "直線"

            for key, value in Master.RACE_CONDITION_DICT.items():
                if key in race_condition:
                    race_flags[value] = 1
                    race_condition = race_condition.replace(key, "").strip()
                else:
                    race_flags[value] = 0

        if "歳以上" in race_condition:
            age = _re_first_int(race_condition.split("歳以上")[0]) + "+"

        else:
            age = _re_first_int(race_condition.split("歳")[0])
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
