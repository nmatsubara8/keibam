import csv
import json
import logging
import os
import pickle
import shutil

import numpy as np
import pandas as pd

from src.constants._url_paths import UrlPaths

logger = logging.getLogger(__name__)


class DataLoader:
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
        from_date="",
        to_date="",
    ):
        self.alias = alias
        self.from_location = from_location
        self.to_temp_location = to_temp_location
        self.temp_save_file_name = temp_save_file_name
        self.to_location = to_location
        self.save_file_name = save_file_name
        self.batch_size = batch_size
        self.from_local_location = from_local_location
        self.from_local_file_name = from_local_file_name
        target_data = []
        self.processing_id = processing_id
        self.obtained_last_key = obtained_last_key

        self.target_data = target_data
        self.skip = skip
        self.from_date = from_date
        self.to_date = to_date

    # プロジェクトルート（DataLoader.py の 3階層上）を絶対パスで保持
    _PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    @classmethod
    def _abs(cls, path: str) -> str:
        """相対パスをプロジェクトルート基準の絶対パスへ変換する。"""
        if not path:
            return path
        if os.path.isabs(path):
            return path
        return os.path.normpath(os.path.join(cls._PROJECT_ROOT, path))

    def set_args(self, alias):
        # クラスの属性を取得
        url_paths = UrlPaths()
        attributes = [attr for attr in dir(url_paths) if not attr.startswith("_")]
        # クラスの属性をたどって、alias_listを作成
        alias_list = [getattr(url_paths, attr)[0] for attr in attributes if isinstance(getattr(url_paths, attr), tuple)]

        # タプルの[0]の中にaliasと等しいものがあれば
        if alias in alias_list:
            # 該当する属性のタプルを取得
            attr = [attr for attr in attributes if getattr(url_paths, attr)[0] == alias][0]
            self.alias = alias
            # エイリアスが表すデータの取得先をセット
            self.from_location = getattr(url_paths, attr)[1]
            # エイリアスが表すデータの一時保存先をセット
            self.to_temp_location = self._abs(getattr(url_paths, attr)[2])
            # エイリアスが表すデータの一時保存先ファイル名をセット
            self.temp_save_file_name = getattr(url_paths, attr)[3]
            # エイリアスが表すデータの正本保存先をセット
            self.to_location = self._abs(getattr(url_paths, attr)[4])
            # エイリアスが表すデータの正本ファイル名をセット
            self.save_file_name = getattr(url_paths, attr)[5]
            self.batch_size = getattr(url_paths, attr)[6]
            # エイリアスが表すデータが参照する必要がある外部キーを保有する
            # ローカルファイル（フォルダ名とファイル名）をセット
            self.from_local_location = self._abs(getattr(url_paths, attr)[7])
            self.from_local_file_name = getattr(url_paths, attr)[8]
            # 異常終了時に使うskip処理を実施させるためのキーとフラグ
            self.skip = getattr(url_paths, attr)[10]  # デフォルトはFalse
            if not self.skip:
                self.obtained_last_key = getattr(url_paths, attr)[9]

            # 処理対象データ範囲を指定する
            self.from_date = getattr(url_paths, attr)[11]
            self.to_date = getattr(url_paths, attr)[12]

        else:
            logger.warning("No such data")

        # skip対象ではないゴミファイルの掃除
        if not self.skip:
            self.delete_files_both()

        self.pre_process_display()

    def get_filetype(self):
        text = self.temp_save_file_name
        if text.endswith("_table.csv"):
            filetype = "df"
        elif text.endswith(".txt"):
            filetype = "txt"
        elif text.endswith(".pkl"):
            filetype = "pkl"
        elif text.endswith("html"):
            filetype = "html"
        elif text.endswith(".h5"):
            filetype = "h5"
        elif text.endswith(".csv"):
            filetype = "csv"
        elif text.endswith(".bin"):
            filetype = "bin"

        return filetype

    def get_local_temp_file_path(self):
        if self.get_filetype() != "bin":
            local_temp_path = os.path.join(self.to_temp_location, self.temp_save_file_name)
        else:
            # print("self.processing_id:", self.processing_id)
            local_temp_path = os.path.join(self.to_temp_location, str(self.processing_id) + ".bin")
        return local_temp_path

    def get_local_comp_file_path(self, alias):
        local_comp_path = os.path.join(self.to_location, self.save_file_name)
        return local_comp_path

    def get_local_comp_path(self, alias):
        return self.to_location

    def save_temp_file(self, alias):
        # ローカル一時保存用ファイルのパス
        local_path = self.get_local_temp_file_path()
        filetype = self.get_filetype()
        if not os.listdir(self.to_temp_location):
            mode = "w"
        else:
            mode = "a"
        if filetype == "csv":
            if not os.listdir(self.to_temp_location):
                mode = "w"
            else:
                mode = "a"
                # CSVファイルにデータを書き込む処理
            with open(local_path, mode=mode, index=True, newline="\n") as csv_file:
                json.dump(self.target_data, csv_file)
                # self.obtained_last_key = self.target_data[-1]

        elif filetype == "txt":
            if not os.listdir(self.to_temp_location):
                mode = "w"
            else:
                mode = "a"
            # TXTファイルにデータを書き込む処理
            with open(local_path, mode=mode, index=False, newline="\n") as txt_file:
                # txt_file.write(self.alias + "\n")
                for item in self.target_data:
                    txt_file.write(str(item) + "\n")

        elif filetype == "pkl":
            if not os.listdir(self.to_temp_location):
                mode = "wb"
            else:
                mode = "ab"
            # pickleファイルにデータを書き込む処理
            with open(local_path, mode=mode) as pkl_file:
                pickle.dump(self.target_data, pkl_file)

        elif filetype == "bin":
            if not os.listdir(self.to_temp_location):
                mode = "wb"
            else:
                mode = "ab"
            # HTMLデータから実際のHTML部分を抽出する正規表現パターン
            # html_pattern = re.compile(r"b'(.+)'", re.DOTALL)
            file_path = self.get_local_temp_file_path()  # ファイルパスを取得
            with open(file_path, mode=mode) as bin_file:  # バイナリモードでファイルを開く
                bin_file.write(self.target_data)  # アイテムをファイルに書き込む

        elif filetype == "html":
            if not os.listdir(self.to_temp_location):
                mode = "wb"
            else:
                mode = "ab"

            # ファイルにデータを書き込む処理
            with open(local_path, mode=mode) as html_file:
                html_file.write(self.target_data)
                # self.obtained_last_key = self.target_data[-1]
        elif filetype == "df":
            # CSVファイルに保存
            os.makedirs(self.to_temp_location, exist_ok=True)
            temp_csv = os.path.join(self.to_temp_location, self.temp_save_file_name)
            if not os.path.exists(temp_csv):
                header = True
                mode = "w"
            else:
                header = False
                mode = "a"

            self.target_data.to_csv(temp_csv, header=header, index=True, mode=mode)

        elif filetype == "h5":
            # ファイルにデータを書き込む処理
            # HDF5形式で保存
            self.target_data.to_hdf(
                os.path.join(self.to_location, self.save_file_name), key=self.target_data.index, mode="a"
            )
            # self.obtained_last_key = self.target_data[-1]
        else:
            logger.error("Unsupported filetype. Please choose 'csv', 'txt', or 'pkl'.")

    def csv_reader(self, local_temp_file_path):
        if self.alias == "race_results_table":
            df = pd.read_csv(local_temp_file_path, dtype={"jockey_id": str, "trainer_id": str, "owner_id": str})

        # if self.alias == "horse_results_table":
        #    df = pd.read_csv(local_temp_file_path)
        # NaNを0に置き換える

        if self.alias == "race_info_table":
            df = pd.read_csv(local_temp_file_path)

        if self.alias == "horse_info_table":
            df = pd.read_csv(local_temp_file_path, dtype={"owner_id": str, "breeder_id": str})

        else:
            df = pd.read_csv(local_temp_file_path)

        return df

    def transfer_temp_file(self):
        local_temp_file_path = self.get_local_temp_file_path()
        new_df = self.csv_reader(local_temp_file_path)

        to_target_file = self.get_local_comp_file_path(self.alias)
        # 既存データとマージ（新データ優先）してから保存する。
        # マージ元は ./data/raw/ の本番 pkl を最優先（全 Processor の正本）。
        # data/html/ 側のキャッシュ pkl は gitignore 対象で fresh 環境に存在しないため、
        # ここを見ないと既存全件が新規スクレイプ分だけで上書きされる（データ喪失）。
        existing = None
        raw_path = os.path.join("./data/raw", self.save_file_name)
        for candidate in (raw_path, to_target_file):
            if os.path.exists(candidate):
                try:
                    existing = pd.read_pickle(candidate)
                    break
                except Exception as e:
                    logger.warning("transfer: 既存 pkl 読込失敗 %s: %s", candidate, e)
        if existing is not None and isinstance(existing, pd.DataFrame) and not existing.empty:
            # マージキーは行の所属単位（レース系: race_id / 馬系: horse_id）。
            # 旧実装の columns[-1]（owner_id 等）では同一馬主の既存行まで落ちていた。
            key_col = next((k for k in ("race_id", "horse_id") if k in new_df.columns), None)
            if key_col is not None and key_col in existing.columns:
                old_only = existing[~existing[key_col].isin(new_df[key_col])]
                new_df = pd.concat([old_only, new_df], ignore_index=True)
            else:
                # キー不明でも既存データは捨てない（重複の可能性より喪失の方が重い）
                logger.warning(
                    "transfer: マージキー(race_id/horse_id)が見つからないため既存 %d 行へ単純追記します",
                    len(existing),
                )
                new_df = pd.concat([existing, new_df], ignore_index=True)
        logger.info("transfer: saved %d rows → %s", len(new_df), to_target_file)
        new_df.to_pickle(to_target_file)

    def copy_files(self):
        files = os.listdir(self.to_location)
        for file in files:
            source_path = os.path.join(self.to_location, file)
            destination_path = os.path.join("./data/raw", file)
            # Copy the file from source to destination
            try:
                shutil.copy2(source_path, destination_path)
                logger.info("File %s copied successfully.", file)
            except FileNotFoundError:
                logger.warning("File %s not found.", file)
            except IOError as e:
                logger.error("Error copying file %s: %s", file, e)

    def load_file_pkl(self):
        if (self.from_local_location and self.from_local_file_name) is not None:
            target_file_path = os.path.join(self.from_local_location, self.from_local_file_name)
            with open(target_file_path, "rb") as f:
                # 最初のデータを無視して読み込む

                # 残りのデータを読み込む
                data = pd.DataFrame()
                data = pickle.load(f)
                # pandas 3.x では Arrow 文字列列に int64 を直接代入できないため
                # numpy array 経由で型変換する
                last_col = data.columns[-1]
                data[last_col] = pd.to_numeric(data[last_col], errors="coerce").fillna(0).astype(int).values
                logger.debug("load_file_pkl:row type int:%s", data.dtypes)
                logger.debug("type of data_Loaded: %s", type(data))
                logger.debug("load_file_pkl:data.head: %s", data.head())
                if len(data) == 0:
                    logger.warning("load_file_pkl: data is empty (path=%s)", target_file_path)
                    return data
                logger.debug("1st data: %s", data.iloc[0].values[0])
                if not self.skip:
                    loaded_list = data
                    logger.info("start from scratch")
                else:  # skip=True時のリスト範囲限定処理
                    try:
                        # ファイルfから1行ずつ読み込んで、文字列としてリストに追加する
                        # lines = [line.strip() for line in data]
                        # print(f"lines:{lines}")
                        ###########################################
                        ###########################################
                        ###########################################
                        ###########################################
                        target_number = np.int64(202406010103)  # 文字列型に変換する

                        ###########################################
                        ###########################################
                        ###########################################
                        ###########################################

                        logger.debug("length of data:%s", len(data))
                        logger.debug("shape of data:%s", data.shape)
                        index = 0
                        for idx in range(0, len(data) - 1):
                            preprocessed_data = np.int64(data.iloc[idx].values[0])
                            if target_number == preprocessed_data:
                                logger.debug("%s:%s", target_number, data.iloc[idx].values[0])
                                index = idx
                                logger.debug("matchde index:%s", index)
                        if index != 0:
                            # 範囲外の場合や最後の要素の場合に注意
                            length = len(data)
                            if index < length:
                                loaded_list = data[index:length]
                                logger.debug("re-entered from %s to %s", data.iloc[index:], data.iloc[:length])
                                logger.info(
                                    "length of reloaded files is %s. / originally %s",
                                    len(loaded_list),
                                    len(data),
                                )
                                self.skip = False
                                return loaded_list
                            else:
                                logger.warning("指定したIDがリストの最後にあります。")

                        else:
                            logger.warning("指定したIDがリスト内に見つかりません。")
                    except ValueError:
                        logger.error("ファイルからの読み込み中にエラーが発生しました。")
                return loaded_list

    def load_file_txt(self):
        if (self.from_local_location and self.from_local_file_name) is not None:
            target_file_path = os.path.join(self.from_local_location, self.from_local_file_name)
            with open(target_file_path, "rb", encoding="UTF-8") as f:
                if not self.skip:
                    loaded_list = [line.strip() for line in f]
                else:  # skip=True時のリスト範囲限定処理
                    try:
                        lines = f.readlines()
                        ids = [int(line.split()[0]) for line in lines]
                        index = ids.index(self.obtained_last_key)
                        # 範囲外の場合や最後の要素の場合に注意
                        if index < len(ids) - 1:
                            f.seek(0)  # Rewind the file to the beginning
                            for _ in range(index + 1):
                                next(f)
                            loaded_list = [line.strip() for line in f]
                        else:
                            logger.warning("指定したIDがリストの最後にあります。")
                            loaded_list = []
                    except UnicodeDecodeError:
                        logger.warning("指定したIDがリスト内に見つかりません。")
                        loaded_list = []
            self.skip = False
            return loaded_list

    def load_file_csv(self):
        if (self.from_local_location and self.from_local_file_name) is not None:
            target_file_path = os.path.join(self.from_local_location, self.from_local_file_name)
            with open(target_file_path, "r", newline="") as f:
                reader = csv.reader(f)
                if not self.skip:
                    loaded_list = [row for row in reader]
                else:  # skip=True時のリスト範囲限定処理
                    try:
                        ids = [int(row[0]) for row in reader]
                        index = ids.index(self.obtained_last_key)
                        # 範囲外の場合や最後の要素の場合に注意
                        if index < len(ids) - 1:
                            f.seek(0)  # Rewind the file to the beginning
                            for _ in range(index + 1):
                                next(reader)
                            loaded_list = [row for row in reader]
                        else:
                            logger.warning("指定したIDがリストの最後にあります。")
                            loaded_list = []
                    except ValueError:
                        logger.warning("指定したIDがリスト内に見つかりません。")
                        loaded_list = []
            self.skip = False
            return loaded_list

    def get_file_list(self, location):
        file_list = []
        # 指定したフォルダ内の全てのファイルおよびディレクトリのリストを取得
        items = os.listdir(location)

        for item in items:
            # .bin ファイルのみリストに追加（.gitkeep 等を除外）
            if os.path.isfile(os.path.join(self.from_local_location, item)) and item.endswith(".bin"):
                file_list.append(item)
        return file_list

    def delete_files_both(self):
        os.makedirs(self.to_temp_location, exist_ok=True)
        os.makedirs(self.to_location, exist_ok=True)
        files_to_temp = os.listdir(self.to_temp_location)
        files_to = os.listdir(self.to_location)
        # ファイルを削除
        for file in files_to_temp:
            file_path_to_temp = os.path.join(self.to_temp_location, file)
            try:
                if os.path.isfile(file_path_to_temp):
                    os.remove(file_path_to_temp)
            except Exception as e:
                logger.error("Error deleting %s: %s", file_path_to_temp, e)
        for file in files_to:
            file_path_to = os.path.join(self.to_location, file)
            try:
                if os.path.isfile(file_path_to):
                    os.remove(file_path_to)
            except Exception as e:
                logger.error("Error deleting %s: %s", file_path_to, e)

    def delete_files_tmp(self):
        files_to_temp = os.listdir(self.to_temp_location)

        # ファイルを削除
        for file in files_to_temp:
            file_path_to_temp = os.path.join(self.to_temp_location, file)
            try:
                if os.path.isfile(file_path_to_temp):
                    os.remove(file_path_to_temp)
            except Exception as e:
                logger.error("Error deleting %s: %s", file_path_to_temp, e)

    def pre_process_display(self):  # 処理開始時のメッセージ出力
        logger.info("処理対象:%s 開始", self.alias)
        logger.info("self.from_location: %s", self.from_location)
        logger.info("to_temp_location: %s", self.to_temp_location)
        logger.info("to_location: %s", self.to_location)
        if self.from_local_location != "":
            logger.info("self.from_local_location: %s", self.from_local_location)
        if self.from_local_file_name != "":
            logger.info("self.from_local_file_name: %s", self.from_local_file_name)
        logger.info("batch_size: %s", self.batch_size)
        if self.from_local_file_name != "":
            logger.info("reloaded_%s type: %s", self.from_local_file_name, self.from_local_file_name)

    def post_process_display(self):  # 処理終了時のメッセージ出力
        logger.info("%starget_df[-5:]: %s", self.alias, self.target_data[-5:])
        logger.info("%s type: %s", self.alias, type(self.target_data))
        logger.info("len: %s", len(os.path.join(self.to_location, self.save_file_name)))
        logger.info("Done / obtained_last_key: %s", self.obtained_last_key)
        logger.info("新規作成: %s -> %s 終了", self.temp_save_file_name, self.save_file_name)


class CustomDataLoader:
    """Lightweight data loader for local file access (no scraping required)."""

    def __init__(
        self,
        from_location: str = "./data/",
        load_file_name: str = "",
        to_location: str = "./data/",
        save_file_name: str = "output.pkl",
    ):
        self.from_location = from_location
        self.load_file_name = load_file_name
        self.to_location = to_location
        self.save_file_name = save_file_name
        self.save_file_path = os.path.join(to_location, save_file_name)
        self._data: pd.DataFrame = pd.DataFrame()

    def load_data_from_local(self) -> pd.DataFrame:
        """Load data from local file and save to save_file_path."""
        src_path = os.path.join(self.from_location, self.load_file_name)
        if os.path.exists(src_path):
            if src_path.endswith(".pkl"):
                self._data = pd.read_pickle(src_path)
            else:
                self._data = pd.DataFrame()
        else:
            self._data = pd.DataFrame()
        os.makedirs(self.to_location, exist_ok=True)
        self._data.to_pickle(self.save_file_path)
        return self._data


"""
    def update_rawdata(filepath: str, new_df: pd.DataFrame) -> pd.DataFrame:

    filepathにrawテーブルのpickleファイルパスを指定し、new_dfに追加したいDataFrameを指定。
    元々のテーブルにnew_dfが追加されてpickleファイルが更新される。
    pickleファイルが存在しない場合は、filepathに新たに作成される。

    # pickleファイルが存在する場合の更新処理
    if os.path.isfile(filepath):
        backupfilepath = filepath + '.bak'
        # 結合データがない場合
        if new_df.empty:
            logger.warning("preparing update raw data empty")
        else:
            # 元々のテーブルを読み込み
            filedf = pd.read_pickle(filepath)
            # new_dfに存在しないindexのみ、旧データを使う
            filtered_old = filedf[~filedf.index.isin(new_df.index)]
            # bakファイルが存在する場合
            if os.path.isfile(backupfilepath):
                os.remove(backupfilepath)
            # バックアップ
            os.rename(filepath, backupfilepath)
            # 結合
            updated = pd.concat([filtered_old, new_df])
            # 保存
            updated.to_pickle(filepath)
    else:
        # pickleファイルが存在しない場合、新たに作成
        new_df.to_pickle(filepath)
"""
