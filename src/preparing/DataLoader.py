import csv
import json
import os
import pickle
import shutil

from src.constants._url_paths import UrlPaths


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
            self.to_temp_location = getattr(url_paths, attr)[2]
            # エイリアスが表すデータの一時保存先ファイル名をセット
            self.temp_save_file_name = getattr(url_paths, attr)[3]
            # エイリアスが表すデータの正本保存先をセット
            self.to_location = getattr(url_paths, attr)[4]
            # エイリアスが表すデータの正本ファイル名をセット
            self.save_file_name = getattr(url_paths, attr)[5]
            self.batch_size = getattr(url_paths, attr)[6]
            # エイリアスが表すデータが参照する必要がある外部キーを保有する
            # ローカルファイル（フォルダ名とファイル名）をセット
            self.from_local_location = getattr(url_paths, attr)[7]
            self.from_local_file_name = getattr(url_paths, attr)[8]
            # 異常終了時に使うskip処理を実施させるためのキーとフラグ
            self.obtained_last_key = getattr(url_paths, attr)[9]
            self.skip = getattr(url_paths, attr)[10]  # デフォルトはFalse
            # 処理対象データ範囲を指定する
            self.from_date = getattr(url_paths, attr)[11]
            self.to_date = getattr(url_paths, attr)[12]

        else:
            print("No such data")

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

    def get_local_temp_file_path(self, alias):
        if self.get_filetype() != "html":
            local_temp_path = os.path.join(self.to_temp_location, self.temp_save_file_name)
        else:
            # print("self.processing_id:", self.processing_id)
            local_temp_path = os.path.join(self.to_temp_location, self.processing_id + ".bin")
        return local_temp_path

    def get_local_comp_file_path(self, alias):
        local_comp_path = os.path.join(self.to_location + self.save_file_name)
        return local_comp_path

    def get_local_comp_path(self, alias):
        return self.to_location

    def save_temp_file(self, alias):
        # ローカル一時保存用ファイルのパス
        local_path = self.get_local_temp_file_path(self.alias)
        filetype = self.get_filetype()

        if filetype == "csv":
            # CSVファイルに保存
            if not os.listdir(self.to_temp_location):
                mode = "w"
            else:
                mode = "a"

            # CSVファイルにデータを書き込む処理
            with open(local_path, mode=mode, newline="\n") as csv_file:
                json.dump(self.target_data, csv_file)
                self.obtained_last_key = self.target_data[-1]

        elif filetype == "txt":
            # テキストファイルにデータを書き込む処理
            # リストをファイルに保存
            with open(local_path, "a") as file:
                for item in self.target_data:
                    file.write("%s\n" % item)
                    self.obtained_last_key = self.target_data[-1]

        elif filetype == "pkl":
            # pickleファイルにデータを書き込む処理
            with open(local_path, "ab") as pkl_file:
                pickle.dump(self.target_data, pkl_file)
                self.obtained_last_key = self.target_data[-1]

        elif filetype == "html":
            # ファイルにデータを書き込む処理
            with open(local_path, "ab") as html_file:
                html_file.write(self.target_data)
                self.obtained_last_key = self.target_data[-1]
        elif filetype == "df":
            # CSVファイルに保存
            if not os.listdir(self.to_temp_location):
                header = True
            else:
                header = False

            self.target_data.to_csv(
                os.path.join(self.to_temp_location, self.temp_save_file_name), header=header, index=True, mode="a"
            )
            self.obtained_last_key = self.target_data.index[-1]

        elif filetype == "h5":
            # ファイルにデータを書き込む処理
            # HDF5形式で保存
            self.target_data.to_hdf(
                os.path.join(self.to_location, self.save_file_name), key=self.target_data.index, mode="a"
            )
            self.obtained_last_key = self.target_data[-1]
        else:
            print("Unsupported filetype. Please choose 'csv', 'txt', or 'pkl'.")

    def transfer_temp_file(self):
        from_target_file = self.get_local_temp_file_path(self.alias)
        with open(from_target_file, "r") as file:
            from_target_file = [line.strip() for line in file]

        to_target_file = self.get_local_comp_file_path(self.alias)

        with open(to_target_file, "ab") as pkl_file:
            pickle.dump(from_target_file, pkl_file)

    def copy_files(self):
        files = os.listdir(self.to_temp_location)
        for file in files:
            source_path = os.path.join(self.to_temp_location, file)
            destination_path = os.path.join(self.to_location, file)
            # Copy the file from source to destination
            try:
                shutil.copy2(source_path, destination_path)
                print(f"File {file} copied successfully.")
            except FileNotFoundError:
                print(f"File {file} not found.")
            except IOError as e:
                print(f"Error copying file {file}: {e}")

    def load_file_pkl(self):
        if (self.from_local_location and self.from_local_file_name) is not None:
            target_file_path = os.path.join(self.from_local_location, self.from_local_file_name)
            with open(target_file_path, "rb") as f:
                if not self.skip:
                    loaded_list = pickle.load(f)
                else:  # skip=True時のリスト範囲限定処理
                    try:
                        ids = [int(line.strip()) for line in f]
                        index = ids.index(self.obtained_last_key)
                        # 範囲外の場合や最後の要素の場合に注意
                        if index < len(ids) - 1:
                            loaded_list = ids[index + 1 :]
                        else:
                            print("指定したIDがリストの最後にあります。")
                    except ValueError:
                        print("指定したIDがリスト内に見つかりません。")
            self.skip = False
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
                            print("指定したIDがリストの最後にあります。")
                            loaded_list = []
                    except UnicodeDecodeError:
                        print("指定したIDがリスト内に見つかりません。")
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
                            print("指定したIDがリストの最後にあります。")
                            loaded_list = []
                    except ValueError:
                        print("指定したIDがリスト内に見つかりません。")
                        loaded_list = []
            self.skip = False
            return loaded_list

    def get_file_list(self, location):
        file_list = []
        # 指定したフォルダ内の全てのファイルおよびディレクトリのリストを取得
        items = os.listdir(location)

        for item in items:
            # ファイルの場合のみリストに追加
            if os.path.isfile(os.path.join(self.from_local_location, item)):
                file_list.append(item)
        return file_list

    def delete_files(self):
        files_to_temp = os.listdir(self.to_temp_location)
        files_to = os.listdir(self.to_location)
        # ファイルを削除
        for file in files_to_temp:
            file_path_to_temp = os.path.join(self.to_temp_location, file)
            try:
                if os.path.isfile(file_path_to_temp):
                    os.remove(file_path_to_temp)
            except Exception as e:
                print(f"Error deleting {file_path_to_temp}: {e}")
        for file in files_to:
            file_path_to = os.path.join(self.to_location, file)
            try:
                if os.path.isfile(file_path_to):
                    os.remove(file_path_to)
            except Exception as e:
                print(f"Error deleting {file_path_to}: {e}")

    def pre_process_display(self):  # 処理開始時のメッセージ出力
        print(f"{self.alias}")
        print("self.from_location: ", self.from_location)
        print("to_temp_location: ", self.to_temp_location)
        print("to_location: ", self.to_location)
        if self.from_local_location != "":
            print("self.from_local_location: ", self.from_local_location)
        if self.from_local_file_name != "":
            print("self.from_local_file_name: ", self.from_local_file_name)
        if len(self.target_data) != 0:
            print("len:", len(self.target_data))
        print("batch_size:", self.batch_size)
        if self.from_local_file_name != "":
            print(f"reloaded_{self.from_local_file_name} type: ", self.from_local_file_name)

    def post_process_display(self):  # 処理終了時のメッセージ出力
        print(f"{self.alias}[:5]:", self.target_data[-5:])
        print(f"{self.alias} type: ", type(self.target_data))
        print("len:", len(self.target_data))
        print("Done / obtained_last_key: ", self.obtained_last_key)
        print(f"新規作成: {self.temp_save_file_name} -> {self.save_file_name}")


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
            print('preparing update raw data empty')
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
