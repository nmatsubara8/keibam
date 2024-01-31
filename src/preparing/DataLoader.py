import csv
import os
import pickle

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
        target_data=None,
        rerun=False,
    ):
        self.alias = alias
        self.from_location = from_location
        self.to_temp_location = to_temp_location
        self.temp_save_file_name = temp_save_file_name
        self.to_location = to_location
        self.save_file_name = save_file_name
        self.batch_size = batch_size
        self.target_data = target_data
        self.rerun = rerun

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
        else:
            print("No such data")

    def get_local_temp_file_path(self, alias):
        self.set_args(alias)
        local_temp_path = os.path.join(self.to_temp_location + self.temp_save_file_name)
        return local_temp_path

    def get_local_comp_file_path(self, alias):
        self.set_args(alias)
        local_comp_path = os.path.join(self.to_location + self.save_file_name)
        return local_comp_path

    def save_temp_file(self, alias):
        # テキストファイルのパス
        local_path = self.get_local_temp_file_path(alias)
        filetype = self.temp_save_file_name[-3:]

        print("to_temp_location: ", self.to_temp_location)
        print("to_location: ", self.to_location)
        if filetype == "csv":
            # CSVファイルにデータを書き込む処理
            with open(local_path, "w", newline="") as csv_file:
                csv_writer = csv.writer(csv_file, quoting=csv.QUOTE_NONNUMERIC)
                # リストの各要素を1行に書き込む（各要素をシングルクォーテーションで囲んでから書き込む）
                csv_writer.writerow(self.target_data)

        elif filetype == "txt":
            # テキストファイルにデータを書き込む処理
            with open(local_path, "w") as file:
                for date in self.target_data:
                    file.write(date + "\n")

        elif filetype == "pkl":
            # pickleファイルにデータを書き込む処理
            with open(local_path, "wb") as pkl_file:
                pickle.dump(self.target_data, pkl_file)
        else:
            print("Unsupported filetype. Please choose 'csv', 'txt', or 'pkl'.")

        print(f'File saved at "{local_path}"')

    def transfer_temp_file(self):
        from_target_file = self.get_local_temp_file_path(self.alias)
        with open(from_target_file, "r") as file:
            data = file.read()

        # Pickleファイルに変換して保存
        to_target_file = self.get_local_comp_file_path(self.alias)
        with open(to_target_file, "wb") as file:
            pickle.dump(data, file)

        print(f"変換完了: {self.temp_save_file_name} -> {self.save_file_name}")
