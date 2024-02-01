import json
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
        from_local_location="",
        from_local_file_name="",
        obtained_last_key="",
        target_data=None,
        skip=False,
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
        self.target_data = target_data
        self.obtained_last_key = obtained_last_key
        self.skip = skip

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

        else:
            print("No such data")

    def get_local_temp_file_path(self, alias):
        # self.set_args(alias)
        local_temp_path = os.path.join(self.to_temp_location + self.temp_save_file_name)
        return local_temp_path

    def get_local_comp_file_path(self, alias):
        # self.set_args(alias)
        local_comp_path = os.path.join(self.to_location + self.save_file_name)
        return local_comp_path

    def save_temp_file(self, alias):
        # テキストファイルのパス
        local_path = self.get_local_temp_file_path(alias)
        filetype = self.temp_save_file_name[-3:]

        if filetype == "csv":
            # CSVファイルにデータを書き込む処理
            with open(local_path, "w", newline="\n") as csv_file:
                # csv_writer = csv.writer(csv_file, quoting=csv.QUOTE_NONNUMERIC)
                # リストの各要素を1行に書き込む（各要素をシングルクォーテーションで囲んでから書き込む）
                json.dump(self.target_data, csv_file)

        elif filetype == "txt":
            # テキストファイルにデータを書き込む処理
            # リストをファイルに保存
            with open(local_path, "w") as file:
                for item in self.target_data:
                    file.write("%s\n" % item)

        elif filetype == "pkl":
            # pickleファイルにデータを書き込む処理
            with open(local_path, "wb") as pkl_file:
                pickle.dump(self.target_data, pkl_file)
        else:
            print("Unsupported filetype. Please choose 'csv', 'txt', or 'pkl'.")

        self.obtained_last_key = self.target_data[-1]

    def transfer_temp_file(self):
        from_target_file = self.get_local_temp_file_path(self.alias)
        with open(from_target_file, "r") as file:
            from_target_file = [line.strip() for line in file]

        to_target_file = self.get_local_comp_file_path(self.alias)

        with open(to_target_file, "wb") as pkl_file:
            pickle.dump(from_target_file, pkl_file)

    def load_file_pkl(self):
        target_file_path = os.path.join(self.from_local_location, self.from_local_file_name)
        with open(target_file_path, "rb") as f:
            if not self.skip:
                loaded_list = pickle.load(f)
            else:
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

    def pre_process_display(self):
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

    def post_process_display(self):
        print(f"{self.alias}[:5]:", self.target_data[-5:])
        print(f"{self.alias} type: ", type(self.target_data))
        print("len:", len(self.target_data))
        print("Done / obtained_last_key: ", self.obtained_last_key)
        print(f"新規作成: {self.temp_save_file_name} -> {self.save_file_name}")
