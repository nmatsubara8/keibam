import csv
import pickle


class DataLoader:
    def __init__(
        self,
        alias: str,
        from_location: str,
        to_temp_location: str,
        to_location: str,
        temp_save_file_name: str,
        save_file_name: str,
        rerun=False,
        batch_size=100,
        failed_data=None,
    ):
        self.alias = alias
        self.from_location = from_location
        self.to_temp_location = to_temp_location
        self.to_location = to_location
        self.temp_save_file_name = temp_save_file_name
        self.save_file_name = save_file_name
        self.rerun = rerun
        self.batch_size = batch_size

    def save_temp_file(self, target, filetype):
        # テキストファイルのパス

        # path_to_save = self.to_temp_location + self.temp_save_file_name + filetype
        print("self.to_temp_location: ", self.to_temp_location)
        print("self.temp_save_file_name : ", self.temp_save_file_name)
        print("self.save_file_name : ", self.save_file_name)
        print("filetype: ", filetype)

        if filetype == "csv":
            # CSVファイルにデータを書き込む処理
            with open(path_to_save, "w", newline="") as csv_file:
                csv_writer = csv.writer(csv_file, quoting=csv.QUOTE_NONNUMERIC)
                # リストの各要素を1行に書き込む（各要素をシングルクォーテーションで囲んでから書き込む）
                csv_writer.writerow(target)

        elif filetype == "txt":
            # テキストファイルにデータを書き込む処理
            with open(path_to_save, "w") as file:
                for date in target:
                    file.write(date + "\n")
        elif filetype == "pkl":
            # pickleファイルにデータを書き込む処理
            with open(path_to_save, "wb") as pkl_file:
                pickle.dump(target, pkl_file)
        else:
            print("Unsupported filetype. Please choose 'csv', 'txt', or 'pkl'.")

        print(f'File saved at "{path_to_save}"')

    def load_file(self, target, filetype):
        # ファイルを読み込む
        with open(target, "r") as file:
            # ファイルの中身をリストとして読み込む
            kaisai_date_list_loaded = [line.strip() for line in file.readlines()]

        # 読み込んだリストの中身を表示
        print("読み込まれたリスト:", kaisai_date_list_loaded)
