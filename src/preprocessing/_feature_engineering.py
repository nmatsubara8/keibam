import os

import pandas as pd

from src.constants._horse_results_cols import HorseResultsCols
from src.constants._local_paths import LocalPaths
from src.constants._master import Master
from src.preprocessing._data_merger import DataMerger


class FeatureEngineering:
    """
    使うテーブルを全てマージした後の処理をするクラス。
    新しい特徴量を作りたいときは、メソッド単位で追加していく。
    各メソッドは依存関係を持たないよう注意。
    """

    def __init__(self, data_merger: DataMerger):
        self.__data = data_merger.merged_data.copy()
        # dict = dict_selector("horse_id")
        # convert_column_types(self.__data, dict)
        # print(type(self.__data))

    @property
    def featured_data(self):
        return self.__data

    def add_interval(self):
        """
        前走からの経過日数
        """
        self.__data["date"] = pd.to_datetime(self.__data["date"])
        self.__data["latest"] = pd.to_datetime(self.__data["latest"])
        self.__data["interval"] = (self.__data["date"] - self.__data["latest"]).dt.days
        self.__data.drop("latest", axis=1, inplace=True)
        # print(f"self.__data.columns_1:{self.__data.columns}")
        return self

    def add_agedays(self):
        """
        レース出走日から日齢を算出
        """
        # 日齢を算出
        self.__data["birthday"] = pd.to_datetime(self.__data["birthday"])
        self.__data["age_days"] = (self.__data["date"] - self.__data["birthday"]).dt.days
        self.__data.drop("birthday", axis=1, inplace=True)
        # print(f"self.__data.columns_2:{self.__data.columns}")
        return self

    def dumminize_kaisai(self):
        """
        開催カラムをダミー変数化する
        """
        self.__data[HorseResultsCols.PLACE] = pd.Categorical(
            self.__data[HorseResultsCols.PLACE], list(Master.PLACE_DICT.values())
        )
        temp_data = pd.get_dummies(self.__data[HorseResultsCols.PLACE], prefix=f"{HorseResultsCols.PLACE}_")
        self.__data = pd.concat([self.__data, temp_data], axis=1)
        # self.__data.drop(f"{HorseResultsCols.PLACE}", axis=1, inplace=True)
        self.__data.drop("place", axis=1, inplace=True)
        return self

    def dumminize_race_type(self):
        """
        race_typeカラムをダミー変数化する
        """
        self.__data["race_type"] = pd.Categorical(self.__data["race_type"], list(Master.RACE_TYPE_DICT.values()))
        temp_data = pd.get_dummies(self.__data["race_type"], prefix="race_type_")
        self.__data = pd.concat([self.__data, temp_data], axis=1)
        self.__data.drop("race_type", axis=1, inplace=True)
        return self

    def dumminize_weather(self):
        """
        weatherカラムをダミー変数化する
        """
        self.__data["weather"] = pd.Categorical(self.__data["weather"], Master.WEATHER_LIST)
        temp_data = pd.get_dummies(self.__data["weather"], prefix="weather_")
        self.__data = pd.concat([self.__data, temp_data], axis=1)
        self.__data.drop("weather", axis=1, inplace=True)
        return self

    def dumminize_ground_state1(self):
        """
        ground_stateカラムをダミー変数化する
        """
        self.__data["ground_state1"] = pd.Categorical(self.__data["ground_state1"], Master.GROUND_STATE_LIST)
        temp_data = pd.get_dummies(self.__data["ground_state1"], prefix="ground_state1_")
        self.__data = pd.concat([self.__data, temp_data], axis=1)
        self.__data.drop("ground_state1", axis=1, inplace=True)

        return self

    def dumminize_ground_state2(self):
        """
        ground_stateカラムをダミー変数化する
        """

        self.__data["ground_state2"] = pd.Categorical(self.__data["ground_state2"], Master.GROUND_STATE_LIST)
        temp_data = pd.get_dummies(self.__data["ground_state2"], prefix="ground_state2_")
        self.__data = pd.concat([self.__data, temp_data], axis=1)
        self.__data.drop("ground_state2", axis=1, inplace=True)

        return self

    def dumminize_sex(self):
        """
        sexカラムをダミー変数化する
        """
        self.__data["性"] = pd.Categorical(self.__data["性"], Master.SEX_LIST)
        temp_data = pd.get_dummies(self.__data["性"], prefix="性_")
        self.__data = pd.concat([self.__data, temp_data], axis=1)
        self.__data.drop("性", axis=1, inplace=True)

        return self

    def dumminize_around(self):
        """
        aroundカラムをダミー変数化する
        """

        self.__data["around"] = pd.Categorical(self.__data["around"], Master.AROUND_LIST)
        temp_data = pd.get_dummies(self.__data["around"], prefix="around_")
        self.__data = pd.concat([self.__data, temp_data], axis=1)
        self.__data.drop("around", axis=1, inplace=True)

        return self

    def dumminize_race_class(self):
        """
        race_classカラムをダミー変数化する
        """
        self.__data["race_class"] = pd.Categorical(self.__data["race_class"], Master.RACE_CLASS_LIST)
        temp_data = pd.get_dummies(self.__data["race_class"], prefix="race_class_")
        self.__data = pd.concat([self.__data, temp_data], axis=1)
        self.__data.drop("race_class", axis=1, inplace=True)
        return self

    def __label_encode(self, target_col: str):
        """
        引数で指定されたID（horse_id/jockey_id/trainer_id/owner_id/breeder_id）を
        ラベルエンコーディングして、Categorical型に変換する。
        """
        csv_path = os.path.join(LocalPaths.MASTER_DIR, target_col + ".csv")
        # ファイルが存在しない場合、空のDataFrameを作成
        if not os.path.isfile(csv_path):
            target_master = pd.DataFrame(columns=[target_col, "encoded_id"])
        else:
            target_master = pd.read_csv(csv_path, dtype=object)

        if target_col == "horse_id":
            target_master[target_col] = target_master[target_col].astype(str)
        # 後のmaxでエラーになるので、整数に変換
        target_master["encoded_id"] = target_master["encoded_id"].astype(int)

        # masterに存在しない、新しい情報を抽出
        new_target = self.__data[[target_col]][
            ~self.__data[target_col].isin(target_master[target_col])
        ].drop_duplicates(subset=[target_col])
        # 新しい情報を登録
        if len(target_master) > 0:
            new_target["encoded_id"] = [i + max(target_master["encoded_id"]) for i in range(1, len(new_target) + 1)]
            # 整数に変換
            new_target["encoded_id"] = new_target["encoded_id"].astype(int)
        else:  # まだ1行も登録されていない場合の処理
            new_target["encoded_id"] = [i for i in range(len(new_target))]

        # インデックスをリセット
        new_target.reset_index(drop=True, inplace=True)
        new_target_master = pd.concat([target_master, new_target]).set_index(target_col)["encoded_id"]
        # new_target_master = pd.concat([target_master, new_target], axis=0, ignore_index=True)
        ########################################

        #########################################
        #### 元のマスタと繋げる
        new_target_master = pd.concat([target_master, new_target]).set_index(target_col)["encoded_id"]
        ##### マスタファイルを更新
        new_target_master.to_csv(csv_path)
        ##### ラベルエンコーディング実行
        self.__data[target_col] = pd.Categorical(self.__data[target_col].map(new_target_master))
        return self

    def encode_horse_id(self):
        """
        horse_idをラベルエンコーディングして、Categorical型に変換する。
        """
        self.__label_encode("horse_id")
        return self

    def encode_jockey_id(self):
        """
        jockey_idをラベルエンコーディングして、Categorical型に変換する。
        """
        self.__label_encode("jockey_id")
        return self

    def encode_trainer_id(self):
        """
        trainer_idをラベルエンコーディングして、Categorical型に変換する。
        """
        self.__label_encode("trainer_id")
        return self

    def encode_owner_id(self):
        """
        owner_idをラベルエンコーディングして、Categorical型に変換する。
        """
        self.__label_encode("owner_id")
        return self

    def encode_breeder_id(self):
        """
        breeder_idをラベルエンコーディングして、Categorical型に変換する。
        """
        self.__label_encode("breeder_id")
        return self
