import optuna.integration.lightgbm as lgb_o

from src.constants._results_cols import ResultsCols


def calculate_start_index(horse_number_table, required_samples: int, start_race_index: int, start_df_index: int):
    """
    Calculate the start index for splitting the data based on the required number of samples.
    """

    df_index = start_df_index
    total_race_index = start_race_index + required_samples
    for race_index in range(start_race_index, total_race_index):
        df_index += horse_number_table["count"].iloc[race_index]
    return df_index


class DataSplitter:
    def __init__(self, featured_data, test_size, valid_size) -> None:
        self.__featured_data = featured_data
        # self.__feature_columns = featured_data.drop(["rank", "date", ResultsCols.TANSHO_ODDS], axis=1).columns
        self.train_valid_test_split(test_size, valid_size)

    def train_valid_test_split(self, test_size, valid_size):
        """
        Split the data into train, validation, and test sets.
        Further split the training data into optuna's train and validation sets.
        """
        df_sorted = self.__featured_data.sort_values(by=["date", "race_id"])
        grouped = df_sorted.groupby(["date", "race_id"])
        horse_number_table = grouped.size().reset_index(name="count")
        print("horse:", horse_number_table)
        total_groups = len(horse_number_table)

        # 検証セットのサンプル数を計算
        valid_samples = int(total_groups * valid_size)
        # テストセットのサンプル数を計算
        test_samples = int(total_groups * test_size)
        train_samples = total_groups - valid_samples - test_samples
        # テストセットの最初のインデックスを計算
        valid_index_from = calculate_start_index(horse_number_table, train_samples, 0, 0)
        # 検証セットの最初のインデックスを計算
        test_index_from = calculate_start_index(horse_number_table, valid_samples, train_samples, valid_index_from)

        # データを分割
        self.__train_data_optuna = df_sorted.iloc[:valid_index_from]
        self.__valid_data_optuna = df_sorted.iloc[valid_index_from:test_index_from]
        self.__test_data = df_sorted.iloc[test_index_from:]
        print("#################################train_data_optuna", self.__train_data_optuna)
        print("#################################valid_data_optuna", self.__valid_data_optuna)
        print("#################################test_data", self.__test_data)

    @property
    def train_data_optuna(self):
        # 目的変数を取得
        y_train_optuna = self.__train_data_optuna["rank"]
        y_valid_optuna = self.__valid_data_optuna["rank"]

        # 不要な列を削除
        # ResultsColsが定義されていると仮定して削除
        train_data_optuna = self.__train_data_optuna.drop(["rank", "date", ResultsCols.TANSHO_ODDS], axis=1).values
        valid_data_optuna = self.__valid_data_optuna.drop(["rank", "date", ResultsCols.TANSHO_ODDS], axis=1).values

        return train_data_optuna, valid_data_optuna, y_train_optuna, y_valid_optuna

    @property
    def test_data(self):
        # 目的変数を取得
        y_test = self.__test_data["rank"]
        # 不要な列を削除
        # ResultsColsが定義されていると仮定して削除
        X_test = self.__test_data.drop(["rank", "date", ResultsCols.TANSHO_ODDS], axis=1)
        return X_test, y_test

    @property
    def lgb_train_optuna(self):
        train_data_optuna, _, y_train_optuna, _ = self.train_data_optuna

        return lgb_o.Dataset(train_data_optuna, y_train_optuna)

    @property
    def lgb_valid_optuna(self):
        _, valid_data_optuna, _, y_valid_optuna = self.train_data_optuna

        return lgb_o.Dataset(valid_data_optuna, y_valid_optuna)

    @property
    def featured_data(self):
        return self.__featured_data

    @property
    def X_train(self):
        train_data_optuna, _, _, _ = self.train_data_optuna
        return train_data_optuna.values

    @property
    def y_train(self):
        _, _, y_train_optuna, _ = self.train_data_optuna
        return y_train_optuna

    @property
    def X_test(self):
        X_test, _ = self.test_data
        return X_test.values

    @property
    def y_test(self):
        _, y_test = self.test_data
        return y_test
