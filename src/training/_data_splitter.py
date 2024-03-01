import optuna.integration.lightgbm as lgb_o

from src.constants._results_cols import ResultsCols


class DataSplitter:
    def __init__(self, featured_data, test_size, valid_size) -> None:
        self.__featured_data = featured_data
        self.train_valid_test_split(test_size, valid_size)

    def train_valid_test_split(self, test_size, valid_size):
        """
        訓練データとテストデータに分ける。さらに訓練データをoptuna用の訓練データと検証データに分ける。
        """
        self.__train_data, self.__test_data = self.__split_by_date(self.__featured_data, test_size)
        self.__train_data_optuna, self.__valid_data_optuna = self.__split_by_date(self.__train_data, valid_size)

    def __split_by_date(self, df, test_size):
        """
        時系列に沿って訓練データとテストデータに分ける関数。test_sizeは0~1。
        """
        sorted_id_list = df.sort_values("date").index.unique()
        train_size = round(len(sorted_id_list) * (1 - test_size))
        train_index = sorted_id_list[:train_size]
        test_index = sorted_id_list[train_size:]
        train = df.iloc[df.index.isin(train_index)]
        test = df.iloc[df.index.isin(test_index)]
        return train, test

    @property
    def train_data_optuna(self):
        # 目的変数を取得
        y_train_optuna = self.__train_data_optuna["rank"]
        y_valid_optuna = self.__valid_data_optuna["rank"]

        # 不要な列を削除
        # ResultsColsが定義されていると仮定して削除
        train_data_optuna = self.__train_data_optuna.drop(["rank", "date", ResultsCols.TANSHO_ODDS], axis=1).values
        valid_data_optuna = self.__valid_data_optuna.drop(["rank", "date", ResultsCols.TANSHO_ODDS], axis=1).values

        print("self.__train_data_optuna", train_data_optuna.shape[1])
        print("self.__valid_data_optuna", valid_data_optuna.shape[1])

        return train_data_optuna, valid_data_optuna, y_train_optuna, y_valid_optuna

    @property
    def test_data(self):
        # 特徴量と目的変数を取得
        X_test = self.__test_data.drop(["rank", "date", ResultsCols.TANSHO_ODDS], axis=1)
        y_test = self.__test_data["rank"]

        print("self.__X_test_data", X_test.shape[1])

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
    def X_test(self):
        return self.test_data[0]

    @property
    def y_test(self):
        return self.test_data[1]

    @property
    def X_train(self):
        train_data_optuna, _, _, _ = self.train_data_optuna
        return train_data_optuna

    @property
    def y_train(self):
        _, _, y_train_optuna, _ = self.train_data_optuna
        return y_train_optuna
