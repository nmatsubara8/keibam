import logging

import pandas as pd

from src.constants._results_cols import ResultsCols

logger = logging.getLogger(__name__)

# 学習特徴量から除外する列。
# - "rank": 二値目的変数
# - "date": 時系列分割キー（特徴量ではない）
# - TANSHO_ODDS('単勝'): EV 計算で別途使うため特徴量からは除外
# - RANK('着順'): 当該レースの実着順。rank = (着順 < 4) の元データであり、
#   特徴量に残すと目的変数リーク。§2c/2j 集計のため ResultsProcessor が選択するが
#   学習入力からは必ず除外する。
_DROP_FOR_TRAIN = ["rank", "date", "horse_id", ResultsCols.TANSHO_ODDS, ResultsCols.RANK]

# テスト入力用: EV 計算のため TANSHO_ODDS('単勝') は残し、実着順 RANK は除外する。
_DROP_FOR_TEST = ["rank", "date", "horse_id", ResultsCols.RANK]


class DataSplitter:
    def __init__(self, featured_data, test_size, valid_size) -> None:
        # PreparedFeatures または plain DataFrame を受け付ける
        from src.preprocessing._prepared_features import PreparedFeatures
        if isinstance(featured_data, PreparedFeatures):
            self.__featured_data = featured_data.gbdt
            self.__nn_raw: pd.DataFrame | None = featured_data.nn
        else:
            self.__featured_data = featured_data
            self.__nn_raw = None

        # スタッキング用
        self.__base_train: pd.DataFrame | None = None
        self.__meta_train: pd.DataFrame | None = None

        # NN ストリーム（PreparedFeatures が渡された場合のみ有効）
        self.__nn_scaler = None
        self.__nn_train: pd.DataFrame | None = None
        self.__nn_test: pd.DataFrame | None = None
        self.__nn_optuna_train: pd.DataFrame | None = None
        self.__nn_valid: pd.DataFrame | None = None
        self.__nn_base_train: pd.DataFrame | None = None
        self.__nn_meta_train: pd.DataFrame | None = None

        self.train_valid_test_split(test_size, valid_size)

    def train_valid_test_split(self, test_size, valid_size):
        """
        訓練データとテストデータに分ける。さらに訓練データをoptuna用の訓練データと検証データに分ける。
        """
        import optuna.integration.lightgbm as lgb_o

        self.__train_data, self.__test_data = self.__split_by_date(self.__featured_data, test_size=test_size)
        self.__train_data_optuna, self.__valid_data_optuna = self.__split_by_date(
            self.__train_data, test_size=valid_size
        )
        logger.info(
            "split sizes: total=%d train_optuna=%d valid_optuna=%d test=%d",
            len(self.__featured_data),
            len(self.__train_data_optuna),
            len(self.__valid_data_optuna),
            len(self.__test_data),
        )

        self.__lgb_train_optuna = lgb_o.Dataset(
            self.__train_data_optuna.drop(_DROP_FOR_TRAIN, axis=1, errors="ignore").values,
            self.__train_data_optuna["rank"],
        )
        self.__lgb_valid_optuna = lgb_o.Dataset(
            self.__valid_data_optuna.drop(_DROP_FOR_TRAIN, axis=1, errors="ignore").values,
            self.__valid_data_optuna["rank"],
        )
        # 説明変数と目的変数に分ける。開催はエラーなるので一度drop。
        self.__X_train = self.__train_data.drop(_DROP_FOR_TRAIN, axis=1, errors="ignore")
        self.__y_train = self.__train_data["rank"]
        self.__X_test = self.__test_data.drop(_DROP_FOR_TEST, axis=1, errors="ignore")
        self.__y_test = self.__test_data["rank"]
        logger.info("X_test size: %d", len(self.__X_test))

        # NN ストリーム: PreparedFeatures が渡された場合のみ実行
        if self.__nn_raw is not None:
            self.__nn_scaler = self.__make_nn_scaler()
            train_ids = self.__train_data.index.unique()
            test_ids = self.__test_data.index.unique()
            optuna_ids = self.__train_data_optuna.index.unique()
            valid_ids = self.__valid_data_optuna.index.unique()
            # 訓練データのみで fit（リーク防止）
            self.__nn_train = self.__nn_scaler.fit_transform(self.__nn_raw.loc[train_ids])
            self.__nn_test = self.__nn_scaler.transform(self.__nn_raw.loc[test_ids])
            self.__nn_optuna_train = self.__nn_scaler.transform(self.__nn_raw.loc[optuna_ids])
            self.__nn_valid = self.__nn_scaler.transform(self.__nn_raw.loc[valid_ids])

    def __make_nn_scaler(self):
        """nn_raw の category/numeric 列を自動検出して NnFeatureScaler を生成する。"""
        from src.preprocessing._nn_feature_scaler import NnFeatureScaler
        sample = self.__nn_raw.iloc[:0]  # dtype 確認用に空 DataFrame を使用
        entity_cols = [c for c in sample.columns if str(sample[c].dtype) == "category"]
        numeric_cols = [c for c in sample.columns if c not in entity_cols]
        return NnFeatureScaler(entity_cols=entity_cols, numeric_cols=numeric_cols)

    def __split_by_date(self, df, test_size):
        """
        時系列に沿って訓練データとテストデータに分ける関数。test_sizeは0~1。
        """
        sorted_id_list = df.sort_values("date").index.unique()
        train_id_list = sorted_id_list[: round(len(sorted_id_list) * (1 - test_size))]
        test_id_list = sorted_id_list[round(len(sorted_id_list) * (1 - test_size)) :]
        train = df.loc[train_id_list]
        test = df.loc[test_id_list]

        return train, test

    @property
    def featured_data(self):
        return self.__featured_data

    @property
    def train_data(self):
        return self.__train_data

    @property
    def test_data(self):
        return self.__test_data

    @property
    def train_data_optuna(self):
        return self.__train_data_optuna

    @property
    def valid_data_optuna(self):
        return self.__valid_data_optuna

    @property
    def lgb_train_optuna(self):
        return self.__lgb_train_optuna

    @property
    def lgb_valid_optuna(self):
        return self.__lgb_valid_optuna

    @property
    def X_train(self):
        return self.__X_train

    @property
    def y_train(self):
        return self.__y_train

    @property
    def X_test(self):
        return pd.DataFrame(self.__X_test)

    @property
    def y_test(self):
        return pd.Series(self.__y_test)

    # ------------------------------------------------------------------
    # スタッキング用 3-way 分割
    # ------------------------------------------------------------------

    def make_stacking_splits(self, meta_ratio: float = 0.3) -> None:
        """train_data_optuna を base_train / meta_train に分割し lgb Optuna データを再生成する。

        calib_holdout は valid_data_optuna をそのまま使用。
        base_train 内の 80/20 split で Optuna ハイパラ探索用データを再構築する。
        """
        import optuna.integration.lightgbm as lgb_o

        self.__base_train, self.__meta_train = self.__split_by_date(
            self.__train_data_optuna, test_size=meta_ratio
        )
        base_opt_train, base_opt_valid = self.__split_by_date(self.__base_train, test_size=0.2)
        self.__lgb_train_optuna = lgb_o.Dataset(
            base_opt_train.drop(_DROP_FOR_TRAIN, axis=1, errors="ignore").values,
            base_opt_train["rank"],
        )
        self.__lgb_valid_optuna = lgb_o.Dataset(
            base_opt_valid.drop(_DROP_FOR_TRAIN, axis=1, errors="ignore").values,
            base_opt_valid["rank"],
        )
        logger.info(
            "stacking sizes: base_train=%d meta_train=%d calib_holdout=%d",
            len(self.__base_train),
            len(self.__meta_train),
            len(self.__valid_data_optuna),
        )

        # NN ストリーム（PreparedFeatures が渡された場合のみ）
        if self.__nn_raw is not None and self.__nn_scaler is not None:
            self.__nn_base_train = self.__nn_scaler.transform(
                self.__nn_raw.loc[self.__base_train.index.unique()]
            )
            self.__nn_meta_train = self.__nn_scaler.transform(
                self.__nn_raw.loc[self.__meta_train.index.unique()]
            )

    @property
    def base_train_data(self) -> pd.DataFrame:
        if self.__base_train is None:
            raise RuntimeError("make_stacking_splits() を先に呼んでください。")
        return self.__base_train

    @property
    def meta_train_data(self) -> pd.DataFrame:
        if self.__meta_train is None:
            raise RuntimeError("make_stacking_splits() を先に呼んでください。")
        return self.__meta_train

    @property
    def calib_holdout_data(self) -> pd.DataFrame:
        return self.__valid_data_optuna

    @property
    def X_base_train(self) -> pd.DataFrame:
        return self.base_train_data.drop(_DROP_FOR_TRAIN, axis=1, errors="ignore")

    @property
    def y_base_train(self) -> pd.Series:
        return self.base_train_data["rank"]

    @property
    def X_meta_train(self) -> pd.DataFrame:
        return self.meta_train_data.drop(_DROP_FOR_TRAIN, axis=1, errors="ignore")

    @property
    def y_meta_train(self) -> pd.Series:
        return self.meta_train_data["rank"]

    @property
    def X_calib(self) -> pd.DataFrame:
        return self.__valid_data_optuna.drop(_DROP_FOR_TRAIN, axis=1, errors="ignore")

    @property
    def y_calib(self) -> pd.Series:
        return self.__valid_data_optuna["rank"]

    # ------------------------------------------------------------------
    # NN ストリームプロパティ（PreparedFeatures 使用時のみ有効）
    # ------------------------------------------------------------------

    @property
    def nn_scaler(self):
        """学習済み NnFeatureScaler。KeibaAI に渡して dill 保存する。"""
        return self.__nn_scaler

    @property
    def X_nn_train(self) -> "pd.DataFrame | None":
        return self.__nn_train

    @property
    def X_nn_test(self) -> "pd.DataFrame | None":
        return self.__nn_test

    @property
    def X_nn_base_train(self) -> "pd.DataFrame | None":
        if self.__nn_train is not None and self.__nn_base_train is None:
            raise RuntimeError("make_stacking_splits() を先に呼んでください。")
        return self.__nn_base_train

    @property
    def X_nn_meta_train(self) -> "pd.DataFrame | None":
        if self.__nn_train is not None and self.__nn_meta_train is None:
            raise RuntimeError("make_stacking_splits() を先に呼んでください。")
        return self.__nn_meta_train

    @property
    def X_nn_calib(self) -> "pd.DataFrame | None":
        return self.__nn_valid
