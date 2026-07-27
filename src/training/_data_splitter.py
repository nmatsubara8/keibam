import logging

import pandas as pd

from src.constants._horse_results_cols import HorseResultsCols
from src.constants._results_cols import TARGET_LEAK_COLS
from src.constants._results_cols import ResultsCols

logger = logging.getLogger(__name__)

# 学習特徴量から除外する列。
# - "rank": 二値目的変数
# - "date": 時系列分割キー（特徴量ではない）
# - TANSHO_ODDS('単勝'): EV 計算で別途使うため特徴量からは除外
# - RANK('着順'): 当該レースの実着順。rank = (着順 < 4) の元データであり、
#   特徴量に残すと目的変数リーク。§2c/2j 集計のため ResultsProcessor が選択するが
#   学習入力からは必ず除外する。
# - CORNER('通過'): 当該レースのコーナー通過順（例 "7-8-3-3"）。着順確定後にしか
#   分からない post-race 情報＝リーク列であり、かつ生文字列なので LightGBM の
#   数値変換で落ちる。ResultsProcessor は過去走の脚質(first_corner)復元のために
#   選択・保持するが、学習入力からは必ず除外する（§10 で results に追加された）。
# rank(top3) と rank_win(1着) は二値目的変数。どちらを学習しても**両方**を入力から除外し
# 相互リーク（top3 に win が含まれる等）を防ぐ。rank_win 等の目的変数リーク列は
# TARGET_LEAK_COLS（_results_cols）に一元化した単一定義元を参照する。
_DROP_FOR_TRAIN = [
    "rank", *TARGET_LEAK_COLS, "date", "horse_id",
    ResultsCols.TANSHO_ODDS, ResultsCols.RANK, HorseResultsCols.CORNER,
]

# テスト入力用: EV 計算のため TANSHO_ODDS('単勝') は残し、実着順 RANK は除外する。
_DROP_FOR_TEST = ["rank", *TARGET_LEAK_COLS, "date", "horse_id", ResultsCols.RANK, HorseResultsCols.CORNER]


class DataSplitter:
    def __init__(self, featured_data, test_size, valid_size, target_col: str = "rank") -> None:
        # target_col: 目的変数列。"rank"=複勝(top3, 既定) / "rank_win"=単勝(1着)。
        # Win ヘッドを学習するときは target_col="rank_win" を渡す。
        self.__target = target_col
        # PreparedFeatures または plain DataFrame を受け付ける
        from src.preprocessing._prepared_features import PreparedFeatures
        if isinstance(featured_data, PreparedFeatures):
            self.__featured_data = self.__downcast_floats(featured_data.gbdt)
            # nn_raw も float32 にダウンキャストしてメモリを約半減（NN は float32 で十分）
            self.__nn_raw: pd.DataFrame | None = self.__downcast_floats(featured_data.nn)
        else:
            self.__featured_data = self.__downcast_floats(featured_data)
            self.__nn_raw = None
        self.__featured_data = self.__coerce_object_features(self.__featured_data)

        # スタッキング用
        self.__base_train: pd.DataFrame | None = None
        self.__meta_train: pd.DataFrame | None = None

        # NN ストリーム（PreparedFeatures が渡された場合のみ有効）。
        # 各 split は遅延 transform で導出するため scaler のみ保持する。
        self.__nn_scaler = None

        self.train_valid_test_split(test_size, valid_size)

    def train_valid_test_split(self, test_size, valid_size):
        """
        訓練データとテストデータに分ける。さらに訓練データをoptuna用の訓練データと検証データに分ける。

        メモリ最適化のため、lgb Dataset / X_train / X_test 等の重い materialization は
        ここでは行わず、各プロパティへの初回アクセス時に遅延生成する。これにより
        スタッキング経路（with_tuning=False）では lgb_train_optuna 等を一切作らずに済む。
        """
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

        # 遅延生成キャッシュ（None = 未生成）
        self.__lgb_train_optuna = None
        self.__lgb_valid_optuna = None
        self.__X_train = None
        self.__y_train = None
        self.__X_test = None
        self.__y_test = None

        # NN ストリーム: PreparedFeatures が渡された場合のみ scaler を fit する。
        # 各 split の変換結果は保持せず、property 側で遅延 transform する（メモリ節約）。
        # stream-aware StackingModel は NN 入力を gbdt DataFrame から内部導出するため、
        # NN 専用 split を恒久的に持つ必要がない（数百万行分のコピーを回避）。
        if self.__nn_raw is not None:
            self.__nn_scaler = self.__make_nn_scaler()
            train_ids = self.__train_data.index.unique()
            # 訓練データのみで fit（リーク防止）。戻り値は破棄してメモリを解放。
            self.__nn_scaler.fit_transform(self.__nn_raw.loc[train_ids])

    # 数値化しない非特徴量列（時系列分割キー date / 文字列 ID horse_id）。学習入力からは
    # _DROP_FOR_TRAIN で除外されるが、__split_by_date が date を使うので coerce から保護する。
    __PROTECTED_NON_NUMERIC = ("date", "horse_id")

    @classmethod
    def __coerce_object_features(cls, df):
        """object dtype の特徴量列を数値へ強制変換する（予測側 _coerce_for_predict と対称）。

        featured_data は全特徴量が数値であることを前提に LightGBM へ ``.values`` で渡す。
        脚質集計の best_class_won 等、race_class_level が None を返し object dtype に
        なった列が混じると "pandas dtypes must be int, float or bool" で学習が落ちる。
        非特徴量（date/horse_id）を除く object 列を to_numeric（非数値→NaN）で数値化する。
        featured 再ビルド不要で既存 parquet をそのまま学習可能にするセーフティネット。
        """
        # object / 文字列 dtype のみ対象（category は LightGBM がネイティブに扱うため触らない）。
        # pandas 3 の select_dtypes(["object"]) は str も巻き込み警告を出すので dtype を直接判定。
        obj_cols = [
            c for c in df.columns
            if c not in cls.__PROTECTED_NON_NUMERIC
            and (df[c].dtype == object or pd.api.types.is_string_dtype(df[c]))
        ]
        if not obj_cols:
            return df
        df = df.copy()
        for c in obj_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("float32")
        logger.info("coerced %d object feature column(s) to numeric: %s", len(obj_cols), obj_cols)
        return df

    @staticmethod
    def __downcast_floats(df):
        """float64 列を float32 にダウンキャストしてメモリを約半減させる。

        LightGBM は内部でビニングするため float32 精度で十分。int / bool /
        category / str（horse_id 等）には影響を与えない。元 DataFrame は
        書き換えず、変換が必要な場合のみコピーを返す。
        """
        float_cols = df.select_dtypes(include=["float64"]).columns
        if len(float_cols) == 0:
            return df
        df = df.copy()
        df[float_cols] = df[float_cols].astype("float32")
        return df

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
        if self.__lgb_train_optuna is None:
            from src.training._lgb_optuna import lgb_o

            self.__lgb_train_optuna = lgb_o.Dataset(
                self.__train_data_optuna.drop(_DROP_FOR_TRAIN, axis=1, errors="ignore").values,
                self.__train_data_optuna[self.__target],
            )
        return self.__lgb_train_optuna

    @property
    def lgb_valid_optuna(self):
        if self.__lgb_valid_optuna is None:
            from src.training._lgb_optuna import lgb_o

            self.__lgb_valid_optuna = lgb_o.Dataset(
                self.__valid_data_optuna.drop(_DROP_FOR_TRAIN, axis=1, errors="ignore").values,
                self.__valid_data_optuna[self.__target],
            )
        return self.__lgb_valid_optuna

    @property
    def X_train(self):
        if self.__X_train is None:
            self.__X_train = self.__train_data.drop(_DROP_FOR_TRAIN, axis=1, errors="ignore")
        return self.__X_train

    @property
    def y_train(self):
        if self.__y_train is None:
            self.__y_train = self.__train_data[self.__target]
        return self.__y_train

    @property
    def X_test(self):
        if self.__X_test is None:
            self.__X_test = self.__test_data.drop(_DROP_FOR_TEST, axis=1, errors="ignore")
        return pd.DataFrame(self.__X_test)

    @property
    def y_test(self):
        if self.__y_test is None:
            self.__y_test = self.__test_data[self.__target]
        return pd.Series(self.__y_test)

    # ------------------------------------------------------------------
    # スタッキング用 3-way 分割
    # ------------------------------------------------------------------

    def make_stacking_splits(self, meta_ratio: float = 0.3, build_optuna_datasets: bool = True) -> None:
        """train_data_optuna を base_train / meta_train に分割し lgb Optuna データを再生成する。

        calib_holdout は valid_data_optuna をそのまま使用。
        base_train 内の 80/20 split で Optuna ハイパラ探索用データを再構築する。

        build_optuna_datasets=False の場合は lgb Optuna Dataset の生成を省略する
        （チューニングを行わないスタッキング学習ではメモリ節約のため不要）。
        """
        self.__base_train, self.__meta_train = self.__split_by_date(
            self.__train_data_optuna, test_size=meta_ratio
        )
        if build_optuna_datasets:
            from src.training._lgb_optuna import lgb_o

            base_opt_train, base_opt_valid = self.__split_by_date(self.__base_train, test_size=0.2)
            self.__lgb_train_optuna = lgb_o.Dataset(
                base_opt_train.drop(_DROP_FOR_TRAIN, axis=1, errors="ignore").values,
                base_opt_train[self.__target],
            )
            self.__lgb_valid_optuna = lgb_o.Dataset(
                base_opt_valid.drop(_DROP_FOR_TRAIN, axis=1, errors="ignore").values,
                base_opt_valid[self.__target],
            )
        logger.info(
            "stacking sizes: base_train=%d meta_train=%d calib_holdout=%d",
            len(self.__base_train),
            len(self.__meta_train),
            len(self.__valid_data_optuna),
        )

        # NN 専用 split は遅延 transform（property 側）で導出するため、ここでは保持しない。

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
        return self.base_train_data[self.__target]

    @property
    def X_meta_train(self) -> pd.DataFrame:
        return self.meta_train_data.drop(_DROP_FOR_TRAIN, axis=1, errors="ignore")

    @property
    def y_meta_train(self) -> pd.Series:
        return self.meta_train_data[self.__target]

    @property
    def X_calib(self) -> pd.DataFrame:
        return self.__valid_data_optuna.drop(_DROP_FOR_TRAIN, axis=1, errors="ignore")

    @property
    def y_calib(self) -> pd.Series:
        return self.__valid_data_optuna[self.__target]

    # ------------------------------------------------------------------
    # NN ストリームプロパティ（PreparedFeatures 使用時のみ有効）
    # ------------------------------------------------------------------

    @property
    def nn_scaler(self):
        """学習済み NnFeatureScaler。KeibaAI に渡して dill 保存する。"""
        return self.__nn_scaler

    @property
    def nn_categorical_cardinalities(self):
        """NN Entity Embedding 用 {nn入力列index: カーディナリティ}。未知バケット用に +1。"""
        if self.__nn_raw is None or self.__nn_scaler is None:
            return None
        cards = {}
        for i, col in enumerate(self.__nn_scaler.entity_cols):
            if col in self.__nn_raw.columns and str(self.__nn_raw[col].dtype) == "category":
                cards[i] = int(len(self.__nn_raw[col].cat.categories)) + 1
        return cards

    @property
    def has_nn_stream(self) -> bool:
        """NN ストリーム（2系統特徴量）が利用可能か。NN 入力導出の軽量ゲート。"""
        return self.__nn_raw is not None and self.__nn_scaler is not None

    def __nn_slice(self, df) -> "pd.DataFrame | None":
        """指定 split の NN ストリームを遅延 transform して返す（恒久保持しない）。"""
        if not self.has_nn_stream:
            return None
        # has_nn_stream で両者 not None を保証済み（mypy はプロパティ越しに narrow できないため明示）
        assert self.__nn_scaler is not None and self.__nn_raw is not None
        return self.__nn_scaler.transform(self.__nn_raw.loc[df.index.unique()])

    @property
    def X_nn_train(self) -> "pd.DataFrame | None":
        return self.__nn_slice(self.__train_data)

    @property
    def X_nn_test(self) -> "pd.DataFrame | None":
        return self.__nn_slice(self.__test_data)

    @property
    def X_nn_base_train(self) -> "pd.DataFrame | None":
        if self.has_nn_stream and self.__base_train is None:
            raise RuntimeError("make_stacking_splits() を先に呼んでください。")
        return self.__nn_slice(self.__base_train) if self.__base_train is not None else None

    @property
    def X_nn_meta_train(self) -> "pd.DataFrame | None":
        if self.has_nn_stream and self.__meta_train is None:
            raise RuntimeError("make_stacking_splits() を先に呼んでください。")
        return self.__nn_slice(self.__meta_train) if self.__meta_train is not None else None

    @property
    def X_nn_calib(self) -> "pd.DataFrame | None":
        return self.__nn_slice(self.__valid_data_optuna)
