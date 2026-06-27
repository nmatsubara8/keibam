import logging
import os
from typing import TYPE_CHECKING

import pandas as pd

from src.constants._horse_results_cols import HorseResultsCols
from src.constants._local_paths import LocalPaths
from src.constants._master import Master
from src.constants._nn_cols import NN_DROP_COLS, NN_ENTITY_COLS
from src.preprocessing._data_merger import DataMerger

if TYPE_CHECKING:
    from src.preprocessing._prepared_features import PreparedFeatures

logger = logging.getLogger(__name__)


class FeatureEngineering:
    """
    使うテーブルを全てマージした後の処理をするクラス。
    新しい特徴量を作りたいときは、メソッド単位で追加していく。
    各メソッドは依存関係を持たないよう注意。
    """

    def __init__(self, data_merger: DataMerger):
        self.__data = data_merger.merged_data.copy()
        logger.debug("FeatureEngineering: input type=%s", type(self.__data))

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
        return self

    def add_agedays(self):
        """
        レース出走日から日齢を算出
        """
        # 日齢を算出
        self.__data["birthday"] = pd.to_datetime(self.__data["birthday"])
        self.__data["age_days"] = (self.__data["date"] - self.__data["birthday"]).dt.days
        self.__data.drop("birthday", axis=1, inplace=True)
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
        self.__data.drop("place", axis=1, inplace=True, errors="ignore")
        self.__data.drop("time", axis=1, inplace=True, errors="ignore")
        return self

    def _dummify(self, col, categories, prefix=None, extra_drops=()):
        """カテゴリ列を One-Hot 化する共通処理。

        col を categories で Categorical 化 → get_dummies → 連結 → col と
        extra_drops を drop する。dumminize_* 各メソッドはこのヘルパーに委譲する。

        Parameters
        ----------
        col : ダミー化する列名。
        categories : pd.Categorical に渡すカテゴリ（順序固定で列を安定化）。
        prefix : get_dummies の prefix（None なら f"{col}_"）。
        extra_drops : col 以外に追加で drop する列名。
        """
        prefix = prefix if prefix is not None else f"{col}_"
        if col not in self.__data.columns:
            return self
        self.__data[col] = pd.Categorical(self.__data[col], list(categories))
        temp_data = pd.get_dummies(self.__data[col], prefix=prefix)
        self.__data = pd.concat([self.__data, temp_data], axis=1)
        drops = [col, *[c for c in extra_drops if c in self.__data.columns]]
        self.__data.drop(drops, axis=1, inplace=True)
        return self

    def dumminize_race_type(self):
        """race_typeカラムをダミー変数化する"""
        return self._dummify("race_type", Master.RACE_TYPE_DICT.values(), prefix="race_type_")

    def dumminize_weather(self):
        """weatherカラムをダミー変数化する"""
        return self._dummify("weather", Master.WEATHER_LIST, prefix="weather_")

    def dumminize_ground_state(self):
        """ground_stateカラムをダミー変数化する（shutubaパイプライン用）"""
        return self._dummify("ground_state", Master.GROUND_STATE_LIST, prefix="ground_state_")

    def dumminize_ground_state1(self):
        """ground_state1カラムをダミー変数化する"""
        return self._dummify("ground_state1", Master.GROUND_STATE_LIST, prefix="ground_state1_")

    def dumminize_ground_state2(self):
        """ground_state2カラムをダミー変数化する（race_condition も drop）"""
        return self._dummify(
            "ground_state2", Master.GROUND_STATE_LIST, prefix="ground_state2_", extra_drops=["race_condition"]
        )

    def dumminize_sex(self):
        """sex(性)カラムをダミー変数化する"""
        return self._dummify("性", Master.SEX_LIST, prefix="性_")

    def dumminize_around(self):
        """aroundカラムをダミー変数化する"""
        return self._dummify("around", Master.AROUND_LIST, prefix="around_")

    def dumminize_race_class(self):
        """race_classカラムをダミー変数化する"""
        return self._dummify("race_class", Master.RACE_CLASS_LIST, prefix="race_class_")

    def __label_encode(self, target_col):
        """
        引数で指定されたID（horse_id/jockey_id/trainer_id/owner_id/breeder_id）を
        ラベルエンコーディングして、Categorical型に変換する。
        """
        csv_path = os.path.join(LocalPaths.MASTER_DIR, target_col + ".csv")
        os.makedirs(LocalPaths.MASTER_DIR, exist_ok=True)
        # ファイルが存在しない場合、空のDataFrameを作成
        if not os.path.isfile(csv_path):
            target_master = pd.DataFrame(columns=[target_col, "encoded_id"])
        elif target_col == "horse_id":
            target_master = pd.read_csv(csv_path)
        else:
            target_master = pd.read_csv(csv_path, dtype=object)

        # 後のmaxでエラーになるので、整数に変換（NaN行は除去してから変換）
        target_master = target_master.dropna(subset=["encoded_id"])
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

        # インデックスをリセットし、元のマスタと繋げる
        new_target.reset_index(drop=True, inplace=True)
        new_target_master = pd.concat([target_master, new_target]).set_index(target_col)["encoded_id"]
        new_target_master = new_target_master[~new_target_master.index.duplicated(keep="first")]
        # マスタファイルを更新
        new_target_master.to_csv(csv_path)
        # ラベルエンコーディング実行
        self.__data[target_col] = pd.Categorical(self.__data[target_col].map(new_target_master))
        return self

    def encode(self, id_type):
        """指定 ID 列（horse_id/jockey_id/trainer_id/owner_id/breeder_id）を
        ラベルエンコードして Categorical 型に変換する共通メソッド。"""
        return self.__label_encode(id_type)

    def encode_horse_id(self):
        """horse_idをラベルエンコーディングして、Categorical型に変換する。"""
        return self.encode("horse_id")

    def encode_jockey_id(self):
        """jockey_idをラベルエンコーディングして、Categorical型に変換する。"""
        return self.encode("jockey_id")

    def encode_trainer_id(self):
        """trainer_idをラベルエンコーディングして、Categorical型に変換する。"""
        return self.encode("trainer_id")

    def encode_owner_id(self):
        """owner_idをラベルエンコーディングして、Categorical型に変換する。"""
        return self.encode("owner_id")

    def encode_breeder_id(self):
        """breeder_idをラベルエンコーディングして、Categorical型に変換する。"""
        return self.encode("breeder_id")

    def add_interaction_features(self):
        """§2b: 交互作用特徴量（frame_x_course / sex_x_month_sin/cos / distance_x_around）を追加。

        dummify 前に呼ぶこと（race_type / around / 性 を使用）。
        """
        from src.preprocessing._interaction_features import add_interaction_features as _add

        self.__data = _add(self.__data)
        return self

    def add_race_level_zscore(self):
        """§2g: レース内 Z-score 標準化。

        RACE_LEVEL_ZSCORE_COLS_G1（現レース特徴量）に加え、
        多窓集計列（_{stat}_{n}R, _{stat}_allR 形式）と §2c/2d/2e/2j の集計列を動的に追加。
        各列に `_z` サフィックスを付けた新列を追加し、元列は保持する。
        """
        from src.constants._feature_cols import (
            AGG_STATS,
            COND_TS_FEATURE_COLS,
            COURSE_CONDITION_FEATURE_COLS,
            ELO_FEATURE_COLS,
            JOCKEY_TRAINER_FEATURE_COLS,
            N_RACES_LIST,
            PACE_FEATURE_COLS,
            RACE_LEVEL_ZSCORE_COLS,
            SIRE_FEATURE_COLS,
            TS_FEATURE_COLS,
        )

        # Start with G1 static list
        zscore_cols = [c for c in RACE_LEVEL_ZSCORE_COLS if c in self.__data.columns]

        # Dynamically detect multi-window agg columns (e.g. 着順_mean_5R, 着順_std_allR)
        for n in N_RACES_LIST:
            for stat in AGG_STATS:
                for col in self.__data.columns:
                    if col.endswith(f"_{stat}_{n}R") or col.endswith(f"_{stat}_allR"):
                        if col not in zscore_cols:
                            zscore_cols.append(col)

        # §2c/2d/2e/2j named feature columns（+ §2k Elo / §2l TrueSkill: field_mean は
        # レース内一定のため z-score 対象から除外。rating/μ/σ/n_races/vs_field の
        # レース内相対値を素性化）
        elo_zscore_cols = [c for c in ELO_FEATURE_COLS if c != "elo_field_mean"]
        ts_zscore_cols = [c for c in TS_FEATURE_COLS if c != "ts_field_mean"]
        named_feature_cols = (
            JOCKEY_TRAINER_FEATURE_COLS
            + PACE_FEATURE_COLS
            + COURSE_CONDITION_FEATURE_COLS
            + SIRE_FEATURE_COLS
            + elo_zscore_cols
            + ts_zscore_cols
            + COND_TS_FEATURE_COLS  # 条件別 TrueSkill（vs_field 含むがレース内 z も有用）
        )
        for col in named_feature_cols:
            if col in self.__data.columns and col not in zscore_cols:
                zscore_cols.append(col)

        # Apply within-race (race_id index level 0) Z-score normalisation
        for col in zscore_cols:
            self.__data[f"{col}_z"] = self.__data.groupby(level=0)[col].transform(
                lambda x: (x - x.mean()) / (x.std() + 1e-8)
            )

        return self

    def build(self) -> "PreparedFeatures":
        """全特徴量エンジニアリング後、2系統 DataFrame を生成して PreparedFeatures DTO で返す。

        gbdt: self.featured_data のコピー（スケーリングなし・One-Hot 含む）
        nn:   entity_cols + numeric_cols のみ列選択（スケーリング未適用）
              DataSplitter が訓練データのみで NnFeatureScaler を fit する。

        numeric_cols の決定ロジック:
          - float64 / int64 / int32 / float32 の dtype を持つ列
          - entity_cols (category dtype) は dtype で自動除外
          - One-Hot ダミー列 (bool / uint8) は dtype で自動除外
          - NN_DROP_COLS (rank / date / 単勝) を明示除外
        """
        from src.preprocessing._prepared_features import PreparedFeatures

        gbdt_df = self.__data.copy()

        entity_cols = [c for c in NN_ENTITY_COLS if c in self.__data.columns]
        exclude = set(NN_DROP_COLS) | set(entity_cols)
        numeric_cols = [
            c for c in self.__data.select_dtypes(
                include=["float64", "int64", "int32", "float32"]
            ).columns
            if c not in exclude
        ]

        nn_df = self.__data[entity_cols + numeric_cols].copy()
        return PreparedFeatures(gbdt=gbdt_df, nn=nn_df)
