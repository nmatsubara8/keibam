import sys

import pandas as pd
from tqdm.auto import tqdm

from src.preprocessing._data_cleaner import convert_column_types
from src.preprocessing._data_cleaner import dict_selector
from src.preprocessing._horse_info_processor import HorseInfoProcessor
from src.preprocessing._horse_results_processor import HorseResultsProcessor
from src.preprocessing._peds_processor import PedsProcessor
from src.preprocessing._race_info_processor import RaceInfoProcessor
from src.preprocessing._results_processor import ResultsProcessor

# 表示を省略しないように設定
sys.maxsize = sys.maxsize


class DataMerger:
    def __init__(
        self,
        results_processor: ResultsProcessor,
        race_info_processor: RaceInfoProcessor,
        horse_results_processor: HorseResultsProcessor,
        horse_info_processor: HorseInfoProcessor,
        peds_processor: PedsProcessor,
        target_cols: list,
        group_cols: list,
    ):
        """
        初期処理
        """
        # レース結果テーブル（前処理後）
        self._results = results_processor.preprocessed_data
        # レース情報テーブル（前処理後）
        self._race_info = race_info_processor.preprocessed_data
        # 馬の過去成績テーブル（前処理後）
        self._horse_results = horse_results_processor.preprocessed_data
        # 馬の基本情報テーブル（前処理後）
        # 馬主情報はレース情報テーブルのものを利用するため、列を削除
        self._horse_info = horse_info_processor.preprocessed_data.drop(["owner_id"], axis=1)
        # 血統テーブル（前処理後）
        self._peds = peds_processor.preprocessed_data
        # 集計対象列
        self._target_cols = target_cols
        # horse_idと一緒にターゲットエンコーディングしたいカテゴリ変数
        self._group_cols = group_cols
        # 全てのマージが完了したデータ
        self._merged_data = pd.DataFrame()
        # 日付(date列)ごとに分かれたレース結果
        self._separated_results_dict = {}
        # レース結果データのdateごとに分かれた馬の過去成績
        self._separated_horse_results_dict = {}

    def merge(self):
        """
        マージ処理
        """
        self._merge_race_info()

        # print("merge_infos", self._results.sort_values(by="race_id").head().T)

        self._merge_horse_results()
        # print("merge_horse", self._merged_data.sort_values(by="horse_id").head().T)
        # print("merge_horse_results() done")
        self._merge_horse_info()
        # print("merge_horse_info", self._merged_data.sort_values(by="horse_id").head().T)
        # print("merge_horse_info() done")

        self._merge_peds()
        # print("merge_peds() done")
        # print("mergerd_data type:")
        # print(self.merged_data.dtypes.T)
        self.merged_data.to_csv("./data/tmp/for_sandbox/test_df.csv", index=True)
        # print("sex:")
        # print(self.merged_data["sex"].value_counts())

    def _merge_race_info(self):
        """
        レース情報テーブルを、レース結果テーブルにマージ
        """
        # print("self._results", self._results.dtypes.T)
        # print("self._race_info", self._race_info.dtypes.T)
        self._results = self._results.merge(self._race_info, left_index=True, right_index=True, how="left")
        dict = dict_selector("_results")
        df = convert_column_types(self._results, dict)
        self._results = df
        # print(self._results.dtypes.T)

    def _separate_by_date(self):
        """
        - レース結果を日付(date列)ごとに分ける
        - (レース結果のdate, その日に走る馬の過去成績データ)のペアを作る
        """
        print("separating horse results by date")
        dict = dict_selector("_horse_results")
        df = convert_column_types(self._horse_results, dict)
        self._horse_results = df

        # dateでデータを分割
        for date, df_by_date in tqdm(self._results.groupby("date")):
            self._separated_results_dict[date] = df_by_date
            # その日に走る馬一覧
            horse_id_list = df_by_date["horse_id"].unique()
            # dateより過去に絞る
            self._separated_horse_results_dict[date] = self._horse_results.query("date < @date").query(
                "horse_id in @horse_id_list"
            )

    def _merge_horse_results(self, n_races_list=[5, 9]):
        """
        馬の過去成績テーブルのマージ
        """
        n_races_list = [5, 9]
        self._separate_by_date()
        print("merging horse_results")
        output_results_dict = {}
        for date in tqdm(self._separated_results_dict):
            results = self._separated_results_dict[date].copy()
            horse_results = self._separated_horse_results_dict[date].copy()
            # 直近nレースに絞った過去成績をマージ
            for n_races in n_races_list:
                # 直近nレースに絞った過去成績
                n_race_horse_results = self._filter_horse_results(self._separated_horse_results_dict[date], n_races)

                # horse_idのみのターゲットエンコーディング
                # 何レース分集計しているか分かるように、列名に接尾辞をつける
                summarized = self._summarize(n_race_horse_results, self._target_cols).add_suffix("_{}R".format(n_races))
                # resultsにマージ
                results = results.merge(summarized, left_on="horse_id", right_index=True, how="left")

                # horse_idとカテゴリ変数を合わせてターゲットエンコーディング
                for group_col in self._group_cols:
                    # 何レース分、どのカテゴリ変数とともに集計しているか分かるように、列名に接尾辞をつける
                    summarized_with = self._summarize_with(
                        n_race_horse_results, self._target_cols, group_col
                    ).add_suffix("_{}_{}R".format(group_col, n_races))
                    # resultsにマージ
                    results = results.merge(
                        summarized_with, left_on=["horse_id", group_col], right_index=True, how="left"
                    )
            # 直近nレースに絞らないで過去成績をマージ
            summarized = self._summarize(horse_results, self._target_cols).add_suffix("_allR")
            results = results.merge(summarized, left_on="horse_id", right_index=True, how="left")
            for group_col in self._group_cols:
                # どのカテゴリ変数とともに集計しているか分かるように、列名に接尾辞をつける
                summarized_with = self._summarize_with(horse_results, self._target_cols, group_col).add_suffix(
                    "_{}_allR".format(group_col)
                )
                # resultsにマージ
                results = results.merge(summarized_with, left_on=["horse_id", group_col], right_index=True, how="left")
            # 前走の日付をマージ
            latest = horse_results.groupby("horse_id")["date"].max().rename("latest")
            results = results.merge(latest, left_on="horse_id", right_index=True, how="left")
            # 辞書型に格納
            output_results_dict[date] = results
        # 日付で分かれていたものを結合
        merged_data = pd.concat([output_results_dict[date] for date in output_results_dict])
        self._merged_data = merged_data

    def _merge_horse_info(self):
        """
        馬の基本情報テーブルのマージ
        """

        self._merged_data = self._merged_data.merge(self._horse_info, left_on="horse_id", right_index=True, how="left")
        # print(f"self.s:{self._merged_data.tail()}")
        # print(self._merged_data["birthday"])

    def _merge_peds(self):
        """
        血統テーブルのマージ
        """

        self._merged_data = self._merged_data.merge(self._peds, left_on="horse_id", right_index=True, how="left")

    @property
    def merged_data(self):
        return self._merged_data

    def _filter_horse_results(self, horse_results, n_races):
        """
        直近nレースに絞る
        """
        return horse_results.sort_values("date", ascending=False).groupby(level=0).head(n_races)

    def _summarize(self, horse_results, target_cols):
        """
        horse_idごとに、target_colsを集計する
        """
        return horse_results.groupby(level=0)[target_cols].mean()

    def _summarize_with(self, horse_results, target_cols, group_col):
        """
        horse_id, group_colごとにtarget_colsを集計する
        """
        return horse_results.groupby(["horse_id", group_col])[target_cols].mean()
