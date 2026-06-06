"""§2c/2d/2e/2i/2j 特徴量生成を含むデータマージクラス。

§2i: 多窓（5/9/20R）× 多統計量（mean/std/max/min/median）集計
§2c: 騎手・調教師集計特徴量（jockey_win_rate, jockey_avg_rank, trainer_win_rate, trainer_avg_rank）
§2d: 脚質集計特徴量（pace_median, leg_type_binary, pace_at_distance）
§2e: コース条件別集計特徴量（win_rate_at_distance, avg_rank_at_course_type）
§2j: 種牡馬集計特徴量（sire_win_rate, sire_avg_rank, sire_recent_win_rate）
"""

import sys

import pandas as pd
from tqdm.auto import tqdm

from src.constants._feature_cols import (
    AGG_STATS,
    JOCKEY_RECENT_N,
    N_RACES_LIST,
    PACE_CATEGORY_MAP,
    PACE_RECENT_N,
    SIRE_RECENT_YEARS,
)
from src.constants._horse_results_cols import HorseResultsCols as HRCols
from src.preprocessing._data_cleaner import convert_column_types
from src.preprocessing._data_cleaner import dict_selector
from src.preprocessing._horse_info_processor import HorseInfoProcessor
from src.preprocessing._horse_results_processor import HorseResultsProcessor
from src.preprocessing._peds_processor import PedsProcessor
from src.preprocessing._race_info_processor import RaceInfoProcessor
from src.preprocessing._results_processor import ResultsProcessor

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
        self._results = results_processor.preprocessed_data
        self._race_info = race_info_processor.preprocessed_data
        self._horse_results = horse_results_processor.preprocessed_data
        self._horse_info = horse_info_processor.preprocessed_data.drop(["owner_id"], axis=1)
        self._peds = peds_processor.preprocessed_data
        self._target_cols = target_cols
        self._group_cols = group_cols
        self._merged_data = pd.DataFrame()
        self._separated_results_dict: dict = {}
        self._separated_horse_results_dict: dict = {}
        self._separated_hr_with_sire_dict: dict = {}

    def merge(self):
        self._merge_race_info()

        print("merge_infos", self._results.sort_values(by="race_id").head().T)

        self._merge_horse_results()
        print("merge_horse", self._merged_data.sort_values(by="horse_id").head().T)

        self._merge_horse_info()
        print("merge_horse_info", self._merged_data.sort_values(by="horse_id").head().T)

        self._merge_peds()
        self.merged_data.to_csv("./data/tmp/for_sandbox/test_df.csv", index=True)

    def _merge_race_info(self):
        self._results = self._results.merge(self._race_info, left_index=True, right_index=True, how="left")
        dict_ = dict_selector("_results")
        self._results = convert_column_types(self._results, dict_)

    def _separate_by_date(self):
        print("separating horse results by date")
        dict_ = dict_selector("_horse_results")
        self._horse_results = convert_column_types(self._horse_results, dict_)

        # Pre-join horse_results with sire id (peds_0) for §2j sire stats.
        if "peds_0" in self._peds.columns:
            hr_with_sire = self._horse_results.join(self._peds[["peds_0"]], how="left")
        else:
            hr_with_sire = self._horse_results.copy()

        for date, df_by_date in tqdm(self._results.groupby("date")):
            self._separated_results_dict[date] = df_by_date
            horse_id_list = df_by_date["horse_id"].unique()
            # Past horse results (only horses racing on this date)
            self._separated_horse_results_dict[date] = self._horse_results.query(
                "date < @date"
            ).query("horse_id in @horse_id_list")
            # Past horse results with sire info (all horses, for §2j aggregation by sire)
            self._separated_hr_with_sire_dict[date] = hr_with_sire.query("date < @date")

    def _merge_horse_results(self):
        self._separate_by_date()
        print("merging horse_results")
        output_results_dict: dict = {}

        for date in tqdm(self._separated_results_dict):
            results = self._separated_results_dict[date].copy()
            horse_results = self._separated_horse_results_dict[date].copy()

            # ── §2i: Multi-window × multi-stat aggregation ────────────
            for n_races in N_RACES_LIST:
                n_race_hr = self._filter_horse_results(horse_results, n_races)
                summarized = self._summarize(n_race_hr, self._target_cols).add_suffix(f"_{n_races}R")
                results = results.merge(summarized, left_on="horse_id", right_index=True, how="left")

                for group_col in self._group_cols:
                    if group_col not in results.columns:
                        continue  # guard: broken mechanism when col absent from results
                    summarized_with = self._summarize_with(
                        n_race_hr, self._target_cols, group_col
                    ).add_suffix(f"_{group_col}_{n_races}R")
                    results = results.merge(
                        summarized_with, left_on=["horse_id", group_col], right_index=True, how="left"
                    )

            # All-races summary (no window limit)
            summarized_all = self._summarize(horse_results, self._target_cols).add_suffix("_allR")
            results = results.merge(summarized_all, left_on="horse_id", right_index=True, how="left")
            for group_col in self._group_cols:
                if group_col not in results.columns:
                    continue
                summarized_with_all = self._summarize_with(
                    horse_results, self._target_cols, group_col
                ).add_suffix(f"_{group_col}_allR")
                results = results.merge(
                    summarized_with_all, left_on=["horse_id", group_col], right_index=True, how="left"
                )

            # Latest race date (for interval feature in FeatureEngineering)
            latest = horse_results.groupby("horse_id")["date"].max().rename("latest")
            results = results.merge(latest, left_on="horse_id", right_index=True, how="left")

            # ── §2c: Jockey / trainer stats ────────────────────────────
            results = self._add_jockey_trainer_stats(results, date)

            # ── §2d: Pace / leg type stats ─────────────────────────────
            results = self._add_pace_stats(results, horse_results)

            # ── §2e: Course condition stats ────────────────────────────
            results = self._add_course_condition_stats(results, horse_results)

            # ── §2j: Sire stats ────────────────────────────────────────
            results = self._add_sire_stats(results, date)

            output_results_dict[date] = results

        self._merged_data = pd.concat([output_results_dict[d] for d in output_results_dict])

    # ──────────────────────────────────────────
    # §2c: Jockey / trainer aggregate features
    # ──────────────────────────────────────────

    def _add_jockey_trainer_stats(self, results: pd.DataFrame, target_date) -> pd.DataFrame:
        """直近 JOCKEY_RECENT_N レースの騎手・調教師集計特徴量を追加する。"""
        if "jockey_id" not in self._results.columns:
            return results

        past = self._results[self._results["date"] < target_date].copy()

        # Use actual finishing position for win rate and relative rank.
        rank_col = "着順"  # HRCols-equivalent in results (added back to ResultsProcessor)
        n_horses_col = "n_horses"
        has_actual_rank = rank_col in past.columns and n_horses_col in past.columns

        if has_actual_rank:
            past["_is_win"] = (past[rank_col] == 1).astype(float)
            past["_rel_rank"] = past[rank_col] / past[n_horses_col]
        else:
            # Fallback: use binary rank (top-3 indicator)
            past["_is_win"] = past["rank"].astype(float)
            past["_rel_rank"] = 1.0 - past["rank"].astype(float)  # 0 = top-3, 1 = not

        # Jockey stats (last N races per jockey_id)
        jockey_recent = (
            past.sort_values("date", ascending=False)
            .groupby("jockey_id")
            .head(JOCKEY_RECENT_N)
        )
        jockey_stats = jockey_recent.groupby("jockey_id").agg(
            jockey_win_rate=("_is_win", "mean"),
            jockey_avg_rank=("_rel_rank", "mean"),
        )
        results = results.merge(jockey_stats, left_on="jockey_id", right_index=True, how="left")

        # Trainer stats
        if "trainer_id" not in past.columns:
            return results
        trainer_recent = (
            past.sort_values("date", ascending=False)
            .groupby("trainer_id")
            .head(JOCKEY_RECENT_N)
        )
        trainer_stats = trainer_recent.groupby("trainer_id").agg(
            trainer_win_rate=("_is_win", "mean"),
            trainer_avg_rank=("_rel_rank", "mean"),
        )
        results = results.merge(trainer_stats, left_on="trainer_id", right_index=True, how="left")

        return results

    # ──────────────────────────────────────────
    # §2d: Pace / leg-type features
    # ──────────────────────────────────────────

    def _add_pace_stats(self, results: pd.DataFrame, horse_results: pd.DataFrame) -> pd.DataFrame:
        """直近 N レースの脚質集計特徴量（pace_median / leg_type_binary / pace_at_distance）を追加。"""
        if HRCols.PACE not in horse_results.columns:
            return results

        hr = horse_results.copy()
        hr["_pace_num"] = hr[HRCols.PACE].map(PACE_CATEGORY_MAP)

        # Overall pace median over last N races
        n_hr = self._filter_horse_results(hr, PACE_RECENT_N)
        pace_median = n_hr.groupby(level=0)["_pace_num"].median().rename("pace_median")

        # leg_type_binary: 逃/先(< 2) → 0, 差/追(>= 2) → 1, 中間値 → NaN
        def _to_binary(v: float) -> float:
            if pd.isna(v) or v == 1.5:
                return float("nan")
            return 0.0 if v < 2.0 else 1.0

        leg_binary = pace_median.map(_to_binary).rename("leg_type_binary")

        results = results.merge(pace_median, left_on="horse_id", right_index=True, how="left")
        results = results.merge(leg_binary, left_on="horse_id", right_index=True, how="left")

        # pace_at_distance: 同距離帯(±100m = ±1 in 100m units)での脚質中央値
        if "course_len" not in results.columns or "course_len" not in hr.columns:
            return results

        current_info = results[["horse_id", "course_len"]].drop_duplicates("horse_id")
        hr_reset = hr.reset_index()
        hr_with_cur = hr_reset.merge(
            current_info, on="horse_id", suffixes=("_past", "_cur")
        )
        at_dist = hr_with_cur[
            abs(hr_with_cur["course_len_past"] - hr_with_cur["course_len_cur"]) <= 1
        ]
        pace_at_dist = at_dist.groupby("horse_id")["_pace_num"].median().rename("pace_at_distance")
        results = results.merge(pace_at_dist, left_on="horse_id", right_index=True, how="left")

        return results

    # ──────────────────────────────────────────
    # §2e: Course condition aggregate features
    # ──────────────────────────────────────────

    def _add_course_condition_stats(
        self, results: pd.DataFrame, horse_results: pd.DataFrame
    ) -> pd.DataFrame:
        """同距離帯勝率・同コース種別平均着順を追加する。"""
        rank_col = HRCols.RANK  # '着順'
        n_horses_col = HRCols.N_HORSES  # '頭数'
        if horse_results.empty or rank_col not in horse_results.columns:
            return results
        if "course_len" not in results.columns:
            return results

        hr = horse_results.copy()
        hr["_is_win"] = (hr[rank_col] == 1).astype(float)
        if n_horses_col in hr.columns:
            hr["_rel_rank"] = hr[rank_col] / hr[n_horses_col]

        # Build per-horse current race info for distance/type filtering
        info_cols = ["horse_id", "course_len"]
        if "race_type" in results.columns:
            info_cols.append("race_type")
        current_info = results[info_cols].drop_duplicates("horse_id")

        hr_reset = hr.reset_index()
        hr_with_cur = hr_reset.merge(
            current_info, on="horse_id", suffixes=("_past", "_cur")
        )

        # win_rate_at_distance: ±100m (±1 unit)
        at_dist = hr_with_cur[
            abs(hr_with_cur["course_len_past"] - hr_with_cur["course_len_cur"]) <= 1
        ]
        win_rate = at_dist.groupby("horse_id")["_is_win"].mean().rename("win_rate_at_distance")
        results = results.merge(win_rate, left_on="horse_id", right_index=True, how="left")

        # avg_rank_at_course_type: same race_type
        if (
            "_rel_rank" in hr_with_cur.columns
            and "race_type_past" in hr_with_cur.columns
            and "race_type_cur" in hr_with_cur.columns
        ):
            at_type = hr_with_cur[hr_with_cur["race_type_past"] == hr_with_cur["race_type_cur"]]
            avg_rank = at_type.groupby("horse_id")["_rel_rank"].mean().rename("avg_rank_at_course_type")
            results = results.merge(avg_rank, left_on="horse_id", right_index=True, how="left")

        return results

    # ──────────────────────────────────────────
    # §2j: Sire aggregate features
    # ──────────────────────────────────────────

    def _add_sire_stats(self, results: pd.DataFrame, target_date) -> pd.DataFrame:
        """種牡馬産駒の集計特徴量（sire_win_rate / sire_avg_rank / sire_recent_win_rate）を追加。

        _separated_hr_with_sire_dict は _separate_by_date() 内で peds_0 列を付与した
        horse_results のサブセット（date < target_date）を保持する。
        """
        if target_date not in self._separated_hr_with_sire_dict:
            return results

        phs = self._separated_hr_with_sire_dict[target_date]
        rank_col = HRCols.RANK  # '着順'
        n_horses_col = HRCols.N_HORSES  # '頭数'

        if "peds_0" not in phs.columns or phs.empty or rank_col not in phs.columns:
            return results

        phs = phs.copy()
        # Use string representation of peds_0 to avoid category dtype issues in groupby
        phs["_sire_key"] = phs["peds_0"].astype(str)
        phs["_is_win"] = (phs[rank_col] == 1).astype(float)
        if n_horses_col in phs.columns:
            phs["_rel_rank"] = phs[rank_col] / phs[n_horses_col]

        agg_dict: dict = {"_is_win": "mean"}
        if "_rel_rank" in phs.columns:
            agg_dict["_rel_rank"] = "mean"

        sire_all = phs.groupby("_sire_key").agg(agg_dict)
        sire_all.columns = [
            "sire_win_rate" if c == "_is_win" else "sire_avg_rank"
            for c in sire_all.columns
        ]

        # Recent N years sire stats
        cutoff = pd.Timestamp(target_date) - pd.DateOffset(years=SIRE_RECENT_YEARS)
        recent = phs[phs["date"] >= cutoff]
        if not recent.empty:
            sire_recent = recent.groupby("_sire_key")["_is_win"].mean().rename("sire_recent_win_rate")
            sire_all = sire_all.join(sire_recent, how="left")
        else:
            sire_all["sire_recent_win_rate"] = float("nan")

        # Current horses' sire key from peds
        if "peds_0" not in self._peds.columns:
            return results
        horse_sire = self._peds[["peds_0"]].reset_index()
        horse_sire["_sire_key"] = horse_sire["peds_0"].astype(str)

        results = results.merge(horse_sire[["horse_id", "_sire_key"]], on="horse_id", how="left")
        results = results.merge(sire_all, left_on="_sire_key", right_index=True, how="left")
        results = results.drop(columns=["_sire_key"], errors="ignore")

        return results

    # ──────────────────────────────────────────
    # Core helpers
    # ──────────────────────────────────────────

    def _merge_horse_info(self):
        self._merged_data = self._merged_data.merge(
            self._horse_info, left_on="horse_id", right_index=True, how="left"
        )

    def _merge_peds(self):
        self._merged_data = self._merged_data.merge(
            self._peds, left_on="horse_id", right_index=True, how="left"
        )

    @property
    def merged_data(self):
        return self._merged_data

    def _filter_horse_results(self, horse_results: pd.DataFrame, n_races: int) -> pd.DataFrame:
        """直近 n_races レースに絞る（index=horse_id の前提）。"""
        return horse_results.sort_values("date", ascending=False).groupby(level=0).head(n_races)

    def _summarize(self, horse_results: pd.DataFrame, target_cols: list) -> pd.DataFrame:
        """§2i: horse_id ごとに target_cols を AGG_STATS で多統計量集計する。

        返り値の列名形式: {col}_{stat}（例: 着順_mean, 着順_std）
        呼び出し元で .add_suffix("_5R") 等を付与する。
        """
        agg = horse_results.groupby(level=0)[target_cols].agg(AGG_STATS)
        agg.columns = [f"{col}_{stat}" for col, stat in agg.columns]
        return agg

    def _summarize_with(
        self, horse_results: pd.DataFrame, target_cols: list, group_col: str
    ) -> pd.DataFrame:
        """(horse_id, group_col) ごとに target_cols を AGG_STATS で集計する。"""
        agg = horse_results.groupby(["horse_id", group_col])[target_cols].agg(AGG_STATS)
        agg.columns = [f"{col}_{stat}" for col, stat in agg.columns]
        return agg
