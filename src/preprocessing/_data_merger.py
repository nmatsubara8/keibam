"""§2c/2d/2e/2i/2j 特徴量生成を含むデータマージクラス。

§2i: 多窓（5/9/20R）× 多統計量（mean/std/max/min/median）集計
§2c: 騎手・調教師集計特徴量（jockey_win_rate, jockey_avg_rank, trainer_win_rate, trainer_avg_rank）
§2d: 脚質集計特徴量（pace_median, leg_type_binary, pace_at_distance）
§2e: コース条件別集計特徴量（win_rate_at_distance, avg_rank_at_course_type）
§2j: 種牡馬集計特徴量（sire_win_rate, sire_avg_rank, sire_recent_win_rate）
"""

import logging
import sys

import pandas as pd
from tqdm.auto import tqdm

from src.constants._feature_cols import (
    AGG_STATS,
    JOCKEY_RECENT_N,
    N_RACES_LIST,
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

logger = logging.getLogger(__name__)


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
        # (horse_id, group_col) 集計は着順のみに限定して列爆発を防ぐ（馬×騎手の組合せは
        # 多窓×多統計で膨らみやすい）。馬単独の多窓集計は target_cols 全体を使う。
        self._group_target_cols = ["着順"] if "着順" in target_cols else target_cols
        self._group_cols = group_cols
        self._merged_data = pd.DataFrame()
        self._separated_results_dict: dict = {}
        self._separated_horse_results_dict: dict = {}
        self._separated_hr_with_sire_dict: dict = {}

    def merge(self):
        import os as _os
        import time as _time

        def _step(name: str, fn) -> None:
            t0 = _time.perf_counter()
            fn()
            logger.info("[merge] %s: %.1fs", name, _time.perf_counter() - t0)

        self._normalize_join_keys()
        _step("race_info", self._merge_race_info)
        _step("horse_results", self._merge_horse_results)
        _step("horse_info", self._merge_horse_info)
        _step("peds", self._merge_peds)

        # 旧実装にあった巨大データ(数十万行)の debug CSV ダンプは to_csv だけで数分かかる
        # ため既定で無効化（純粋なサンドボックス用）。KEIBA_DUMP_MERGE_CSV=1 でのみ書き出す。
        if _os.environ.get("KEIBA_DUMP_MERGE_CSV") == "1":
            _os.makedirs("./data/tmp/for_sandbox", exist_ok=True)
            t0 = _time.perf_counter()
            self.merged_data.to_csv("./data/tmp/for_sandbox/test_df.csv", index=True)
            logger.info("[merge] debug CSV dump: %.1fs", _time.perf_counter() - t0)

    @staticmethod
    def _to_id_str(values: pd.Series | pd.Index) -> pd.Series | pd.Index:
        """ID（horse_id/race_id）を統一フォーマットの文字列へ正規化する。

        DB 復元由来（object 文字列）と pickle 由来（Int64/float64）が混在しても
        merge キーの dtype が一致するよう、全て文字列化し float の末尾 ``.0`` を除去する。
        欠損は文字列化で "nan"/"<NA>" 等になり得るが、それらは元々ジョインしないため許容する。
        """
        as_str = values.astype(str)
        return as_str.str.replace(r"\.0$", "", regex=True)

    def _normalize_join_keys(self) -> None:
        """horse_id / race_id の merge キーをソース横断で文字列に正規化する。

        netkeiba(pickle) と DB 復元データが混在しても join できるよう、
        DataMerger が参照する全テーブルの horse_id 列・index を文字列へ揃える。
        """
        if "horse_id" in self._results.columns:
            self._results["horse_id"] = self._to_id_str(self._results["horse_id"])
        if self._horse_results.index.name == "horse_id":
            self._horse_results.index = self._to_id_str(self._horse_results.index)
        if self._horse_info.index.name == "horse_id":
            self._horse_info.index = self._to_id_str(self._horse_info.index)
        if self._peds.index.name == "horse_id":
            self._peds.index = self._to_id_str(self._peds.index)

    def _merge_race_info(self):
        # race_id インデックスの dtype 不一致（int64/float64 vs str）によるジョイン失敗を防ぐ
        self._results.index = self._results.index.astype(str).str.replace(r'\.0$', '', regex=True)
        self._race_info.index = self._race_info.index.astype(str).str.replace(r'\.0$', '', regex=True)
        self._results = self._results.merge(self._race_info, left_index=True, right_index=True, how="left")
        dict_ = dict_selector("_results")
        self._results = convert_column_types(self._results, dict_)

    def _merge_horse_results(self):
        """日付ごとに horse_results / results をスライスしてマージする。

        全ての「date < target_date」フィルタを searchsorted に統一し、
        ループごとの全件コピーを排除してメモリを節約する。
        """
        import numpy as np

        logger.info("separating horse results by date")
        dict_ = dict_selector("_horse_results")
        self._horse_results = convert_column_types(self._horse_results, dict_)

        if "peds_0" in self._peds.columns:
            hr_with_sire = self._horse_results.join(self._peds[["peds_0"]], how="left")
        else:
            hr_with_sire = self._horse_results.copy()

        # horse_results / hr_with_sire を日付ソート済みで保持
        hr_sorted = self._horse_results.sort_values("date").reset_index()
        hr_dates = hr_sorted["date"].values
        hrs_sorted = hr_with_sire.sort_values("date").reset_index()
        hrs_dates = hrs_sorted["date"].values

        # 騎手・調教師統計を self._results に直接付与（ループ内の全件コピーを排除）。
        # race_id をキーにした merge は (race_id, trainer_id) が非一意になり得るため使わず、
        # 位置ベースで列を付与してインデックス（race_id）と整合させる。
        self._attach_jockey_trainer_stats()

        logger.info("merging horse_results")
        output_list: list = []

        for date, df_by_date in tqdm(self._results.groupby("date")):
            horse_id_list = df_by_date["horse_id"].unique()

            cut = int(np.searchsorted(hr_dates, date, side="left"))
            past_hr = (
                hr_sorted.iloc[:cut].set_index("horse_id")
                if cut > 0
                else hr_sorted.iloc[:0].set_index("horse_id")
            )
            horse_results = past_hr[past_hr.index.isin(horse_id_list)]

            cut2 = int(np.searchsorted(hrs_dates, date, side="left"))
            self._separated_hr_with_sire_dict[date] = (
                hrs_sorted.iloc[:cut2].set_index("horse_id") if cut2 > 0
                else hrs_sorted.iloc[:0].set_index("horse_id")
            )

            # df_by_date は既に jockey/trainer 統計列を含む（race_id インデックス保持）
            results = df_by_date.copy()

            for n_races in [*N_RACES_LIST, None]:
                results = self._merge_aggregates(results, horse_results, n_races)

            latest = horse_results.groupby("horse_id")["date"].max().rename("latest")
            results = results.merge(latest, left_on="horse_id", right_index=True, how="left")

            results = self._add_pace_stats(results, horse_results)
            results = self._add_growth_stats(results, horse_results)
            results = self._add_prev_race_features(results, horse_results)
            results = self._add_aptitude_stats(results, horse_results)
            results = self._add_speed_figure_stats(results, horse_results)
            results = self._add_course_condition_stats(results, horse_results)
            results = self._add_sire_stats(results, date)

            output_list.append(results)
            del self._separated_hr_with_sire_dict[date]

        # ignore_index=False で race_id インデックスを温存する
        # （下流の FeatureEngineering は race_id をインデックスから参照する）
        self._merged_data = pd.concat(output_list)

    def _attach_jockey_trainer_stats(self) -> None:
        """騎手・調教師・馬主の直近 JOCKEY_RECENT_N レース統計列を self._results に付与する。

        groupby + rolling で全レースを1回で計算する（ループ内の全件コピーを排除）。
        race_id は self._results の（非一意な）インデックスなので、列ではなく
        位置ベースで結果を書き戻し、インデックスとの不整合を防ぐ。
        shift(1) で自レースを除外し、未来情報のリークを防ぐ。

        馬主は従来「生の owner_id（ラベル符号化）」しか特徴量が無く、GBDT が ID を
        丸暗記して過学習しやすかった（重要度診断で owner_id が突出）。騎手・調教師と
        同じく平滑な勝率/平均着順を与え、汎化可能な馬主シグナルにする。
        """
        rank_col = "着順"
        n_horses_col = "n_horses"
        has_rank = rank_col in self._results.columns and n_horses_col in self._results.columns
        has_jockey = "jockey_id" in self._results.columns
        if not has_rank or not has_jockey:
            return
        has_trainer = "trainer_id" in self._results.columns
        has_owner = "owner_id" in self._results.columns

        # 位置を保持したまま計算するため reset_index（race_id は捨てて位置で戻す）
        res = self._results.reset_index(drop=True).copy()
        res["_pos"] = range(len(res))
        rank_num = pd.to_numeric(res[rank_col], errors="coerce")
        n_horses_num = pd.to_numeric(res[n_horses_col], errors="coerce")
        res["_is_win"] = (rank_num == 1).astype("float32")
        res["_rel_rank"] = (rank_num / n_horses_num).astype("float32")

        def _recent(id_col: str, win_name: str, rank_name: str) -> None:
            nonlocal res
            res = res.sort_values([id_col, "date"], kind="stable")
            res[win_name] = res.groupby(id_col)["_is_win"].transform(
                lambda x: x.shift(1).rolling(JOCKEY_RECENT_N, min_periods=1).mean()
            )
            res[rank_name] = res.groupby(id_col)["_rel_rank"].transform(
                lambda x: x.shift(1).rolling(JOCKEY_RECENT_N, min_periods=1).mean()
            )

        _recent("jockey_id", "jockey_win_rate", "jockey_avg_rank")
        if has_trainer:
            _recent("trainer_id", "trainer_win_rate", "trainer_avg_rank")
        if has_owner:
            _recent("owner_id", "owner_win_rate", "owner_avg_rank")

        # 元の行順に戻して self._results へ位置ベースで列を付与
        res = res.sort_values("_pos")
        cols = ["jockey_win_rate", "jockey_avg_rank"]
        if has_trainer:
            cols += ["trainer_win_rate", "trainer_avg_rank"]
        if has_owner:
            cols += ["owner_win_rate", "owner_avg_rank"]
        for c in cols:
            self._results[c] = res[c].to_numpy()

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
            past["_is_win"] = past[rank_col].astype(float) if rank_col in past.columns else 0.0
            past["_rel_rank"] = 1.0 - past[rank_col].astype(float) if rank_col in past.columns else 0.5

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
        """直近 N レースの脚質(走法)集計特徴量（pace_median / leg_type_binary / pace_at_distance）を追加。

        脚質は「ペース」列（レースのペース＝タイム文字列）ではなく通過順（第1コーナー位置）
        から導く。``_pace_num = first_corner / 頭数 ∈ [0,1]``（0=逃げ/前、1=追込/後）。
        旧実装は 'ペース'(6千種のタイム文字列)を脚質カテゴリ表(逃先差追)で map しており
        全 NaN だった（重要度0%の原因）。first_corner は HorseResultsProcessor が通過順から
        パース済み。
        """
        n_horses_col = HRCols.N_HORSES  # '頭数'
        if "first_corner" not in horse_results.columns or n_horses_col not in horse_results.columns:
            return results

        hr = horse_results.copy()
        fc = pd.to_numeric(hr["first_corner"], errors="coerce")
        nh = pd.to_numeric(hr[n_horses_col], errors="coerce")
        hr["_pace_num"] = (fc / nh).clip(lower=0.0, upper=1.0)

        # Overall pace median over last N races
        n_hr = self._filter_horse_results(hr, PACE_RECENT_N)
        pace_median = n_hr.groupby(level=0)["_pace_num"].median().rename("pace_median")

        # leg_type_binary: 前半(<0.5)=0(逃げ・先行), 後半(>=0.5)=1(差し・追込)
        def _to_binary(v: float) -> float:
            if pd.isna(v):
                return float("nan")
            return 0.0 if v < 0.5 else 1.0

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
    # §2k: Growth / form-trajectory features
    # ──────────────────────────────────────────

    def _add_growth_stats(self, results: pd.DataFrame, horse_results: pd.DataFrame) -> pd.DataFrame:
        """成長/フォーム・トレンド特徴量を追加する（早熟/晩成の客観代理、リーク無し）。

        ``growth_trend = 直近3走の平均相対着順 − それ以前の平均相対着順``。相対着順は
        ``着順/頭数`` で 0=勝ち〜1=最下位。負＝直近の方が良い＝上昇基調（成長/復調）、
        正＝直近が悪い＝下降。年齢とともに良化する馬（晩成）を捉える。``n_starts``（出走数）
        も付与してキャリアの厚みを表す。horse_results は当該レース日より前のみ（リーク無し）。
        """
        rank_col = HRCols.RANK  # '着順'
        n_horses_col = HRCols.N_HORSES  # '頭数'
        if (
            horse_results.empty
            or rank_col not in horse_results.columns
            or n_horses_col not in horse_results.columns
        ):
            return results

        hr = horse_results.copy()
        rr = pd.to_numeric(hr[rank_col], errors="coerce") / pd.to_numeric(hr[n_horses_col], errors="coerce")
        hr["_rr"] = rr
        hr = hr.sort_values("date")
        # 馬ごとの新しい順インデックス（0=最新）。ベクトル化して per-group apply を避ける。
        hr["_ridx"] = hr.groupby(level=0).cumcount(ascending=False)
        recent = hr[hr["_ridx"] < 3].groupby(level=0)["_rr"].mean()
        older = hr[hr["_ridx"] >= 3].groupby(level=0)["_rr"].mean()
        growth = (recent - older).rename("growth_trend")
        n_starts = hr.groupby(level=0)["_rr"].count().rename("n_starts")

        results = results.merge(growth, left_on="horse_id", right_index=True, how="left")
        results = results.merge(n_starts, left_on="horse_id", right_index=True, how="left")
        return results

    # ──────────────────────────────────────────
    # §2m: Previous-race comparison features (Batch A)
    # ──────────────────────────────────────────

    def _add_prev_race_features(self, results: pd.DataFrame, horse_results: pd.DataFrame) -> pd.DataFrame:
        """前走との比較特徴（距離延長/短縮・斤量増減・乗り替わり）を追加する。

        前走＝最も新しい過去走（horse_results は当該レース日より前のみなのでリーク無し）。
        - ``dist_change``    : 今回 course_len − 前走 course_len（正=延長・負=短縮）
        - ``kinryo_delta``   : 今回 斤量 − 前走 斤量（ハンデ増減）
        - ``jockey_change``  : 騎手が前走から替わったか（1=乗り替わり、0=継続、初出走=NaN）
        """
        if horse_results.empty:
            return results

        hr = horse_results.sort_values("date")
        prev = hr.groupby(level=0).tail(1)  # 馬ごとの最新（=前走）1行
        rename: dict = {}
        if "course_len" in prev.columns:
            rename["course_len"] = "_prev_course_len"
        if HRCols.KINRYO in prev.columns:
            rename[HRCols.KINRYO] = "_prev_kinryo"
        if HRCols.JOCKEY in prev.columns:
            rename[HRCols.JOCKEY] = "_prev_jockey"
        if not rename:
            return results

        prev_sub = prev[list(rename)].rename(columns=rename)
        results = results.merge(prev_sub, left_on="horse_id", right_index=True, how="left")

        if "_prev_course_len" in results.columns and "course_len" in results.columns:
            results["dist_change"] = (
                pd.to_numeric(results["course_len"], errors="coerce")
                - pd.to_numeric(results["_prev_course_len"], errors="coerce")
            )
        if "_prev_kinryo" in results.columns and HRCols.KINRYO in results.columns:
            results["kinryo_delta"] = (
                pd.to_numeric(results[HRCols.KINRYO], errors="coerce")
                - pd.to_numeric(results["_prev_kinryo"], errors="coerce")
            )
        if "_prev_jockey" in results.columns and HRCols.JOCKEY in results.columns:
            cur_j = results[HRCols.JOCKEY].astype(str).str.strip()
            prev_j = results["_prev_jockey"].astype(str).str.strip()
            jc = (cur_j != prev_j).astype(float)
            jc[results["_prev_jockey"].isna()] = float("nan")  # 初出走は欠損
            results["jockey_change"] = jc

        results = results.drop(
            columns=[c for c in ("_prev_course_len", "_prev_kinryo", "_prev_jockey") if c in results.columns],
            errors="ignore",
        )
        return results

    # ──────────────────────────────────────────
    # §2n: Aptitude features — wet track & racecourse (Batch B)
    # ──────────────────────────────────────────

    def _add_aptitude_stats(self, results: pd.DataFrame, horse_results: pd.DataFrame) -> pd.DataFrame:
        """馬場・競馬場の適性特徴を追加する（リーク無し）。

        - ``wet_win_rate`` / ``wet_rel_rank`` : 道悪（馬場∈稍重/重/不良）での勝率・相対着順。
          今回の馬場に依らず「この馬の道悪実績」を表す（モデルが当日馬場ダミーと併用）。
        - ``place_win_rate`` : 今回と同じ競馬場（開催）での過去勝率（東京専用機/中山の鬼を捕捉）。
        horse_results は当該レース日より前のみ（リーク無し）。
        """
        rank_col = HRCols.RANK  # '着順'
        n_horses_col = HRCols.N_HORSES  # '頭数'
        ground_col = HRCols.GROUND_STATE  # '馬場'
        place_col = HRCols.PLACE  # '開催'
        if horse_results.empty or rank_col not in horse_results.columns:
            return results

        hr = horse_results.copy()
        hr["_is_win"] = (pd.to_numeric(hr[rank_col], errors="coerce") == 1).astype(float)
        if n_horses_col in hr.columns:
            hr["_rel_rank"] = pd.to_numeric(hr[rank_col], errors="coerce") / pd.to_numeric(
                hr[n_horses_col], errors="coerce"
            )

        # 道悪（非・良）実績: 当日馬場に依らない馬固有の適性
        if ground_col in hr.columns:
            wet = hr[hr[ground_col].astype(str).isin(["稍重", "重", "不良"])]
            if not wet.empty:
                wet_win = wet.groupby(level=0)["_is_win"].mean().rename("wet_win_rate")
                results = results.merge(wet_win, left_on="horse_id", right_index=True, how="left")
                if "_rel_rank" in wet.columns:
                    wet_rr = wet.groupby(level=0)["_rel_rank"].mean().rename("wet_rel_rank")
                    results = results.merge(wet_rr, left_on="horse_id", right_index=True, how="left")

        # 競馬場別実績: 今回と同じ開催での過去勝率。開催コードは horse_results が
        # ゼロ詰め文字列("05")・race_info が整数(5) と表現が異なるため数値化して比較する。
        if place_col in hr.columns and place_col in results.columns:
            hr_reset = hr.reset_index()
            hr_reset["_place"] = pd.to_numeric(hr_reset[place_col], errors="coerce")
            cur = results[["horse_id", place_col]].drop_duplicates("horse_id")
            cur["_cur_place"] = pd.to_numeric(cur[place_col], errors="coerce")
            merged = hr_reset.merge(cur[["horse_id", "_cur_place"]], on="horse_id")
            same = merged[merged["_place"] == merged["_cur_place"]]
            if not same.empty:
                place_win = same.groupby("horse_id")["_is_win"].mean().rename("place_win_rate")
                results = results.merge(place_win, left_on="horse_id", right_index=True, how="left")

        return results

    # ──────────────────────────────────────────
    # §2l: Speed-figure features (Batch C)
    # ──────────────────────────────────────────

    def _add_speed_figure_stats(self, results: pd.DataFrame, horse_results: pd.DataFrame) -> pd.DataFrame:
        """スピード指数（タイム偏差）の集計を追加する（リーク無し）。

        speed_figure は HorseResultsProcessor が各過去走に付与済み（基準タイムから何σ速いか、
        faster=正）。ここでは馬ごとに:
        - ``speed_fig_best``  : 過去最高指数（ピーク能力＝この馬の地力上限）
        - ``speed_fig_mean5`` : 直近5走平均（現在の調子・近走の地力）
        を算出する。horse_results は当該レース日より前のみ（リーク無し）。
        """
        if horse_results.empty or "speed_figure" not in horse_results.columns:
            return results

        hr = horse_results
        best = hr.groupby(level=0)["speed_figure"].max().rename("speed_fig_best")
        recent5 = self._filter_horse_results(hr, 5)
        mean5 = recent5.groupby(level=0)["speed_figure"].mean().rename("speed_fig_mean5")

        results = results.merge(best, left_on="horse_id", right_index=True, how="left")
        results = results.merge(mean5, left_on="horse_id", right_index=True, how="left")
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

        horse_sire_indexed = horse_sire[["horse_id", "_sire_key"]].set_index("horse_id")
        # horse_id の型を揃える: 出馬表 results は str、peds 由来 index は pkl 欠落時の
        # DB 復元で Int64 になりうる。両者 str に正規化してから結合する（学習では既に
        # str のため no-op）。
        horse_sire_indexed.index = horse_sire_indexed.index.astype(str)
        results = results.copy()
        results["horse_id"] = results["horse_id"].astype(str)
        results = results.merge(horse_sire_indexed, left_on="horse_id", right_index=True, how="left")
        results = results.merge(sire_all, left_on="_sire_key", right_index=True, how="left")
        results = results.drop(columns=["_sire_key"], errors="ignore")

        return results

    # ──────────────────────────────────────────
    # Core helpers
    # ──────────────────────────────────────────

    def _merge_horse_info(self):
        self._merged_data["horse_id"] = self._merged_data["horse_id"].astype(str)
        info = self._horse_info.copy()
        info.index = info.index.astype(str)
        self._merged_data = self._merged_data.merge(
            info, left_on="horse_id", right_index=True, how="left"
        )

    def _merge_peds(self):
        self._merged_data["horse_id"] = self._merged_data["horse_id"].astype(str)
        peds = self._peds.copy()
        peds.index = peds.index.astype(str)
        self._merged_data = self._merged_data.merge(
            peds, left_on="horse_id", right_index=True, how="left"
        )

    @property
    def merged_data(self):
        return self._merged_data

    def _merge_aggregates(
        self, results: pd.DataFrame, horse_results: pd.DataFrame, n_races: int | None
    ) -> pd.DataFrame:
        """horse_id / (horse_id, group_col) の集計結果を results にマージする。

        n_races=None のとき全レース集計（suffix: _allR）、整数のとき直近 n_races 集計（suffix: _NR）。
        """
        if n_races is None:
            hr = horse_results
            suffix = "_allR"
        else:
            hr = self._filter_horse_results(horse_results, n_races)
            suffix = f"_{n_races}R"

        summarized = self._summarize(hr, self._target_cols).add_suffix(suffix)
        results = results.merge(summarized, left_on="horse_id", right_index=True, how="left")

        for group_col in self._group_cols:
            if group_col not in results.columns:
                continue
            summarized_with = self._summarize_with(hr, self._group_target_cols, group_col).add_suffix(
                f"_{group_col}{suffix}"
            )
            results = results.merge(
                summarized_with, left_on=["horse_id", group_col], right_index=True, how="left"
            )
        return results

    def _filter_horse_results(self, horse_results: pd.DataFrame, n_races: int) -> pd.DataFrame:
        """直近 n_races レースに絞る（index=horse_id の前提）。"""
        return horse_results.sort_values("date", ascending=False).groupby(level=0).head(n_races)

    def _summarize(self, horse_results: pd.DataFrame, target_cols: list) -> pd.DataFrame:
        """§2i: horse_id ごとに target_cols を AGG_STATS で多統計量集計する。

        返り値の列名形式: {col}_{stat}（例: 着順_mean, 着順_std）
        呼び出し元で .add_suffix("_5R") 等を付与する。

        target_cols に string dtype の列（DB復元で混入しうる）が来ても落ちないよう、
        集計前に数値へ強制変換する（mean/std 等の reduction が str で失敗するのを防ぐ）。
        """
        num = horse_results[target_cols].apply(pd.to_numeric, errors="coerce")
        agg = num.groupby(level=0).agg(AGG_STATS)
        agg.columns = [f"{col}_{stat}" for col, stat in agg.columns]
        return agg

    def _summarize_with(
        self, horse_results: pd.DataFrame, target_cols: list, group_col: str
    ) -> pd.DataFrame:
        """(horse_id, group_col) ごとに target_cols を AGG_STATS で集計する。"""
        agg = horse_results.groupby(["horse_id", group_col])[target_cols].agg(AGG_STATS)
        agg.columns = [f"{col}_{stat}" for col, stat in agg.columns]
        return agg
