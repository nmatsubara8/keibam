"""§2c/2d/2e/2i/2j 特徴量生成を含むデータマージクラス。

§2i: 多窓（5/9/20R）× 多統計量（mean/std/max/min/median）集計
§2c: 騎手・調教師集計特徴量（jockey_win_rate, jockey_avg_rank, trainer_win_rate, trainer_avg_rank）
§2d: 脚質集計特徴量（pace_median, leg_type_binary, pace_at_distance）
§2e: コース条件別集計特徴量（win_rate_at_distance, avg_rank_at_course_type）
§2j: 種牡馬集計特徴量（sire_win_rate, sire_avg_rank, sire_recent_win_rate）
"""

import logging
import sys

import numpy as np
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
        speed_index_test_size: float | None = None,
        speed_index_base_path: str | None = None,
    ):
        from src.constants._local_paths import LocalPaths
        from src.constants._speed_index import SPEED_INDEX_TEST_SIZE

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
        # Phase 3: スピード指数。学習側は基準タイム表を build して保存、ライブ側(Shutuba)は load。
        self._speed_index_build: bool = True
        self._speed_index_test_size: float = (
            speed_index_test_size if speed_index_test_size is not None else SPEED_INDEX_TEST_SIZE
        )
        self._speed_index_base_path: str = speed_index_base_path or LocalPaths.BASE_TIME_TABLE_PATH
        # Phase 5: エンティティ(騎手/調教師/馬主/生産者)統計。学習側は最新スナップショットを
        # 保存、ライブ側(Shutuba)は load してマージ（過去履歴が空でも全 NaN にしない）。
        self._entity_stats_build: bool = True
        self._entity_stats_dir: str = LocalPaths.MASTER_DIR

    def merge(self):
        self._merge_race_info()
        logger.debug("merge_infos\n%s", self._results.sort_values(by="race_id").head().T)

        # Phase 5: 生産者集計用に breeder_id を results へ事前 join（1 回だけ）。
        # owner_id は results に既存。breeder_id は _merge_horse_info の重複除去で二重化を防ぐ。
        self._attach_breeder_id()

        self._merge_horse_results()
        logger.debug("merge_horse\n%s", self._merged_data.sort_values(by="horse_id").head().T)

        self._merge_horse_info()
        logger.debug("merge_horse_info\n%s", self._merged_data.sort_values(by="horse_id").head().T)

        self._merge_peds()

        # Phase 5: ライブ推論用に最新スナップショット統計を保存（学習側のみ）。
        self._save_entity_stats_snapshot()

        import os as _os
        _os.makedirs("./data/tmp/for_sandbox", exist_ok=True)
        self.merged_data.to_csv("./data/tmp/for_sandbox/test_df.csv", index=True)

    def _attach_breeder_id(self):
        """self._horse_info の breeder_id を horse_id で self._results に付与する（学習側）。"""
        if "breeder_id" in self._results.columns or "breeder_id" not in self._horse_info.columns:
            return
        hi = self._horse_info[["breeder_id"]].copy()
        hi.index = hi.index.astype(str)
        self._results = self._results.copy()
        self._results["horse_id"] = self._results["horse_id"].astype(str)
        self._results = self._results.merge(hi, left_on="horse_id", right_index=True, how="left")

    def _merge_race_info(self):
        # race_id インデックスの dtype 不一致（int64/float64 vs str）によるジョイン失敗を防ぐ
        self._results.index = self._results.index.astype(str).str.replace(r'\.0$', '', regex=True)
        self._race_info.index = self._race_info.index.astype(str).str.replace(r'\.0$', '', regex=True)
        self._results = self._results.merge(self._race_info, left_index=True, right_index=True, how="left")
        dict_ = dict_selector("_results")
        self._results = convert_column_types(self._results, dict_)

    def _separate_by_date(self):
        logger.info("separating horse results by date")
        dict_ = dict_selector("_horse_results")
        self._horse_results = convert_column_types(self._horse_results, dict_)

        # Pre-join horse_results with sire(peds_0) / damsire(peds_2) ids for §2j / Phase 7.
        ped_cols = [c for c in ("peds_0", "peds_2") if c in self._peds.columns]
        if ped_cols:
            hr_with_sire = self._horse_results.join(self._peds[ped_cols], how="left")
        else:
            hr_with_sire = self._horse_results.copy()

        for date, df_by_date in tqdm(self._results.groupby("date")):
            self._separated_results_dict[date] = df_by_date
            horse_id_list = df_by_date["horse_id"].unique()  # noqa: F841  pandas query の @horse_id_list で参照
            # Past horse results (only horses racing on this date)
            self._separated_horse_results_dict[date] = self._horse_results.query(
                "date < @date"
            ).query("horse_id in @horse_id_list")
            # Past horse results with sire info (all horses, for §2j aggregation by sire)
            self._separated_hr_with_sire_dict[date] = hr_with_sire.query("date < @date")

    def _speed_index_cutoff(self):
        """基準タイムの train/test 境界日を返す（DataSplitter の分割規則に整合）。

        results の各レース（race_id 単位）を date 昇順に並べ、(1 - test_size) 位置の
        レース日を境界とする。この日より前の horse_results で基準タイムを作ることで、
        テスト期間の time が基準統計に混入しないようにする。
        """
        if "date" not in self._results.columns or len(self._results) == 0:
            return None
        race_date = (
            pd.to_datetime(self._results["date"], errors="coerce")
            .groupby(self._results.index)
            .first()
            .sort_values()
        )
        race_date = race_date.dropna()
        if len(race_date) == 0:
            return None
        boundary = int(round(len(race_date) * (1 - self._speed_index_test_size)))
        boundary = min(max(boundary, 0), len(race_date) - 1)
        return race_date.iloc[boundary]

    def _ensure_speed_index(self):
        """Phase 3: self._horse_results に speed_index 列を付与する。

        学習側は train 期間限定で基準タイム表を build して artifact 保存、ライブ側
        (ShutubaDataMerger, _speed_index_build=False) は保存済み artifact をロードする。
        speed_index が AGG_TARGET_COLS に無ければ何もしない。
        """
        if "speed_index" not in self._target_cols:
            return
        from src.preprocessing._speed_index import (
            attach_speed_index,
            build_base_time_table,
            load_base_time_table,
            save_base_time_table,
        )

        path = getattr(self, "_speed_index_base_path", None)
        if getattr(self, "_speed_index_build", True):
            cutoff = self._speed_index_cutoff()
            base = build_base_time_table(self._horse_results, cutoff_date=cutoff)
            if path:
                save_base_time_table(base, path)
            logger.info("[speed_index] base table built (cutoff=%s)", cutoff)
        else:
            base = load_base_time_table(path) if path else {}
            logger.info("[speed_index] base table loaded: %s", path)
        self._horse_results = attach_speed_index(self._horse_results, base)

    def _merge_horse_results(self):
        self._ensure_speed_index()
        self._separate_by_date()
        logger.info("merging horse_results")
        output_results_dict: dict = {}

        for date in tqdm(self._separated_results_dict):
            results = self._separated_results_dict[date].copy()
            horse_results = self._separated_horse_results_dict[date].copy()

            # ── §2i: Multi-window × multi-stat aggregation ────────────
            # None は全レース集計（ウィンドウなし）
            for n_races in [*N_RACES_LIST, None]:
                results = self._merge_aggregates(results, horse_results, n_races)

            # ── Phase 1: 馬自身の通算成績 ──────────────────────────────
            results = self._add_horse_career_stats(results, horse_results)

            # ── Phase 8: 前走単独（直前走）の生値 ──────────────────────
            results = self._add_prev_race_stats(results, horse_results)

            # ── Phase 2: 乗り替わり / テン乗り ──────────────────────────
            results = self._add_jockey_change(results, horse_results)

            # Latest race date (for interval feature in FeatureEngineering)
            latest = horse_results.groupby("horse_id")["date"].max().rename("latest")
            results = results.merge(latest, left_on="horse_id", right_index=True, how="left")

            # ── §2c: Jockey / trainer stats ────────────────────────────
            results = self._add_jockey_trainer_stats(results, date)

            # ── Phase 5: 馬主・生産者 stats ────────────────────────────
            results = self._add_owner_breeder_stats(results, date)

            # ── §2d: Pace / leg type stats ─────────────────────────────
            results = self._add_pace_stats(results, horse_results)

            # ── Phase 4: レース展開予測（全馬の脚質確定後の横集計）─────────
            results = self._add_race_pace_forecast(results)

            # ── §2e: Course condition stats ────────────────────────────
            results = self._add_course_condition_stats(results, horse_results)

            # ── §2j: Sire stats ────────────────────────────────────────
            results = self._add_sire_stats(results, date)

            # ── Phase 4: 種牡馬 距離帯別適性 ───────────────────────────
            results = self._add_sire_distance_stats(results, date)

            # ── Phase 7: 母父(damsire) stats ──────────────────────────
            results = self._add_damsire_stats(results, date)

            output_results_dict[date] = results

        self._merged_data = pd.concat([output_results_dict[d] for d in output_results_dict])

    # ──────────────────────────────────────────
    # §2c: Jockey / trainer aggregate features
    # ──────────────────────────────────────────

    def _add_jockey_trainer_stats(self, results: pd.DataFrame, target_date) -> pd.DataFrame:
        """直近 JOCKEY_RECENT_N レースの騎手・調教師集計特徴量を追加する。

        ライブ推論（_entity_stats_build=False）では過去履歴が空のため、学習時に保存した
        最新スナップショットをロードして id でマージする（全 NaN 化を回避）。
        """
        if not getattr(self, "_entity_stats_build", True):
            return self._merge_loaded_entity_stats(
                results,
                [
                    ("jockey_id", "jockey_win_rate", "jockey_avg_rank"),
                    ("trainer_id", "trainer_win_rate", "trainer_avg_rank"),
                ],
            )
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
    # Phase 5: 馬主・生産者集計特徴量 + ライブ用エンティティ統計
    # ──────────────────────────────────────────

    def _add_owner_breeder_stats(self, results: pd.DataFrame, target_date) -> pd.DataFrame:
        """馬主・生産者の直近 OWNER_RECENT_N レース勝率/平均着順を追加する。

        学習側: self._results（breeder_id 事前 join 済み）の date < target_date から算出。
        ライブ側: 学習時スナップショットをロードして id でマージ（owner_id/breeder_id は
        horse_info から補完）。過去走のみ参照するためリークしない。
        """
        from src.constants._feature_cols import OWNER_RECENT_N
        from src.preprocessing._entity_stats import compute_entity_stats

        configs = [
            ("owner_id", "owner_win_rate", "owner_avg_rank"),
            ("breeder_id", "breeder_win_rate", "breeder_avg_rank"),
        ]
        if not getattr(self, "_entity_stats_build", True):
            return self._merge_loaded_entity_stats(results, configs)

        results = self._attach_owner_breeder_ids(results)
        past = self._results[self._results["date"] < target_date]
        for id_col, win_col, rank_col in configs:
            if id_col not in results.columns:
                results[win_col] = np.nan
                results[rank_col] = np.nan
                continue
            stats = compute_entity_stats(past, id_col, win_col, rank_col, OWNER_RECENT_N)
            if stats.empty:
                results[win_col] = np.nan
                results[rank_col] = np.nan
            else:
                results = results.merge(stats, left_on=id_col, right_index=True, how="left")
        return results

    def _attach_owner_breeder_ids(self, results: pd.DataFrame) -> pd.DataFrame:
        """results に owner_id / breeder_id を horse_info から補完する（欠けている場合のみ）。"""
        info = self._horse_info
        for col in ("owner_id", "breeder_id"):
            if col in results.columns or col not in info.columns:
                continue
            id_map = info[[col]].copy()
            id_map.index = id_map.index.astype(str)
            results = results.copy()
            results["horse_id"] = results["horse_id"].astype(str)
            results = results.merge(id_map, left_on="horse_id", right_index=True, how="left")
        return results

    def _merge_loaded_entity_stats(self, results: pd.DataFrame, configs: list) -> pd.DataFrame:
        """ライブ推論: 保存済みエンティティ統計をロードして id でマージする。

        artifact が無い/該当 id が無い場合も列を NaN で必ず生成し、学習側との
        列パリティを保つ（feature_names_ reindex より前に構造を揃える）。
        """
        from src.preprocessing._entity_stats import entity_stats_path, load_entity_stats

        results = self._attach_owner_breeder_ids(results)
        for id_col, win_col, rank_col in configs:
            stats = load_entity_stats(entity_stats_path(self._entity_stats_dir, id_col))
            if id_col in results.columns and not stats.empty and win_col in stats.columns:
                key = results[id_col].astype(str)
                results = results.assign(_key=key.values).merge(
                    stats[[win_col, rank_col]], left_on="_key", right_index=True, how="left"
                )
                results = results.drop(columns=["_key"], errors="ignore")
            else:
                results[win_col] = np.nan
                results[rank_col] = np.nan
        return results

    def _save_entity_stats_snapshot(self):
        """学習側: 全 self._results から最新スナップショット統計を保存する（ライブ推論用）。"""
        if not getattr(self, "_entity_stats_build", True):
            return
        from src.constants._feature_cols import JOCKEY_RECENT_N, OWNER_RECENT_N
        from src.preprocessing._entity_stats import (
            compute_entity_stats,
            entity_stats_path,
            save_entity_stats,
        )

        results = self._attach_owner_breeder_ids(self._results.copy())
        specs = [
            ("jockey_id", "jockey_win_rate", "jockey_avg_rank", JOCKEY_RECENT_N),
            ("trainer_id", "trainer_win_rate", "trainer_avg_rank", JOCKEY_RECENT_N),
            ("owner_id", "owner_win_rate", "owner_avg_rank", OWNER_RECENT_N),
            ("breeder_id", "breeder_win_rate", "breeder_avg_rank", OWNER_RECENT_N),
        ]
        for id_col, win_col, rank_col, recent_n in specs:
            if id_col not in results.columns:
                continue
            stats = compute_entity_stats(results, id_col, win_col, rank_col, recent_n)
            if stats.empty:
                continue
            stats.index = stats.index.astype(str)
            save_entity_stats(stats, entity_stats_path(self._entity_stats_dir, id_col))

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

        # Build per-horse current race info for distance/type/place filtering
        place_col = HRCols.PLACE  # '開催'
        info_cols = ["horse_id", "course_len"]
        if "race_type" in results.columns:
            info_cols.append("race_type")
        has_place = place_col in results.columns and place_col in hr.columns
        if has_place:
            info_cols.append(place_col)
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

        # Phase 7: win_rate_at_place / avg_rank_at_place（同一競馬場での成績）。
        # results 開催 は place_id(Int64)、horse_results 開催 は PLACE コード(str) のため
        # 双方 2 桁ゼロ埋め文字列に正規化して比較する。
        pc_past, pc_cur = f"{place_col}_past", f"{place_col}_cur"
        if has_place and pc_past in hr_with_cur.columns and pc_cur in hr_with_cur.columns:
            def _norm_place(s: pd.Series) -> pd.Series:
                return pd.to_numeric(s, errors="coerce").astype("Int64").astype(str).str.zfill(2)

            at_place = hr_with_cur[_norm_place(hr_with_cur[pc_past]) == _norm_place(hr_with_cur[pc_cur])]
            wr_place = at_place.groupby("horse_id")["_is_win"].mean().rename("win_rate_at_place")
            results = results.merge(wr_place, left_on="horse_id", right_index=True, how="left")
            if "_rel_rank" in hr_with_cur.columns:
                ar_place = at_place.groupby("horse_id")["_rel_rank"].mean().rename("avg_rank_at_place")
                results = results.merge(ar_place, left_on="horse_id", right_index=True, how="left")

        return results

    # ──────────────────────────────────────────
    # §2j / Phase 7: Sire / damsire aggregate features
    # ──────────────────────────────────────────

    def _add_pedline_stats(
        self, results: pd.DataFrame, target_date, ped_col: str,
        win_col: str, rank_col: str, recent_col: str,
    ) -> pd.DataFrame:
        """血統ライン（種牡馬 peds_0 / 母父 peds_2）の産駒集計特徴量を追加する汎用処理。

        _separated_hr_with_sire_dict は _separate_by_date() 内で peds_0/peds_2 を付与した
        horse_results のサブセット（date < target_date）を保持する。過去走のみ参照。
        """
        if target_date not in self._separated_hr_with_sire_dict:
            return results

        phs = self._separated_hr_with_sire_dict[target_date]
        rk = HRCols.RANK  # '着順'
        nh = HRCols.N_HORSES  # '頭数'
        if ped_col not in phs.columns or phs.empty or rk not in phs.columns:
            return results

        phs = phs.copy()
        # Categorical dtype 由来の groupby 問題を避けるため str 化
        phs["_ped_key"] = phs[ped_col].astype(str)
        phs["_is_win"] = (phs[rk] == 1).astype(float)
        if nh in phs.columns:
            phs["_rel_rank"] = phs[rk] / phs[nh]

        agg_dict: dict = {"_is_win": "mean"}
        if "_rel_rank" in phs.columns:
            agg_dict["_rel_rank"] = "mean"
        stats = phs.groupby("_ped_key").agg(agg_dict)
        stats.columns = [win_col if c == "_is_win" else rank_col for c in stats.columns]

        # 直近 N 年
        cutoff = pd.Timestamp(target_date) - pd.DateOffset(years=SIRE_RECENT_YEARS)
        recent = phs[phs["date"] >= cutoff]
        if not recent.empty:
            rec = recent.groupby("_ped_key")["_is_win"].mean().rename(recent_col)
            stats = stats.join(rec, how="left")
        else:
            stats[recent_col] = float("nan")

        if ped_col not in self._peds.columns:
            return results
        horse_ped = self._peds[[ped_col]].reset_index()
        horse_ped["_ped_key"] = horse_ped[ped_col].astype(str)
        hp = horse_ped[["horse_id", "_ped_key"]].set_index("horse_id")
        results = results.merge(hp, left_on="horse_id", right_index=True, how="left")
        results = results.merge(stats, left_on="_ped_key", right_index=True, how="left")
        results = results.drop(columns=["_ped_key"], errors="ignore")
        return results

    def _add_sire_stats(self, results: pd.DataFrame, target_date) -> pd.DataFrame:
        """種牡馬産駒の集計特徴量（sire_win_rate / sire_avg_rank / sire_recent_win_rate）。"""
        return self._add_pedline_stats(
            results, target_date, "peds_0",
            "sire_win_rate", "sire_avg_rank", "sire_recent_win_rate",
        )

    def _add_damsire_stats(self, results: pd.DataFrame, target_date) -> pd.DataFrame:
        """Phase 7: 母父(BMS, peds_2)産駒の集計特徴量を追加する。"""
        return self._add_pedline_stats(
            results, target_date, "peds_2",
            "damsire_win_rate", "damsire_avg_rank", "damsire_recent_win_rate",
        )

    # ──────────────────────────────────────────
    # Phase 4: レース展開予測 / 種牡馬 距離適性
    # ──────────────────────────────────────────

    def _add_race_pace_forecast(self, results: pd.DataFrame) -> pd.DataFrame:
        """レース単位の想定展開を横集計で付与する（_add_pace_stats の直後に呼ぶ）。

        results は index=race_id で同一レースの全馬が揃い、各馬の pace_median /
        leg_type_binary が確定済み。逃/先馬率・想定ペース・自馬の相対位置を生成する。
        レース内定数（own_vs_race_pace を除く）のため Z-score 対象外。
        当日出走馬の過去成績のみから算出されるためリークしない。
        """
        if "leg_type_binary" in results.columns:
            leg = results["leg_type_binary"]
            known = leg.notna()
            is_front = (leg == 0) & known
            front_count = is_front.astype(float).groupby(level=0).transform("sum")
            known_count = known.astype(float).groupby(level=0).transform("sum")
            results["race_front_count"] = front_count
            results["race_front_rate"] = front_count / known_count.replace(0, np.nan)
        if "pace_median" in results.columns:
            race_pace_mean = results["pace_median"].groupby(level=0).transform("mean")
            results["race_pace_mean"] = race_pace_mean
            results["own_vs_race_pace"] = results["pace_median"] - race_pace_mean
        return results

    @staticmethod
    def _dist_band(course_len) -> pd.Series:
        """course_len（100m 単位）を距離帯ラベル（str）に変換する。欠損は 'nan'。"""
        from src.constants._feature_cols import DIST_BAND_EDGES, DIST_BAND_LABELS

        cl = pd.to_numeric(course_len, errors="coerce")
        band = pd.cut(cl, bins=DIST_BAND_EDGES, labels=DIST_BAND_LABELS, right=True)
        return band.astype(str)

    def _add_sire_distance_stats(self, results: pd.DataFrame, target_date) -> pd.DataFrame:
        """種牡馬産駒の距離帯別成績（勝率/平均着順/件数）を現レースの距離帯で付与する。

        _separated_hr_with_sire_dict[target_date]（date < target_date, peds_0 付き）から
        (種牡馬, 距離帯) 別に集計。件数の少ないセルは NaN のまま（LightGBM に委ねる）。
        """
        if target_date not in self._separated_hr_with_sire_dict:
            return results
        phs = self._separated_hr_with_sire_dict[target_date]
        rank_col = HRCols.RANK
        n_horses_col = HRCols.N_HORSES
        if (
            "peds_0" not in phs.columns
            or phs.empty
            or rank_col not in phs.columns
            or "course_len" not in phs.columns
            or "course_len" not in results.columns
            or "peds_0" not in self._peds.columns
        ):
            return results

        phs = phs.copy()
        phs["_sire_key"] = phs["peds_0"].astype(str)
        phs["_dist_band"] = self._dist_band(phs["course_len"])
        phs["_is_win"] = (phs[rank_col] == 1).astype(float)

        band = phs.groupby(["_sire_key", "_dist_band"], observed=True)
        stats = pd.DataFrame(
            {
                "sire_win_rate_distband": band["_is_win"].mean(),
                "sire_n_distband": band["_is_win"].count().astype(float),
            }
        )
        if n_horses_col in phs.columns:
            phs["_rel_rank"] = phs[rank_col] / phs[n_horses_col]
            stats["sire_avg_rank_distband"] = phs.groupby(
                ["_sire_key", "_dist_band"], observed=True
            )["_rel_rank"].mean()

        # 現レースの種牡馬キー + 距離帯
        horse_sire = self._peds[["peds_0"]].reset_index()
        horse_sire["_sire_key"] = horse_sire["peds_0"].astype(str)
        hs = horse_sire[["horse_id", "_sire_key"]].set_index("horse_id")
        results = results.merge(hs, left_on="horse_id", right_index=True, how="left")
        results["_dist_band"] = self._dist_band(results["course_len"])
        results = results.merge(
            stats, left_on=["_sire_key", "_dist_band"], right_index=True, how="left"
        )
        results = results.drop(columns=["_sire_key", "_dist_band"], errors="ignore")
        return results

    # ──────────────────────────────────────────
    # Core helpers
    # ──────────────────────────────────────────

    def _merge_horse_info(self):
        self._merged_data["horse_id"] = self._merged_data["horse_id"].astype(str)
        info = self._horse_info.copy()
        info.index = info.index.astype(str)
        # 既に merged_data に存在する列（breeder_id を事前 join した場合など）は
        # 二重化（_x/_y）を避けるため info 側から除外する。
        dup = [c for c in info.columns if c in self._merged_data.columns]
        if dup:
            info = info.drop(columns=dup)
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

    def _add_horse_career_stats(
        self, results: pd.DataFrame, horse_results: pd.DataFrame
    ) -> pd.DataFrame:
        """Phase 1: 馬自身の通算成績（出走数・勝率・連対率・複勝率）を results に付与する。

        horse_results は呼出時点で「当日より過去（date < 当日）」かつ「当該レース
        出走馬」に絞り込み済み（_separate_by_date）。着順 NaN 行は
        HorseResultsProcessor で drop 済みのため、n_career_starts は完走ベースの
        出走回数となる。過去走のみ参照するためリークしない。

        ライブ推論（ShutubaDataMerger）でも本メソッドは _merge_horse_results 経由で
        呼ばれるため、学習と同一の列が生成される（特徴量パリティ）。
        """
        from src.constants._feature_cols import HORSE_CAREER_FEATURE_COLS

        if HRCols.RANK not in horse_results.columns:
            return results

        rank = pd.to_numeric(horse_results[HRCols.RANK], errors="coerce")
        career = pd.DataFrame(
            {
                "n_career_starts": rank.groupby(level=0).count(),
                "career_win_rate": (rank == 1).groupby(level=0).mean(),
                "career_quinella_rate": (rank <= 2).groupby(level=0).mean(),
                "career_place_rate": (rank <= 3).groupby(level=0).mean(),
            }
        )[HORSE_CAREER_FEATURE_COLS]
        return results.merge(career, left_on="horse_id", right_index=True, how="left")

    def _add_prev_race_stats(
        self, results: pd.DataFrame, horse_results: pd.DataFrame
    ) -> pd.DataFrame:
        """Phase 8: 直前走（最新の過去走）の生値を prev_* として付与する。

        窓集計（_mean_5R 等）と別に「前走そのもの」を明示特徴量化する。
        horse_results は date < 当日 に絞り込み済みのためリークしない。
        """
        src_to_dst = {
            HRCols.RANK: "prev_rank",
            HRCols.RANK_DIFF: "prev_rank_diff",
            "final_corner": "prev_final_corner",
            HRCols.NOBORI: "prev_nobori",
            "speed_index": "prev_speed_index",
        }
        avail = {s: d for s, d in src_to_dst.items() if s in horse_results.columns}
        if not avail or horse_results.empty:
            return results
        prev = (
            horse_results.sort_values("date", ascending=False)
            .groupby(level=0)
            .head(1)[list(avail.keys())]
            .rename(columns=avail)
        )
        return results.merge(prev, left_on="horse_id", right_index=True, how="left")

    @staticmethod
    def _normalize_jockey(s: pd.Series) -> pd.Series:
        """騎手名を正規化して比較を安定化する。

        前後空白と見習い斤量マーク（☆▲△★◇◎ 等）を除去する。nullable string を
        用いて欠損を <NA> のまま保ち、"nan" 文字列化による誤マッチを防ぐ。
        """
        out = (
            s.astype("string")
            .str.strip()
            .str.replace(r"^[☆▲△★◇◎☆*]+", "", regex=True)
            .str.strip()
        )
        return out.replace("", pd.NA)

    def _add_jockey_change(
        self, results: pd.DataFrame, horse_results: pd.DataFrame
    ) -> pd.DataFrame:
        """Phase 2: 乗り替わり(jockey_change)・テン乗り(first_ride)フラグを付与する。

        - jockey_change: 今走騎手 != 前走騎手（最新の過去走の騎手）。履歴なしは NaN。
        - first_ride: この馬への騎乗歴が過去に無い（テン乗り）=1。履歴なしは NaN。

        過去走のみ参照するためリークしない。判定に用いた jockey_name 列は最後に drop
        するため、featured_data には数値フラグのみが残る（生の騎手名は学習に渡らない）。
        ライブ推論（ShutubaDataMerger）でも同経路で呼ばれ列パリティが保たれる。
        """
        if "jockey_name" not in results.columns:
            return results

        has_history = HRCols.JOCKEY in horse_results.columns and len(horse_results) > 0
        if not has_history:
            results["jockey_change"] = np.nan
            results["first_ride"] = np.nan
            return results.drop(columns=["jockey_name"], errors="ignore")

        hr_sorted = horse_results.sort_values("date", ascending=False)
        prev_jockey = self._normalize_jockey(hr_sorted.groupby(level=0)[HRCols.JOCKEY].first())
        past_norm = self._normalize_jockey(horse_results[HRCols.JOCKEY])
        past_sets = past_norm.groupby(level=0).apply(lambda x: set(x.dropna()))

        cur = self._normalize_jockey(results["jockey_name"]).to_numpy()
        prev = results["horse_id"].map(prev_jockey).to_numpy()
        past = results["horse_id"].map(past_sets).to_numpy()

        jockey_change: list = []
        first_ride: list = []
        for c, p, s in zip(cur, prev, past, strict=True):
            if pd.isna(c) or pd.isna(p):
                jockey_change.append(np.nan)
            else:
                jockey_change.append(1.0 if c != p else 0.0)
            if isinstance(s, set):
                first_ride.append(np.nan if pd.isna(c) else (0.0 if c in s else 1.0))
            else:
                first_ride.append(np.nan)
        results["jockey_change"] = jockey_change
        results["first_ride"] = first_ride
        return results.drop(columns=["jockey_name"], errors="ignore")

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
            summarized_with = self._summarize_with(hr, self._target_cols, group_col).add_suffix(
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
        horse_results に存在しない target_col は安全にスキップする（speed_index 未付与など）。
        """
        cols = [c for c in target_cols if c in horse_results.columns]
        agg = horse_results.groupby(level=0)[cols].agg(AGG_STATS)
        agg.columns = [f"{col}_{stat}" for col, stat in agg.columns]
        return agg

    def _summarize_with(
        self, horse_results: pd.DataFrame, target_cols: list, group_col: str
    ) -> pd.DataFrame:
        """(horse_id, group_col) ごとに target_cols を AGG_STATS で集計する。"""
        cols = [c for c in target_cols if c in horse_results.columns]
        agg = horse_results.groupby(["horse_id", group_col])[cols].agg(AGG_STATS)
        agg.columns = [f"{col}_{stat}" for col, stat in agg.columns]
        return agg
