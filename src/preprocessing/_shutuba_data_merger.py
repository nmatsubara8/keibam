from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from src.preprocessing._horse_info_processor import HorseInfoProcessor
from src.preprocessing._horse_results_processor import HorseResultsProcessor
from src.preprocessing._peds_processor import PedsProcessor
from src.preprocessing._shutuba_table_processor import ShutubaTableProcessor

from ._data_merger import DataMerger

if TYPE_CHECKING:
    from src.preprocessing._race_info_processor import RaceInfoProcessor

logger = logging.getLogger(__name__)


class ShutubaDataMerger(DataMerger):
    def __init__(
        self,
        shutuba_table_processor: ShutubaTableProcessor,
        horse_results_processor: HorseResultsProcessor,
        horse_info_processor: HorseInfoProcessor,
        peds_processor: PedsProcessor,
        target_cols: list,
        group_cols: list,
        race_info_processor: RaceInfoProcessor | None = None,
    ):
        """
        初期処理

        Parameters
        ----------
        race_info_processor : オプション。指定すると race_info 由来の列
            (days, times, ground_state1/2, teiryo 等) を結合し、学習時と
            同じ特徴量セットで推論できる。
        """
        # レース結果テーブル（前処理後）
        self._results = shutuba_table_processor.preprocessed_data
        # 馬の過去成績テーブル（前処理後）
        self._horse_results = horse_results_processor.preprocessed_data
        # 馬の基本情報テーブル（前処理後）
        self._horse_info = horse_info_processor.preprocessed_data
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
        # 過去成績に種牡馬(peds_0)を付与した dateごとの辞書（§2j 種牡馬集計用）
        self._separated_hr_with_sire_dict = {}
        # race_info（オプション）: 指定時は _merge_race_info() で結合する
        self._race_info: pd.DataFrame | None = (
            race_info_processor.preprocessed_data if race_info_processor is not None else None
        )

    def _merge_race_info_shutuba(self) -> None:
        """shutuba パイプライン専用の race_info 結合。

        ShutubaTableProcessor が既に course_len / race_type / around / weather /
        race_class / date を保持しているため、race_info からは「差分列」
        (days, times, ground_state1/2, teiryo, 開催 等)だけを追加する。
        これにより列名の _x/_y 衝突を回避する。
        """
        from src.preprocessing._data_cleaner import convert_column_types, dict_selector

        if self._race_info is None:
            return
        ri = self._race_info.copy()
        # race_id インデックスの dtype を統一
        ri.index = ri.index.astype(str).str.replace(r"\.0$", "", regex=True)
        self._results.index = self._results.index.astype(str).str.replace(r"\.0$", "", regex=True)

        # shutuba が既に持っている列は race_info 側から除外して重複を防ぐ
        existing = set(self._results.columns)
        new_cols = [c for c in ri.columns if c not in existing]
        if not new_cols:
            return

        self._results = self._results.join(ri[new_cols], how="left")

        # ground_state1/2 が追加されたら単一の ground_state は不要
        if "ground_state1" in self._results.columns and "ground_state" in self._results.columns:
            self._results = self._results.drop(columns=["ground_state"])

        # 型変換（存在する列のみ対象）
        self._results = convert_column_types(self._results, dict_selector("_results"))

    def merge(self):
        """
        マージ処理
        """
        if self._race_info is not None:
            self._merge_race_info_shutuba()
            logger.info(
                "[ShutubaDataMerger] race_info joined: %d cols added",
                len(self._race_info.columns),
            )
        self._merge_horse_results()
        self._merge_horse_info()
        self._merge_peds()
        self._merge_horse_ratings()
        self._merge_horse_trueskill()
        self._merge_horse_conditional_trueskill()
        self._merge_horse_ability_kalman()

    def _merge_horse_ratings(self):
        """ライブ経路: models/horse_ratings.json のスナップショットを馬ごとに付与する。

        未来レース（出馬表）は実着順が無く as-of ウォークができないため、学習時に
        保存した最新スナップショットの現行レーティングを参照する。フィールド強度
        （elo_field_mean / elo_vs_field）は当該レース出走馬内で計算する。
        スナップショットが無い馬は初期レーティングへフォールバックする。
        """
        import json
        import os

        from src.constants._feature_cols import ELO_FEATURE_COLS
        from src.constants._feature_cols import ELO_INITIAL_RATING
        from src.constants._local_paths import LocalPaths
        from src.constants._results_cols import ResultsCols

        md = self._merged_data
        if md.empty or "horse_id" not in md.columns or ResultsCols.UMABAN not in md.columns:
            return

        snapshot: dict = {}
        path = LocalPaths.HORSE_RATINGS_PATH
        if os.path.exists(path):
            try:
                with open(path) as f:
                    snapshot = json.load(f)
            except (OSError, ValueError) as e:
                logger.warning("[ratings] スナップショット読込失敗 (初期値で継続): %s", e)

        md = md.copy()
        hids = md["horse_id"].astype(str)
        md["elo_rating"] = hids.map(
            lambda h: float(snapshot.get(h, {}).get("rating", ELO_INITIAL_RATING))
        )
        md["elo_n_races"] = hids.map(lambda h: float(snapshot.get(h, {}).get("n_races", 0)))
        # フィールド強度はレース（race_id インデックス）内で算出する
        md["elo_field_mean"] = md.groupby(level=0)["elo_rating"].transform("mean")
        md["elo_vs_field"] = md["elo_rating"] - md["elo_field_mean"]
        self._merged_data = md
        logger.info("[ratings] ライブ Elo 特徴量 %d 列を付与（snapshot=%d 頭）",
                    len(ELO_FEATURE_COLS), len(snapshot))

    def _merge_horse_trueskill(self):
        """ライブ経路: models/horse_trueskill.json のスナップショットを馬ごとに付与する。

        未来レースは実着順が無いため、学習時に保存した最新 (μ, σ) を参照する。
        保守的スキル（μ-3σ）のフィールド強度は当該レース出走馬内で算出する。
        スナップショットが無い馬は初期 μ/σ へフォールバックする。
        """
        import json
        import os

        from src.constants._feature_cols import TS_CONSERVATIVE_K
        from src.constants._feature_cols import TS_FEATURE_COLS
        from src.constants._feature_cols import TS_MU
        from src.constants._feature_cols import TS_SIGMA
        from src.constants._local_paths import LocalPaths
        from src.constants._results_cols import ResultsCols

        md = self._merged_data
        if md.empty or "horse_id" not in md.columns or ResultsCols.UMABAN not in md.columns:
            return

        snapshot: dict = {}
        path = LocalPaths.HORSE_TRUESKILL_PATH
        if os.path.exists(path):
            try:
                with open(path) as f:
                    snapshot = json.load(f)
            except (OSError, ValueError) as e:
                logger.warning("[trueskill] スナップショット読込失敗 (初期値で継続): %s", e)

        md = md.copy()
        hids = md["horse_id"].astype(str)
        md["ts_mu"] = hids.map(lambda h: float(snapshot.get(h, {}).get("mu", TS_MU)))
        md["ts_sigma"] = hids.map(lambda h: float(snapshot.get(h, {}).get("sigma", TS_SIGMA)))
        md["ts_conservative"] = md["ts_mu"] - TS_CONSERVATIVE_K * md["ts_sigma"]
        md["ts_n_races"] = hids.map(lambda h: float(snapshot.get(h, {}).get("n_races", 0)))
        md["ts_field_mean"] = md.groupby(level=0)["ts_conservative"].transform("mean")
        md["ts_vs_field"] = md["ts_conservative"] - md["ts_field_mean"]
        self._merged_data = md
        logger.info("[trueskill] ライブ TrueSkill 特徴量 %d 列を付与（snapshot=%d 頭）",
                    len(TS_FEATURE_COLS), len(snapshot))

    def _merge_horse_conditional_trueskill(self):
        """ライブ経路: models/horse_cond_trueskill.json から当該レース条件の μ/σ を付与。

        各次元（surface/distance/around）について現レースのバケットを解決し、
        snapshot[horse][dim][bucket] を参照する（無ければ初期 μ/σ）。保守的スキルの
        フィールド相対は当該レース出走馬内で算出する。
        """
        import json
        import os

        from src.constants._feature_cols import COND_DIMENSIONS
        from src.constants._feature_cols import COND_TS_FEATURE_COLS
        from src.constants._feature_cols import TS_CONSERVATIVE_K
        from src.constants._feature_cols import TS_MU
        from src.constants._feature_cols import TS_SIGMA
        from src.constants._local_paths import LocalPaths
        from src.constants._results_cols import ResultsCols
        from src.preprocessing._conditional_trueskill import race_buckets

        md = self._merged_data
        if md.empty or "horse_id" not in md.columns or ResultsCols.UMABAN not in md.columns:
            return

        snapshot: dict = {}
        path = LocalPaths.HORSE_COND_TRUESKILL_PATH
        if os.path.exists(path):
            try:
                with open(path) as f:
                    snapshot = json.load(f)
            except (OSError, ValueError) as e:
                logger.warning("[cond-trueskill] スナップショット読込失敗 (初期値で継続): %s", e)

        md = md.copy()
        prior_cons = TS_MU - TS_CONSERVATIVE_K * TS_SIGMA
        for dim in COND_DIMENSIONS:
            cons_vals = []
            n_vals = []
            for _, row in md.iterrows():
                bucket = race_buckets(row).get(dim)
                rec = (
                    snapshot.get(str(row["horse_id"]), {}).get(dim, {}).get(bucket, {})
                    if bucket is not None
                    else {}
                )
                mu = float(rec.get("mu", TS_MU))
                sigma = float(rec.get("sigma", TS_SIGMA))
                cons_vals.append(mu - TS_CONSERVATIVE_K * sigma if rec else prior_cons)
                n_vals.append(float(rec.get("n_races", 0)))
            md[f"ts_{dim}_conservative"] = cons_vals
            md[f"ts_{dim}_n_races"] = n_vals
            field_mean = md.groupby(level=0)[f"ts_{dim}_conservative"].transform("mean")
            md[f"ts_{dim}_vs_field"] = md[f"ts_{dim}_conservative"] - field_mean
        self._merged_data = md
        logger.info("[cond-trueskill] ライブ条件別 TrueSkill 特徴量 %d 列を付与（snapshot=%d 頭）",
                    len(COND_TS_FEATURE_COLS), len(snapshot))

    def _merge_horse_ability_kalman(self):
        """ライブ経路: models/horse_ability_kf.json から 1 ステップ先予測の能力を付与。

        スナップショットの (level, trend) から kf_level=level+trend を予測し、kf_workload は
        last_date→当該レース日の間隔で減衰させる。未知の馬は prior（level=0 等）。
        """
        import json
        import os

        import pandas as pd

        from src.constants._feature_cols import KF_FEATURE_COLS
        from src.constants._feature_cols import KF_INIT_LEVEL
        from src.constants._feature_cols import KF_INIT_TREND
        from src.constants._feature_cols import KF_INIT_VAR_LEVEL
        from src.constants._feature_cols import KF_Q_LEVEL
        from src.constants._feature_cols import KF_TREND_DECAY
        from src.constants._feature_cols import KF_WORKLOAD_HALFLIFE_DAYS
        from src.constants._local_paths import LocalPaths
        from src.constants._results_cols import ResultsCols

        md = self._merged_data
        if md.empty or "horse_id" not in md.columns or ResultsCols.UMABAN not in md.columns:
            return

        snapshot: dict = {}
        path = LocalPaths.HORSE_ABILITY_KF_PATH
        if os.path.exists(path):
            try:
                with open(path) as f:
                    snapshot = json.load(f)
            except (OSError, ValueError) as e:
                logger.warning("[ability-kf] スナップショット読込失敗 (初期値で継続): %s", e)

        md = md.copy()
        race_date = pd.to_datetime(md["date"], errors="coerce").iloc[0] if "date" in md.columns else None
        levels, trends, sigmas, workloads = [], [], [], []
        for _, row in md.iterrows():
            rec = snapshot.get(str(row["horse_id"]), {})
            if rec:
                trend = float(rec.get("trend", KF_INIT_TREND))
                level = float(rec.get("level", KF_INIT_LEVEL)) + trend  # 1 ステップ予測
                var_level = float(rec.get("var_level", KF_INIT_VAR_LEVEL)) + KF_Q_LEVEL
                workload = float(rec.get("workload", 0.0))
                last_date = pd.to_datetime(rec.get("last_date"), errors="coerce")
                if race_date is not None and pd.notna(last_date) and KF_WORKLOAD_HALFLIFE_DAYS > 0:
                    gap = max(0.0, (race_date - last_date).days)
                    workload *= 0.5 ** (gap / KF_WORKLOAD_HALFLIFE_DAYS)
                trend *= KF_TREND_DECAY
            else:
                level, trend = KF_INIT_LEVEL + KF_INIT_TREND, KF_INIT_TREND
                var_level, workload = KF_INIT_VAR_LEVEL + KF_Q_LEVEL, 0.0
            levels.append(level)
            trends.append(trend)
            sigmas.append(var_level ** 0.5)
            workloads.append(workload)
        md["kf_level"] = levels
        md["kf_trend"] = trends
        field_mean = md.groupby(level=0)["kf_level"].transform("mean")
        md["kf_level_vs_field"] = md["kf_level"] - field_mean
        md["kf_sigma"] = sigmas
        md["kf_workload"] = workloads
        self._merged_data = md
        logger.info("[ability-kf] ライブ能力 Kalman 特徴量 %d 列を付与（snapshot=%d 頭）",
                    len(KF_FEATURE_COLS), len(snapshot))
