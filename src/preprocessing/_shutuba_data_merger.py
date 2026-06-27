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
