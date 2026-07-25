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
        # Phase 3: ライブ側は学習時に保存した基準タイム表をロードする（build しない）
        from src.constants._local_paths import LocalPaths
        from src.constants._speed_index import SPEED_INDEX_TEST_SIZE

        self._speed_index_build = False
        self._speed_index_test_size = SPEED_INDEX_TEST_SIZE
        self._speed_index_base_path = LocalPaths.BASE_TIME_TABLE_PATH
        # Phase 5: ライブ側は学習時保存のエンティティ統計をロードする（build しない）
        self._entity_stats_build = False
        self._entity_stats_dir = LocalPaths.MASTER_DIR
        # Phase 9: コース形状マスタ（学習と同じ CSV を読む）
        self._course_master_path = LocalPaths.COURSE_MASTER_PATH

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
        # Phase 9: コース形状マスタを付与（学習と同じ CSV）
        self._attach_course_master()
        self._merge_horse_results()
        self._merge_horse_info()
        self._merge_peds()
