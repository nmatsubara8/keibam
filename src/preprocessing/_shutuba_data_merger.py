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
        self._merge_live_ratings()

    def _merge_live_ratings(self) -> None:
        """ライブ予測用に Elo スナップショットから出走馬のレーティング特徴を付与する。

        学習時（build_rating_frame の as-of 書き出し）と同一の _field_features を再現するため、
        最新スナップショット（HORSE_RATINGS_PATH）を読み、出走馬の現行レーティングで特徴量を作る。
        スナップショット無し/horse_id 欠如時はスキップ（予測時の reindex で 0 埋めにフォールバック）。
        """
        import json
        import os

        from src.constants._feature_cols import ELO_FEATURE_COLS
        from src.constants._local_paths import LocalPaths
        from src.preprocessing._ratings import features_from_snapshot

        path = LocalPaths.HORSE_RATINGS_PATH
        if not path or not os.path.isfile(path):
            return
        if "horse_id" not in self._results.columns or "馬番" not in self._results.columns:
            return
        try:
            with open(path, encoding="utf-8") as f:
                snapshot = json.load(f)
        except Exception:  # noqa: BLE001
            return
        if not snapshot:
            return

        base = self._results.reset_index()
        rid_col = "race_id" if "race_id" in base.columns else base.columns[0]
        rows: list[dict] = []
        for rid, g in base.groupby(rid_col):
            feats = features_from_snapshot(list(g["horse_id"]), snapshot)
            for _, row in g.iterrows():
                uma = pd.to_numeric(row["馬番"], errors="coerce")
                if pd.isna(uma):
                    continue
                rows.append({"race_id": str(rid), "馬番": int(uma), **feats.get(row["horse_id"], {})})
        self._ratings = (
            pd.DataFrame(rows, columns=["race_id", "馬番", *ELO_FEATURE_COLS])
            if rows else pd.DataFrame()
        )
        self._merge_horse_ratings()
