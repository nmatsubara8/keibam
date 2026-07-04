from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from src.constants._results_cols import ResultsCols
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
        # ライブ推論用 履歴スナップショット（results 履歴由来の person_te / form を serve で再計算）。
        # 学習時に _write_serve_history が保存したもの。無ければ None（従来どおり 0 埋めにフォールバック）。
        self._history_results: pd.DataFrame | None = self._load_serve_history()
        # form-from-results の再構成元（基底 _merge_horse_results が参照）。serve は履歴から。
        self._form_history_results = self._history_results
        # 前年の人物年度別成績（jockey_py_* 等）。学習時と同じ RAW_PERSON_YEARLY_PATH を読む。
        self._person_yearly = self._load_person_yearly()

    @staticmethod
    def _load_person_yearly():
        import os

        from src.constants._local_paths import LocalPaths

        path = LocalPaths.RAW_PERSON_YEARLY_PATH
        if not path or not os.path.isfile(path):
            return pd.DataFrame()
        try:
            df = pd.read_pickle(path)
            return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
        except Exception:  # noqa: BLE001
            return pd.DataFrame()

    @staticmethod
    def _load_serve_history():
        import os

        from src.constants._local_paths import LocalPaths

        path = LocalPaths.SERVE_HISTORY_PATH
        if not path or not os.path.isfile(path):
            return None
        try:
            df = pd.read_pickle(path)
            return df if isinstance(df, pd.DataFrame) and not df.empty else None
        except Exception:  # noqa: BLE001
            return None

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
        # person_te / Elo は self._results に列を足す。FeatureEngineering は merged_data を読み、
        # merged_data は _merge_horse_results で self._results から確定される。よって両者は
        # **_merge_horse_results より前**に実行しないと merged_data に載らず 0 埋めされる
        # （学習時は person_te/horse_ratings が horse_results ステップより前なので載る）。
        self._merge_person_target_encoding_shutuba()
        self._merge_person_yearly()   # jockey_py_* 等（前年集計）。horse_results より前で merged_data に載せる
        self._merge_live_ratings()
        self._merge_horse_results()
        self._merge_horse_info()
        self._merge_peds()

    def _merge_person_target_encoding_shutuba(self) -> None:
        """ライブ推論で person_te（騎手/調教師/馬主×context）を履歴スナップショットから as-of 再計算する。

        学習時 `_merge_person_target_encoding` と同じ `build_person_form_features` を、履歴＋出馬表の
        結合に適用し出馬表行の encoding を取り出す（train/serve skew ゼロ）。履歴/日付が無ければスキップ
        （従来どおり 0 埋めにフォールバック）。env `KEIBA_DISABLE_PERSON_TE=1` で無効化。
        """
        import os

        if os.environ.get("KEIBA_DISABLE_PERSON_TE") == "1":
            return
        hist = self._history_results
        if hist is None or hist.empty or ResultsCols.RANK not in hist.columns:
            return
        if "date" not in self._results.columns:
            logger.info("[person_te-serve] 出馬表に date 列が無く as-of 計算不可のためスキップ")
            return
        race_date = pd.to_datetime(self._results["date"], errors="coerce").dropna().max()
        if pd.isna(race_date):
            return
        from src.preprocessing._target_encoding import person_te_for_upcoming

        alpha = float(os.environ.get("KEIBA_TE_ALPHA", "20"))
        feats = person_te_for_upcoming(
            hist, self._results, race_date, date_col="date", rank_col=ResultsCols.RANK, alpha=alpha
        )
        if feats.shape[1] == 0:
            return
        for c in feats.columns:
            self._results[c] = feats[c].to_numpy()
        logger.info("[person_te-serve] %d 列を付与（α=%.0f）: %s",
                    feats.shape[1], alpha, list(feats.columns))

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
