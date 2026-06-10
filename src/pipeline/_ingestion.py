"""日次取込ジョブ: 終了レースの結果・払戻を raw pickle に冪等追記し、
featured_data を再生成して保存する。

副作用の隔離原則:
- 純粋ロジック (find_new_race_ids / append_idempotent / existing_race_ids)
  は外部依存なし・単体テスト可能。
- I/O アダプタ (RawDataFetcher / FeaturedDataBuilder / load_raw / save_raw)
  は selenium / bs4 の遅延 import に閉じる。
  DI で差し込むためテストはスタブ fetcher/builder を使う。

レイヤ: pipeline（operation の上位、training を利用可）。
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import logging
import os
from typing import Protocol

import pandas as pd

from src.constants._local_paths import LocalPaths

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 設定 DTO（frozen）
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class IngestConfig:
    raw_results_path: str = LocalPaths.RAW_RESULTS_PATH
    raw_race_info_path: str = LocalPaths.RAW_RACE_INFO_PATH
    raw_return_tables_path: str = LocalPaths.RAW_RETURN_TABLES_PATH
    raw_horse_results_path: str = LocalPaths.RAW_HORSE_RESULTS_PATH
    raw_horse_info_path: str = LocalPaths.RAW_HORSE_INFO_PATH
    raw_peds_path: str = LocalPaths.RAW_PEDS_PATH
    featured_data_path: str = LocalPaths.FEATURED_DATA_PATH
    # Phase 1: --force 指定時、対象 race_id の DB 行を先に削除してから再投入する
    force: bool = False


# ---------------------------------------------------------------------------
# 純粋ロジック
# ---------------------------------------------------------------------------


def existing_race_ids(df: pd.DataFrame) -> set:
    """race_id インデックスを持つ DataFrame から既存 race_id の集合を返す。"""
    if df.empty:
        return set()
    return set(df.index.unique())


def find_new_race_ids(existing_ids: set, candidate_ids: list) -> list:
    """既存 race_id に存在しない race_id のみを返す（純粋関数）。"""
    return [rid for rid in candidate_ids if rid not in existing_ids]


def append_idempotent(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    """new の行のうち、既存 index に含まれないものだけを追記する（純粋関数）。

    race_id を index に持つ DataFrame を前提とする。
    """
    if new.empty:
        return existing
    if existing.empty:
        return new.copy()
    new_only = new.loc[~new.index.isin(set(existing.index))]
    if new_only.empty:
        return existing
    return pd.concat([existing, new_only])


# ---------------------------------------------------------------------------
# I/O ヘルパ（遅延 import の必要がある重い依存は持たない軽量 pandas 操作）
# ---------------------------------------------------------------------------


def load_raw(path: str) -> pd.DataFrame:
    """pickle を読み込む（ファイルが無ければ空 DataFrame）。"""
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_pickle(path)


def save_raw(df: pd.DataFrame, path: str) -> None:
    """pickle を保存する（ディレクトリは自動作成）。"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_pickle(path)


# ---------------------------------------------------------------------------
# DI 境界（プロトコル）
# ---------------------------------------------------------------------------


class RawDataFetcher(Protocol):
    """終了レースの生データ取得の抽象（netkeiba スクレイピングを隠蔽）。"""

    def fetch_results(self, race_ids: list) -> pd.DataFrame: ...

    def fetch_race_info(self, race_ids: list) -> pd.DataFrame: ...

    def fetch_return_tables(self, race_ids: list) -> pd.DataFrame: ...


class FeaturedDataBuilder(Protocol):
    """raw pickle から特徴量 DataFrame を再生成する抽象（DataMerger/FeatureEngineering を隠蔽）。"""

    def build(self, config: IngestConfig) -> pd.DataFrame: ...


# ---------------------------------------------------------------------------
# ジョブ
# ---------------------------------------------------------------------------


class IngestJob:
    """日次取込ジョブ（DI で fetcher / builder を受け取る）。

    冪等性: 同じ race_id は append_idempotent が弾くため何度呼んでも安全。
    例外継続: 1 テーブルの取得失敗が全体を止めないよう try/except で継続する。
    """

    def __init__(
        self,
        fetcher: RawDataFetcher,
        builder: FeaturedDataBuilder,
        config: IngestConfig | None = None,
    ) -> None:
        self._fetcher = fetcher
        self._builder = builder
        self._cfg = config or IngestConfig()

    def run(self, candidate_race_ids: list) -> dict:
        """指定 race_ids を調べ、新規分だけ取込んで featured_data を再生成する。

        Parameters
        ----------
        candidate_race_ids : 終了済みとして取込を試みる race_id のリスト。

        Returns
        -------
        dict : status, n_new (新規取込数), n_total (全取込累計数), ingest_time。
        """
        # Phase 1: --force 指定時は対象 race_id の DB 行を先に削除し、
        # 「DB 上の既存行」を理由に upsert がスキップされないようにする。
        # pickle 側のマージは既存ロジック（update_rawdata 経由）が new_df を優先するので問題なし。
        if self._cfg.force and candidate_race_ids:
            try:
                from src.storage import RawDataRepo

                repo = RawDataRepo()
                for alias in ("raw_results", "raw_race_info", "raw_return_tables"):
                    deleted = repo.delete_by_index(alias, candidate_race_ids)
                    logger.info("[ingest --force] DB delete %s: %d rows", alias, deleted)
            except Exception as e:  # noqa: BLE001
                logger.warning("[ingest --force] DB delete 失敗 (non-fatal): %s", e)

        existing_results = load_raw(self._cfg.raw_results_path)
        existing_ids = existing_race_ids(existing_results)

        # --force 時は重複判定をスキップして全候補を取り直す
        if self._cfg.force:
            new_ids = list(candidate_race_ids)
        else:
            new_ids = find_new_race_ids(existing_ids, candidate_race_ids)
        if not new_ids:
            return {
                "status": "no_new_races",
                "n_new": 0,
                "n_total": len(existing_ids),
            }

        # results
        try:
            new_results = self._fetcher.fetch_results(new_ids)
            updated = append_idempotent(existing_results, new_results)
            save_raw(updated, self._cfg.raw_results_path)
        except Exception as e:
            logger.error("[ingest] fetch_results failed: %s", e)
            return {"status": "error", "message": str(e), "n_new": 0}

        # race_info（失敗しても results は保存済み）
        try:
            new_info = self._fetcher.fetch_race_info(new_ids)
            if not new_info.empty:
                updated_info = append_idempotent(load_raw(self._cfg.raw_race_info_path), new_info)
                save_raw(updated_info, self._cfg.raw_race_info_path)
        except Exception as e:
            logger.warning("[ingest] fetch_race_info failed (non-fatal): %s", e)

        # return_tables（失敗しても継続）
        try:
            new_ret = self._fetcher.fetch_return_tables(new_ids)
            if not new_ret.empty:
                updated_ret = append_idempotent(load_raw(self._cfg.raw_return_tables_path), new_ret)
                save_raw(updated_ret, self._cfg.raw_return_tables_path)
        except Exception as e:
            logger.warning("[ingest] fetch_return_tables failed (non-fatal): %s", e)

        # 特徴量再生成
        try:
            featured = self._builder.build(self._cfg)
            save_raw(featured, self._cfg.featured_data_path)
        except Exception as e:
            logger.warning("[ingest] featured_data build failed (non-fatal): %s", e)

        n_total = len(existing_race_ids(load_raw(self._cfg.raw_results_path)))
        return {
            "status": "ok",
            "n_new": len(new_ids),
            "n_total": n_total,
            "ingest_time": dt.datetime.now().isoformat(),
        }
