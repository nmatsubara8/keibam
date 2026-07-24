"""Phase 5: エンティティ（騎手/調教師/馬主/生産者）成績の集計と artifact 化。

背景（重要な既存バグ）: 集計系特徴量（jockey_win_rate 等）は学習時に self._results の
過去行から算出されるが、ライブ推論（ShutubaDataMerger）の self._results は出馬表のみで
過去履歴が空のため全 NaN に化け、feature_names_ の reindex fill 0 に依存していた。

是正パターン: 学習時に「最新スナップショット」（全 self._results から算出した id 別統計）を
CSV に保存し、ライブ推論ではこれをロードして id でマージする。学習時点の統計は推論時点で
利用可能な過去情報なので point-in-time 的に正当・リークしない。

レイヤ: preprocessing。統計計算は純関数、I/O は save/load のみ（DI でテスト決定化）。
"""

from __future__ import annotations

import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)

_RANK_COL = "着順"
_N_HORSES_COL = "n_horses"


def compute_entity_stats(
    past: pd.DataFrame, id_col: str, win_col: str, rank_col: str, recent_n: int
) -> pd.DataFrame:
    """past（過去行）から id_col 別の直近 recent_n レース勝率/相対平均着順を返す。

    返り値: index=id_col, 列=[win_col, rank_col]。past が空でも列付き空表を返す
    （マージ時に列を NaN で確実に生成するため）。着順==1 を勝ち、着順/頭数 を相対着順。
    """
    empty = pd.DataFrame(columns=[win_col, rank_col])
    if id_col not in past.columns or _RANK_COL not in past.columns or past.empty:
        return empty

    df = past[[id_col, _RANK_COL, "date"]].copy()
    if _N_HORSES_COL in past.columns:
        df[_N_HORSES_COL] = past[_N_HORSES_COL].values
    df = df.dropna(subset=[id_col])
    if df.empty:
        return empty

    df["_is_win"] = (pd.to_numeric(df[_RANK_COL], errors="coerce") == 1).astype(float)
    if _N_HORSES_COL in df.columns:
        df["_rel_rank"] = pd.to_numeric(df[_RANK_COL], errors="coerce") / pd.to_numeric(
            df[_N_HORSES_COL], errors="coerce"
        )
    else:
        df["_rel_rank"] = pd.to_numeric(df[_RANK_COL], errors="coerce")

    recent = df.sort_values("date", ascending=False).groupby(id_col).head(recent_n)
    stats = recent.groupby(id_col).agg(
        **{win_col: ("_is_win", "mean"), rank_col: ("_rel_rank", "mean")}
    )
    return stats


def entity_stats_path(directory: str, id_col: str) -> str:
    """id_col の統計 artifact パス（data/master/entity_stats_<id_col>.csv）。"""
    return os.path.join(directory, f"entity_stats_{id_col}.csv")


def save_entity_stats(stats: pd.DataFrame, path: str) -> None:
    """統計 DataFrame（index=id）を CSV に保存する。"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    stats.to_csv(path)
    logger.info("[entity_stats] saved: %s (%d rows)", path, len(stats))


def load_entity_stats(path: str) -> pd.DataFrame:
    """save_entity_stats が保存した CSV を index=id で復元する（無ければ空表）。"""
    if not path or not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path, index_col=0)
    df.index = df.index.astype(str)
    return df
