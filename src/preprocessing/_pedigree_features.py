"""血統（父=sire / 母父=damsire）の産駒集計特徴量を生成する関数群。

`DataMerger` の血統族メソッドを `(results, target_date, *, hr_with_sire_dict, peds) -> results`
の自由関数として切り出したもの（リーク無し: hr_with_sire_dict は date < target_date の
horse_results サブセットを保持）。DataMerger からは薄い委譲メソッド経由で呼ぶ。

状態（per-date の産駒成績スライス・peds テーブル）は明示引数で受け取り、ライフサイクル
（_separated_hr_with_sire_dict の構築/破棄）は DataMerger 側に残す。
"""

from __future__ import annotations

import pandas as pd

from src.constants._feature_cols import SIRE_RECENT_YEARS
from src.constants._horse_results_cols import HorseResultsCols as HRCols


def add_pedigree_stats(
    results: pd.DataFrame,
    target_date,
    peds_col: str,
    prefix: str,
    *,
    hr_with_sire_dict: dict,
    peds: pd.DataFrame,
) -> pd.DataFrame:
    """血統（peds_col）単位の産駒集計特徴量を追加する（父/母父で共用、リーク無し）。

    hr_with_sire_dict は _separate_by_date() 内で peds_0/peds_2 を付与した horse_results の
    サブセット（date < target_date）を保持する。
    - ``{prefix}_win_rate``        : 産駒の全期間勝率
    - ``{prefix}_avg_rank``        : 産駒の全期間平均相対着順（着順/頭数）
    - ``{prefix}_recent_win_rate`` : 直近 SIRE_RECENT_YEARS 年の産駒勝率
    """
    if target_date not in hr_with_sire_dict:
        return results

    phs = hr_with_sire_dict[target_date]
    rank_col = HRCols.RANK  # '着順'
    n_horses_col = HRCols.N_HORSES  # '頭数'

    if peds_col not in phs.columns or phs.empty or rank_col not in phs.columns:
        return results

    win_col, rank_name, recent_col = f"{prefix}_win_rate", f"{prefix}_avg_rank", f"{prefix}_recent_win_rate"

    phs = phs.copy()
    # category dtype の groupby 問題を避けるため血統キーは str 化する
    phs["_ped_key"] = phs[peds_col].astype(str)
    phs["_is_win"] = (phs[rank_col] == 1).astype(float)
    if n_horses_col in phs.columns:
        phs["_rel_rank"] = phs[rank_col] / phs[n_horses_col]

    agg_dict: dict = {"_is_win": "mean"}
    if "_rel_rank" in phs.columns:
        agg_dict["_rel_rank"] = "mean"

    ped_all = phs.groupby("_ped_key").agg(agg_dict)
    ped_all.columns = [win_col if c == "_is_win" else rank_name for c in ped_all.columns]

    # 直近 N 年
    cutoff = pd.Timestamp(target_date) - pd.DateOffset(years=SIRE_RECENT_YEARS)
    recent = phs[phs["date"] >= cutoff]
    if not recent.empty:
        ped_recent = recent.groupby("_ped_key")["_is_win"].mean().rename(recent_col)
        ped_all = ped_all.join(ped_recent, how="left")
    else:
        ped_all[recent_col] = float("nan")

    # 現役馬（出馬表）の血統キーを peds から引く
    if peds_col not in peds.columns:
        return results
    horse_ped = peds[[peds_col]].reset_index()
    horse_ped["_ped_key"] = horse_ped[peds_col].astype(str)

    horse_ped_indexed = horse_ped[["horse_id", "_ped_key"]].set_index("horse_id")
    # horse_id の型を揃える（results は str、peds 由来 index は DB 復元で Int64 になりうる）
    horse_ped_indexed.index = horse_ped_indexed.index.astype(str)
    results = results.copy()
    results["horse_id"] = results["horse_id"].astype(str)
    results = results.merge(horse_ped_indexed, left_on="horse_id", right_index=True, how="left")
    results = results.merge(ped_all, left_on="_ped_key", right_index=True, how="left")
    results = results.drop(columns=["_ped_key"], errors="ignore")

    return results


def add_sire_stats(
    results: pd.DataFrame, target_date, *, hr_with_sire_dict: dict, peds: pd.DataFrame
) -> pd.DataFrame:
    """種牡馬（父=peds_0）産駒の集計特徴量を追加（sire_win_rate / sire_avg_rank / sire_recent_win_rate）。"""
    return add_pedigree_stats(
        results, target_date, "peds_0", "sire", hr_with_sire_dict=hr_with_sire_dict, peds=peds
    )


def add_damsire_stats(
    results: pd.DataFrame, target_date, *, hr_with_sire_dict: dict, peds: pd.DataFrame
) -> pd.DataFrame:
    """母父（broodmare sire=peds_32）の産駒集計特徴量を追加（damsire_*）。

    母父は距離・ダート適性に効く競馬の重要軸。父(sire)と同じく過去走の産駒成績で集計する。
    血統表は行順フラット化のため母=peds_31・母父=peds_32（実データで検証済み。
    peds_2 は父父父であり母父ではない）。
    """
    return add_pedigree_stats(
        results, target_date, "peds_32", "damsire", hr_with_sire_dict=hr_with_sire_dict, peds=peds
    )
