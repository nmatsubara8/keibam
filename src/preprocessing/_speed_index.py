"""Phase 3: スピード指数（IDM 相当）の純関数群。

コース（開催×race_type×course_len×馬場）ごとの基準タイム（mean/std）で time_seconds を
標準化し、速さを相対指標化する。

リーク方針: 基準タイムは train 期間限定（cutoff_date 未満）で算出し artifact 化する。
基準統計はコース物理特性の近似（準静的）だが、テスト期間の time を混ぜると AUC 評価が
楽観化するため、cutoff で分布リークを遮断する。ライブ推論は保存済み artifact をロードする。

レイヤ: preprocessing。I/O は save/load のみ、統計計算は純関数（DI でテスト決定化）。
"""

from __future__ import annotations

import logging
import os

import numpy as np
import pandas as pd

from src.constants._speed_index import (
    BASE_TIME_KEYS_COARSE,
    BASE_TIME_KEYS_FINE,
    BASE_TIME_MIN_COUNT,
    SPEED_INDEX_BASE,
    SPEED_INDEX_SCALE,
)

logger = logging.getLogger(__name__)

_TIME_COL = "time_seconds"
_SCOPE_COL = "_scope"
_STAT_COLS = ["mean", "std", "count"]


def _agg_by_keys(hr: pd.DataFrame, keys: list) -> pd.DataFrame:
    """keys で time_seconds を mean/std/count 集計する（キー欠損・空なら空表）。"""
    if hr.empty or not all(k in hr.columns for k in keys):
        return pd.DataFrame()
    grouped = hr.groupby(keys)[_TIME_COL].agg(_STAT_COLS)
    return grouped


def build_base_time_table(
    horse_results: pd.DataFrame, cutoff_date=None
) -> dict:
    """コース別の基準タイム統計（mean/std/count）を細/粗キーで返す。

    cutoff_date 指定時は date < cutoff_date のレースのみ使用（リーク遮断）。
    返り値: {"fine": DataFrame(index=細キー), "coarse": DataFrame(index=粗キー)}。
    """
    empty = {"fine": pd.DataFrame(), "coarse": pd.DataFrame()}
    if _TIME_COL not in horse_results.columns:
        return empty

    hr = horse_results
    if cutoff_date is not None and "date" in hr.columns:
        hr = hr[pd.to_datetime(hr["date"], errors="coerce") < pd.to_datetime(cutoff_date)]
    # time が数値の行のみ
    time = pd.to_numeric(hr[_TIME_COL], errors="coerce")
    hr = hr[time.notna()]

    return {
        "fine": _agg_by_keys(hr, BASE_TIME_KEYS_FINE),
        "coarse": _agg_by_keys(hr, BASE_TIME_KEYS_COARSE),
    }


def _lookup(hr: pd.DataFrame, table: pd.DataFrame, keys: list):
    """hr の各行に対し table（index=keys）から mean/std/count を引く（欠損は NaN）。"""
    n = len(hr)
    nan = np.full(n, np.nan)
    if table.empty or not all(k in hr.columns for k in keys):
        return nan, nan, nan
    idx = pd.MultiIndex.from_frame(hr[keys]) if len(keys) > 1 else pd.Index(hr[keys[0]])
    mean = table["mean"].reindex(idx).to_numpy()
    std = table["std"].reindex(idx).to_numpy()
    count = table["count"].reindex(idx).to_numpy()
    return mean, std, count


def attach_speed_index(horse_results: pd.DataFrame, base_table: dict) -> pd.DataFrame:
    """horse_results に speed_index 列を付与して返す。

    各行の（開催,race_type,course_len,馬場）の基準統計で標準化。細キーの count が
    BASE_TIME_MIN_COUNT 未満/欠損なら粗キー（race_type,course_len）へフォールバック。
    いずれも無ければ NaN。速い（タイム小）ほど高い値になる。
    """
    hr = horse_results.copy()
    if _TIME_COL not in hr.columns or not base_table:
        hr["speed_index"] = np.nan
        return hr

    time = pd.to_numeric(hr[_TIME_COL], errors="coerce").to_numpy()

    fmean, fstd, fcount = _lookup(hr, base_table.get("fine", pd.DataFrame()), BASE_TIME_KEYS_FINE)
    # 細キーは十分なサンプルがある場合のみ採用
    fine_ok = fcount >= BASE_TIME_MIN_COUNT
    mean = np.where(fine_ok, fmean, np.nan)
    std = np.where(fine_ok, fstd, np.nan)

    # 粗キーでフォールバック（細が無効な行のみ）
    cmean, cstd, _ = _lookup(hr, base_table.get("coarse", pd.DataFrame()), BASE_TIME_KEYS_COARSE)
    need = np.isnan(mean)
    mean = np.where(need, cmean, mean)
    std = np.where(need, cstd, std)

    # std<=0 / NaN は 0 除算回避で NaN
    with np.errstate(invalid="ignore", divide="ignore"):
        std_safe = np.where((std > 0) & np.isfinite(std), std, np.nan)
        hr["speed_index"] = SPEED_INDEX_BASE + SPEED_INDEX_SCALE * (mean - time) / std_safe
    return hr


def save_base_time_table(base_table: dict, path: str) -> None:
    """基準タイム表（fine/coarse）を単一 CSV に保存する（_scope 列で区別）。"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    frames = []
    for scope in ("fine", "coarse"):
        df = base_table.get(scope)
        if df is not None and not df.empty:
            d = df.reset_index()
            d[_SCOPE_COL] = scope
            frames.append(d)
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    out.to_csv(path, index=False)
    logger.info("[speed_index] base table saved: %s (%d rows)", path, len(out))


def load_base_time_table(path: str) -> dict:
    """save_base_time_table が保存した CSV を dict へ復元する（無ければ空表）。"""
    empty = {"fine": pd.DataFrame(), "coarse": pd.DataFrame()}
    if not path or not os.path.exists(path):
        return empty
    df = pd.read_csv(path)
    if df.empty or _SCOPE_COL not in df.columns:
        return empty
    result = {}
    for scope, keys in (("fine", BASE_TIME_KEYS_FINE), ("coarse", BASE_TIME_KEYS_COARSE)):
        sub = df[df[_SCOPE_COL] == scope]
        if sub.empty or not all(k in sub.columns for k in keys):
            result[scope] = pd.DataFrame()
        else:
            result[scope] = sub.set_index(keys)[_STAT_COLS]
    return result
