"""JRDB→netkeiba fill: 欠損年（2021-2022 中央）を JRDB 由来行で埋める組み立て。

`_adapter` の build_raw_* で生成した行を netkeiba raw スキーマへ揃え、fill 方針
（recent-only 列は NaN）を適用し、既存 netkeiba に無い分だけ抽出する純関数群。
方針:
  - results / race_info: 対象年（--year）の **既存に無い race_id だけ**追加（既存年・NAR 非改変）。
  - horse_results: 全 JRDB 年から **既存に無い (horse_id, 日付) だけ**追加（netkeiba が疎な
    過去走履歴を包括補完 → 対象年だけでなく後年レースの前走特徴量も正しくなる）。
NAR（地方）は JRDB に無いので一切触らない。既存行も上書きしない（新規のみ union）。
"""
from __future__ import annotations

from typing import Iterable, Optional

import pandas as pd

from src.jrdb._adapter import (
    build_raw_horse_results,
    build_raw_race_info,
    build_raw_results,
)

# race_info で全年充填される（= fill してよい）列。他（place/place_id/times/days/around/
# time/age/race_class/sex/race_condition）は netkeiba が 2023+ のみ充填のため NaN に落とす
# （2023 前後の分布不連続を避ける）。
FILL_RACE_INFO_KEEP = ("race_type", "weather", "ground_state1", "ground_state2",
                       "course_len", "date")


def build_fill_tables(
    sed: pd.DataFrame,
    *,
    jockey_xwalk: Optional[pd.DataFrame] = None,
    trainer_xwalk: Optional[pd.DataFrame] = None,
    minimal_race_info: bool = True,
) -> dict[str, pd.DataFrame]:
    """SED から results / race_info / horse_results の fill 用 DataFrame を作る。

    minimal_race_info=True で fill 方針を適用（FILL_RACE_INFO_KEEP 以外を NaN 化）。
    """
    results = build_raw_results(sed, jockey_xwalk=jockey_xwalk, trainer_xwalk=trainer_xwalk)
    race_info = build_raw_race_info(sed)
    if minimal_race_info and not race_info.empty:
        for c in [c for c in race_info.columns if c not in FILL_RACE_INFO_KEEP]:
            race_info[c] = pd.NA
    horse_results = build_raw_horse_results(sed)
    return {"results": results, "race_info": race_info, "horse_results": horse_results}


def _year(rid: object) -> str:
    return str(rid)[:4]


def filter_years(df: pd.DataFrame, years: Iterable[str]) -> pd.DataFrame:
    """index=race_id の表を対象年だけへ絞る（results/race_info 用）。"""
    if df is None or df.empty:
        return df
    ys = {str(y) for y in years}
    return df[df.index.map(_year).isin(ys)]


def new_by_race_id(df: pd.DataFrame, existing_race_ids: Iterable) -> pd.DataFrame:
    """index=race_id の表を、既存 race_id に無い行だけへ絞る（既存年・NAR 非改変）。"""
    if df is None or df.empty:
        return df
    ex = {str(x) for x in existing_race_ids}
    return df[~df.index.astype(str).isin(ex)]


def new_horse_results(hr: pd.DataFrame, existing_keys: Iterable) -> pd.DataFrame:
    """horse_results を既存 (horse_id, 日付) に無い行だけへ絞る。horse_id 欠損行も落とす。"""
    if hr is None or hr.empty:
        return hr
    ex = set(existing_keys)
    h = hr[hr["horse_id"].notna()].copy()
    keys = list(zip(h["horse_id"].astype(str), h["日付"].astype(str), strict=False))
    mask = pd.Series([k not in ex for k in keys], index=h.index)
    return h[mask]
