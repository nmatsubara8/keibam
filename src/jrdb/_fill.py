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

# race_info で fill してよい列（JRDB が全年で確実に供給できる列）。
# race_class は JRDB SED の joken を 100% 正準クラス(netkeiba 表記)へ写せる（実データ 559,700 行で
# 被覆率 100% を確認）ため保持する＝netkeiba が 2023+ のみ充填する欠損を JRDB で全年埋める。これを
# NaN 化していたため featured の race_class 一族(level/one-hot/TE)が全 DEAD になっていた（是正）。
# 他（place/place_id/times/days/around/time/age/sex/race_condition）は netkeiba が 2023+ のみ充填で
# JRDB 側マップも不完全なため、2023 前後の分布不連続を避けて NaN に落とす。
FILL_RACE_INFO_KEEP = ("race_type", "weather", "ground_state1", "ground_state2",
                       "course_len", "date", "race_class")


def build_fill_tables(
    sed: pd.DataFrame,
    *,
    jockey_xwalk: Optional[pd.DataFrame] = None,
    trainer_xwalk: Optional[pd.DataFrame] = None,
    kyi: Optional[pd.DataFrame] = None,
    minimal_race_info: bool = True,
) -> dict[str, pd.DataFrame]:
    """SED から results / race_info / horse_results の fill 用 DataFrame を作る。

    kyi を渡すと results の 枠番・性齢 を KYI から補う（`build_raw_results` 参照）。
    minimal_race_info=True で fill 方針を適用（FILL_RACE_INFO_KEEP 以外を NaN 化）。
    """
    results = build_raw_results(sed, jockey_xwalk=jockey_xwalk, trainer_xwalk=trainer_xwalk,
                               kyi=kyi)
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


def race_ids_of(df: pd.DataFrame) -> pd.Series:
    """race_id を index / 列どちらに持っていても Series で返す（空なら空 Series）。"""
    if df is None or len(df) == 0:
        return pd.Series([], dtype=str)
    if df.index.name == "race_id":
        return df.index.to_series().astype(str)
    if "race_id" in df.columns:
        return df["race_id"].astype(str)
    return pd.Series([], dtype=str)


def drop_race_ids(existing: pd.DataFrame, race_ids: Iterable) -> pd.DataFrame:
    """existing から指定 race_id 群の行を除去する（overwrite の前段）。

    JRDB が持つ JRA race_id だけを消して JRDB 行で差し替えるため。JRDB に無い race_id
    （NAR や JRDB 未収録レース）は race_ids に含まれないので保持される＝NAR 保護。
    """
    if existing is None or len(existing) == 0:
        return existing
    ids = {str(x) for x in race_ids}
    if not ids:
        return existing
    keep = ~race_ids_of(existing).isin(ids).to_numpy()
    return existing[keep]


def to_raw_shape(df: pd.DataFrame) -> pd.DataFrame:
    """名前付き index（race_id 等）を列へ戻す。

    netkeiba raw pickle は **RangeIndex＋キー列**（race_id/horse_id を列で持つ）構造。
    adapters は index=race_id で返すため、concat 前にこれで列へ戻して構造を揃える。
    """
    if df is None or df.empty:
        return df
    return df.reset_index() if df.index.name is not None else df


def new_horse_results(hr: pd.DataFrame, existing_keys: Iterable) -> pd.DataFrame:
    """horse_results を既存 (horse_id, 日付) に無い行だけへ絞る。horse_id 欠損行も落とす。"""
    if hr is None or hr.empty:
        return hr
    ex = set(existing_keys)
    h = hr[hr["horse_id"].notna()].copy()
    keys = list(zip(h["horse_id"].astype(str), h["日付"].astype(str), strict=False))
    mask = pd.Series([k not in ex for k in keys], index=h.index)
    return h[mask]
