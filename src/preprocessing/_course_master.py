"""Phase 9: コース形状マスタの読込と results への付与（純関数 + load）。

course_master.csv（scripts/scrape_course_master.py が公式サイトから生成）を読み、
現レースの (開催, race_type, course_len) に左結合して course_* 属性列を付与する。
学習(DataMerger)・ライブ(ShutubaDataMerger)とも同じ CSV を参照し特徴量パリティを保つ。
"""

from __future__ import annotations

import logging
import os

import pandas as pd

from src.constants._course_master import (
    COURSE_MASTER_KEY_COLS,
    COURSE_MASTER_VALUE_COLS,
)

logger = logging.getLogger(__name__)


def _norm_place(s: pd.Series) -> pd.Series:
    """開催（place_id Int64 or PLACE コード str）を 2 桁ゼロ埋め文字列に正規化する。"""
    return pd.to_numeric(s, errors="coerce").astype("Int64").astype(str).str.zfill(2)


def load_course_master(path: str) -> pd.DataFrame:
    """course_master.csv を読み込む（無ければ空表）。返り値の place_code は正規化済み。"""
    if not path or not os.path.exists(path):
        return pd.DataFrame(columns=COURSE_MASTER_KEY_COLS + COURSE_MASTER_VALUE_COLS)
    df = pd.read_csv(path)
    if df.empty or not all(c in df.columns for c in COURSE_MASTER_KEY_COLS):
        return pd.DataFrame(columns=COURSE_MASTER_KEY_COLS + COURSE_MASTER_VALUE_COLS)
    df["place_code"] = _norm_place(df["place_code"])
    df["race_type"] = df["race_type"].astype(str)
    df["course_len"] = pd.to_numeric(df["course_len"], errors="coerce").astype("Int64")
    return df


def attach_course_features(results: pd.DataFrame, course_master: pd.DataFrame) -> pd.DataFrame:
    """results に course_<attr> 属性列を付与して返す。

    キー: 開催(place_id/コード) × race_type × course_len。course_master が空/キー欠損なら
    course_* 列を NaN で生成（学習/ライブの列パリティを保つ）。
    """
    out = results.copy()
    feat_cols = [f"course_{c}" for c in COURSE_MASTER_VALUE_COLS]

    needed = {"開催", "race_type", "course_len"}
    if course_master.empty or not needed.issubset(out.columns):
        for c in feat_cols:
            out[c] = float("nan")
        return out

    keyed = pd.DataFrame({
        "place_code": _norm_place(out["開催"]),
        "race_type": out["race_type"].astype(str),
        "course_len": pd.to_numeric(out["course_len"], errors="coerce").astype("Int64"),
    }, index=out.index)

    cm = course_master.rename(columns={c: f"course_{c}" for c in COURSE_MASTER_VALUE_COLS})
    cm = cm.drop_duplicates(subset=COURSE_MASTER_KEY_COLS)
    merged = keyed.merge(cm, on=COURSE_MASTER_KEY_COLS, how="left")
    merged.index = out.index

    for c in feat_cols:
        out[c] = merged[c].to_numpy() if c in merged.columns else float("nan")
    return out
