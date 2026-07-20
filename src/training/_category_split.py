"""featured_data をカテゴリ（全国/地方 × 芝/ダート/障害）で分割するユーティリティ。

分類ロジック自体は constants._model_category（純粋）に置き、ここでは
FeatureEngineering 出力の DataFrame 形状（race_id インデックス + race_type ダミー列）に
依存する「フレームからの race_type 復元」と「カテゴリ別分割」を提供する。

featured_data の想定形状:
- race_id をインデックスに持つ（1 レース = 複数行 = 出走頭数）。
- `dumminize_race_type()` 済みの場合 `race_type__芝` / `race_type__ダート` /
  `race_type__障害` のワンホット列を持つ（元 `race_type` 列は drop 済み）。
- ダミー化前の DataFrame が渡された場合は素の `race_type` 列を使う。

レイヤ: training（constants に依存）。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.constants._master import Master
from src.constants._model_category import ALL_CATEGORIES
from src.constants._model_category import categorize

# dumminize_race_type() が生成するワンホット列名（prefix="race_type_" + get_dummies の "_"）
_RACE_TYPE_VALUES = (Master.RACE_TYPE_TURF, Master.RACE_TYPE_DIRT, Master.RACE_TYPE_HURDLE)
_RACE_TYPE_DUMMY_COLS = {v: f"race_type__{v}" for v in _RACE_TYPE_VALUES}


def recover_race_type(df: pd.DataFrame) -> pd.Series:
    """DataFrame の各行の race_type 正準値（"芝"/"ダート"/"障害"）を復元する。

    - 素の `race_type` 列があればそれを返す。
    - 無ければ `race_type__*` ワンホット列から復元する。
    - どちらも無ければ全行 None。

    Returns
    -------
    pd.Series : df と同じインデックス・長さ（重複インデックス可）。値は正準文字列か None。
    """
    if "race_type" in df.columns:
        return df["race_type"]

    present = {v: c for v, c in _RACE_TYPE_DUMMY_COLS.items() if c in df.columns}
    arr = np.array([None] * len(df), dtype=object)
    for value, col in present.items():
        mask = df[col].to_numpy()
        # bool/0-1/float いずれのダミーでも真偽判定できるようにする
        arr[np.asarray(mask).astype(bool)] = value
    return pd.Series(arr, index=df.index)


def category_series(df: pd.DataFrame) -> pd.Series:
    """各行のカテゴリ slug（分類不能は None）を返す。

    主催者区分は race_id インデックスから、馬場種別は recover_race_type から導出する。
    """
    race_types = recover_race_type(df)
    cats = [
        categorize(race_id, rt)
        for race_id, rt in zip(df.index.to_numpy(), race_types.to_numpy(), strict=True)
    ]
    return pd.Series(cats, index=df.index)


def split_featured_by_category(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """featured_data をカテゴリ slug ごとの部分 DataFrame に分割する。

    分類不能行（race_type 不明）は含めない。データの存在するカテゴリのみを
    キーに持つ dict を返す（決定的順序）。
    """
    cats = category_series(df).to_numpy()
    out: dict[str, pd.DataFrame] = {}
    for slug in ALL_CATEGORIES:
        mask = cats == slug
        if mask.any():
            out[slug] = df[mask]
    return out


def category_race_counts(df: pd.DataFrame) -> dict[str, int]:
    """カテゴリ slug ごとのレース数（ユニーク race_id 数）を返す。"""
    groups = split_featured_by_category(df)
    return {slug: int(sub.index.nunique()) for slug, sub in groups.items()}
