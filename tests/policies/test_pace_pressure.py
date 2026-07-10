"""展開因子 f_pace_pressure と 展開×脚質クロスの単体テスト。

前進安全性（過去脚質傾向 leg_type_binary からレース内先行勢比率を出す）と、
単独ではレース内同値・クロスで per-horse に変わることを固定する。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.policies._manji_factors import NA, factor_series


def _df():
    # R1: 5頭中 先行(0.0,0.2)=2, 差し(0.6,0.8,0.9)=3 → 先行比率 0.4 → "mid"
    # R2: 4頭中 先行(0.1,0.1,0.2)=3, 差し(0.7)=1 → 先行比率 0.75 → "many"
    idx = ["R1"] * 5 + ["R2"] * 4
    lb = [0.0, 0.2, 0.6, 0.8, 0.9, 0.1, 0.1, 0.2, 0.7]
    return pd.DataFrame({"leg_type_binary": lb}, index=idx)


def test_pace_pressure_is_race_level_and_forward_safe():
    s = factor_series(_df(), "pace_pressure")
    assert list(s.loc["R1"].unique()) == ["mid"]     # レース内は全馬同値
    assert list(s.loc["R2"].unique()) == ["many"]    # 先行比率0.75→many


def test_pace_pressure_na_when_leg_missing():
    df = pd.DataFrame({"other": [1, 2]}, index=["R1", "R1"])
    s = factor_series(df, "pace_pressure")
    assert (s == NA).all()                            # leg_type_binary 無し→na


def test_pace_x_leg_cross_varies_within_race():
    # 展開×脚質クロスは per-horse に変わる（先行馬 vs 差し馬で別バケット）
    cross = factor_series(_df(), "pace_pressure*leg_type")
    r1 = cross.loc["R1"]
    # R1 は "mid|front"（先行2頭）と "mid|back"（差し3頭）の2種に割れる
    assert set(r1.unique()) == {"mid|front", "mid|back"}
    r2 = cross.loc["R2"]
    assert set(r2.unique()) == {"many|front", "many|back"}
    # NA 伝播: 自分の脚質不明ならクロスも na
    df = _df().copy()
    df.iloc[0, df.columns.get_loc("leg_type_binary")] = np.nan
    c = factor_series(df, "pace_pressure*leg_type")
    assert c.iloc[0] == NA
