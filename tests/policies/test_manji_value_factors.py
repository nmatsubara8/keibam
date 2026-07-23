"""卍流『妙味度』ファクター（騎手/厩舎/種牡馬/条件）のテスト。"""

import numpy as np
import pandas as pd

from src.constants._results_cols import ResultsCols
from src.policies._manji_factors import FACTORS, NA, factor_series


def _df(cols, index=None, n=None):
    df = pd.DataFrame(cols)
    if index is not None:
        df.index = pd.Index(index, name="race_id")
    return df


def test_jockey_and_trainer_raw_name_buckets():
    df = _df({
        ResultsCols.JOCKEY: ["武豊", "ルメール", "", "nan"],
        ResultsCols.TRAINER: ["矢作芳人", "堀宣行", "友道康夫", "0"],
    })
    jk = factor_series(df, "jockey")
    tr = factor_series(df, "trainer")
    assert list(jk[:2]) == ["武豊", "ルメール"]
    assert jk.iloc[2] == NA and jk.iloc[3] == NA  # 空・nan は na
    assert list(tr[:3]) == ["矢作芳人", "堀宣行", "友道康夫"]
    assert tr.iloc[3] == NA  # "0" は na


def test_sire_individual_distinct_from_sire_line():
    df = _df({"種牡馬": ["キズナ", "ドレフォン", ""]})
    s = factor_series(df, "sire")
    assert list(s[:2]) == ["キズナ", "ドレフォン"]
    assert s.iloc[2] == NA


def test_race_type_and_dist_band():
    df = _df({"race_type": ["芝", "ダート", "障害", "芝"],
              "course_len": [1200, 1800, 2400, 1600]})
    rt = factor_series(df, "race_type")
    assert list(rt) == ["芝", "ダート", "障害", "芝"]
    db = factor_series(df, "dist_band")
    assert list(db) == ["sprint_mile", "mid", "long", "sprint_mile"]


def test_dist_band_handles_bucketed_course_len():
    # course_len が 100m 単位バケット（12=1200m）でも同じ帯に落ちる
    df = _df({"course_len": [12, 18, 24]})
    db = factor_series(df, "dist_band")
    assert list(db) == ["sprint_mile", "mid", "long"]


def test_place_from_race_id_index():
    df = _df({ResultsCols.UMABAN: [1, 2, 3]},
             index=["202605010101", "202606010101", "bad"])
    pl = factor_series(df, "place")
    assert list(pl[:2]) == ["05", "06"]
    assert pl.iloc[2] == NA  # race_id 形式でない


def test_condition_cross_available_via_registry():
    df = _df({ResultsCols.JOCKEY: ["武豊", "武豊"], "race_type": ["芝", "ダート"]})
    cx = factor_series(df, "jockey*race_type")
    assert list(cx) == ["武豊|芝", "武豊|ダート"]


def test_new_factors_registered():
    for f in ("jockey", "trainer", "sire", "race_type", "dist_band", "place"):
        assert f in FACTORS


def test_missing_columns_return_na():
    df = _df({ResultsCols.UMABAN: [1, 2]})
    for f in ("jockey", "trainer", "sire", "race_type", "dist_band"):
        assert (factor_series(df, f) == NA).all()
