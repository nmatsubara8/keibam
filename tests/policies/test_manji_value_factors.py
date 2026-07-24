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


def test_turn_from_place_code():
    # 05東京=左, 04新潟=左, 09阪神=右, 06中山=右, 07中京=左
    df = _df({ResultsCols.UMABAN: [1, 2, 3, 4, 5]},
             index=["202605010101", "202604010101", "202609010101",
                    "202606010101", "202607010101"])
    tn = factor_series(df, "turn")
    assert list(tn) == ["left", "left", "right", "right", "left"]


def test_condition_cross_available_via_registry():
    df = _df({ResultsCols.JOCKEY: ["武豊", "武豊"], "race_type": ["芝", "ダート"]})
    cx = factor_series(df, "jockey*race_type")
    assert list(cx) == ["武豊|芝", "武豊|ダート"]


def test_new_factors_registered():
    for f in ("jockey", "trainer", "sire", "race_type", "dist_band", "place",
              "birth_month_2yo", "foreign_bred", "prev_kanto", "prev_kansai",
              "prev_overseas", "pedigree_2gen"):
        assert f in FACTORS


def test_birth_month_2yo_only_for_2yo():
    df = _df({"年齢": [2, 2, 2, 3], "生年月日": ["2024-02-10", "2024-05-01", "2024-08-20", "2023-02-01"]})
    b = factor_series(df, "birth_month_2yo")
    assert list(b[:3]) == ["early", "mid", "late"]  # 2月/5月/8月
    assert b.iloc[3] == NA  # 3歳は対象外


def test_foreign_bred_from_umakubun():
    df = _df({"馬区分": ["(外)", "抽選", "マル外", ""]})
    b = factor_series(df, "foreign_bred")
    assert list(b) == ["foreign", "domestic", "foreign", NA]


def test_prev_kanto_kansai_overseas_from_prev_place():
    df = _df({"前走場所": ["東京", "京都", "香港", "門別"]})
    assert list(factor_series(df, "prev_kanto")) == ["yes", "no", "no", "no"]
    assert list(factor_series(df, "prev_kansai")) == ["no", "yes", "no", "no"]
    assert list(factor_series(df, "prev_overseas")) == ["no", "no", "yes", "no"]


def test_prev_travel_na_without_prev_place():
    df = _df({ResultsCols.UMABAN: [1, 2]})
    for f in ("prev_kanto", "prev_kansai", "prev_overseas"):
        assert (factor_series(df, f) == NA).all()


def test_pedigree_2gen_combines_sire_and_damsire():
    # daikeito が認識する名前で（父系|母父系 に結合される）
    df = _df({"父": ["ディープインパクト", "キングカメハメハ"],
              "母父": ["サンデーサイレンス", "キングカメハメハ"]})
    b = factor_series(df, "pedigree_2gen")
    assert all("|" in str(v) for v in b)  # 父系|母父系 に結合されている
    # 母父列が無ければ na
    df2 = _df({"父": ["ディープインパクト"]})
    assert (factor_series(df2, "pedigree_2gen") == NA).all()


def test_missing_columns_return_na():
    df = _df({ResultsCols.UMABAN: [1, 2]})
    for f in ("jockey", "trainer", "sire", "race_type", "dist_band"):
        assert (factor_series(df, f) == NA).all()
