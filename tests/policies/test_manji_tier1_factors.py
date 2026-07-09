"""Tier-1 追加因子（人気/枠順/馬体重/増減/斤量比/馬齢×ローテ/距離×馬齢/前走着順）の単体テスト。"""
from __future__ import annotations

import pandas as pd

from src.policies._manji_factors import NA, FACTORS, buckets


def _race(rows):
    return pd.DataFrame(rows, index=["R1"] * len(rows))


def test_popularity_from_ninki_column():
    df = _race([
        {"人気": 1, "単勝": 2.0}, {"人気": 3, "単勝": 6.0},
        {"人気": 5, "単勝": 12.0}, {"人気": 10, "単勝": 80.0},
    ])
    b = buckets(df, ["popularity"])["popularity"].tolist()
    assert b == ["fav1", "fav2_3", "mid4_8", "long9plus"]


def test_popularity_falls_back_to_odds_rank():
    df = _race([{"単勝": 2.0}, {"単勝": 5.0}, {"単勝": 9.0}, {"単勝": 20.0}, {"単勝": 60.0}])
    b = buckets(df, ["popularity"])["popularity"].tolist()
    # 単勝順位 1..5 → fav1, fav2_3, fav2_3, mid4_8, mid4_8
    assert b == ["fav1", "fav2_3", "fav2_3", "mid4_8", "mid4_8"]


def test_waku_by_track():
    df = _race([
        {"枠番": 1, "race_type": "芝"}, {"枠番": 4, "race_type": "芝"},
        {"枠番": 8, "race_type": "ダ"},
    ])
    b = buckets(df, ["waku"])["waku"].tolist()
    assert b == ["芝_inner", "芝_mid", "ダ_outer"]


def test_body_weight_and_diff_from_paren():
    df = _race([
        {"馬体重": "430(-6)"}, {"馬体重": "480(+2)"}, {"馬体重": "520(+20)"},
    ])
    bw = buckets(df, ["body_weight"])["body_weight"].tolist()
    wd = buckets(df, ["weight_diff"])["weight_diff"].tolist()
    assert bw == ["u440", "470_500", "o500"]
    assert wd == ["minus", "flat", "big_plus"]


def test_kinryo_per_weight_rank():
    df = _race([
        {"斤量": 57.0, "馬体重": "440(0)"},   # 高負担
        {"斤量": 54.0, "馬体重": "500(0)"},   # 低負担
        {"斤量": 55.0, "馬体重": "470(0)"},
    ])
    b = buckets(df, ["kinryo_per_weight"])["kinryo_per_weight"].tolist()
    assert b[0] == "heavy" and b[1] == "light"


def test_age_rotation_interaction():
    df = _race([
        {"性齢": "牡3", "interval": 200},  # 若馬×長期明け
        {"性齢": "牡6", "interval": 200},  # 古馬×長期明け
        {"性齢": "牡4", "interval": 14},   # 通常
    ])
    b = buckets(df, ["age_rotation"])["age_rotation"].tolist()
    assert b == ["young_layoff", "old_layoff", "other"]


def test_dist_age_interaction():
    df = _race([
        {"性齢": "牡2", "dist_change": -200},  # 若馬×短縮
        {"性齢": "牡5", "dist_change": 200},   # 古馬×延長
    ])
    b = buckets(df, ["dist_age"])["dist_age"].tolist()
    assert b == ["young_short", "old_extend"]


def test_prev_finish_tolerant():
    # 前走着順列がある場合
    df = _race([{"前走着順": 6}, {"前走着順": 1}, {"前走着順": 12}])
    b = buckets(df, ["prev_finish"])["prev_finish"].tolist()
    assert b == ["p6", "p1", "p11plus"]
    # 無い場合は na
    df2 = _race([{"馬番": 1}])
    assert buckets(df2, ["prev_finish"])["prev_finish"].iloc[0] == NA


def test_all_new_factors_registered():
    for name in ("popularity", "waku", "body_weight", "weight_diff",
                 "kinryo_per_weight", "age_rotation", "dist_age", "prev_finish"):
        assert name in FACTORS
