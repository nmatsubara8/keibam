"""Model 1（ManjiScorer / 卍式因子）の単体テスト。合成フィクスチャで決定的に検証。"""
from __future__ import annotations

import pandas as pd
import pytest

from src.policies._manji_factors import NA, FACTORS, buckets
from src.policies._manji_scorer import ManjiScorer, ManjiScorerConfig


def _race(race_id, rows):
    """rows: list of dict → index=race_id の DataFrame。"""
    return pd.DataFrame(rows, index=[race_id] * len(rows))


@pytest.fixture
def two_races():
    # R1: 4頭, R2: 3頭。性齢/馬体重/斤量/馬番/単勝/interval/dist_change/race_type/date を持つ。
    r1 = _race("R1", [
        {"馬番": 1, "性齢": "牡3", "馬体重": "500(+2)", "斤量": 57.0, "単勝": 2.5,
         "interval": 14, "dist_change": 0, "race_type": "芝", "date": "2021-07-15"},
        {"馬番": 2, "性齢": "牝4", "馬体重": "440(-4)", "斤量": 54.0, "単勝": 8.0,
         "interval": 7, "dist_change": -200, "race_type": "芝", "date": "2021-07-15"},
        {"馬番": 3, "性齢": "セ5", "馬体重": "480(0)", "斤量": 56.0, "単勝": 20.0,
         "interval": 200, "dist_change": 200, "race_type": "芝", "date": "2021-07-15"},
        {"馬番": 4, "性齢": "牝3", "馬体重": "460(+6)", "斤量": 54.0, "単勝": 60.0,
         "interval": 21, "dist_change": 0, "race_type": "芝", "date": "2021-07-15"},
    ])
    r2 = _race("R2", [
        {"馬番": 5, "性齢": "牡6", "馬体重": "計不", "斤量": 58.0, "単勝": 3.0,
         "interval": 30, "dist_change": 0, "race_type": "ダ", "date": "2021-01-10"},
        {"馬番": 6, "性齢": "牝4", "馬体重": "420(0)", "斤量": 55.0, "単勝": 5.0,
         "interval": 14, "dist_change": -100, "race_type": "ダ", "date": "2021-01-10"},
        {"馬番": 7, "性齢": "牡4", "馬体重": "510(+8)", "斤量": 57.0, "単勝": 40.0,
         "interval": 8, "dist_change": 400, "race_type": "ダ", "date": "2021-01-10"},
    ])
    return pd.concat([r1, r2])


def test_all_factors_return_aligned_labels(two_races):
    bk = buckets(two_races)
    assert list(bk.index) == list(two_races.index)
    assert set(bk.columns) == set(FACTORS)
    # 欠損（馬体重「計不」）は na に落ちる
    assert bk.loc["R2", "weight_rank"].iloc[0] == NA


def test_parity_sex_season(two_races):
    bk = buckets(two_races, ["umaban_parity", "sex", "season", "season_sex"])
    assert bk["umaban_parity"].tolist() == ["odd", "even", "odd", "even", "odd", "even", "odd"]
    assert bk["sex"].tolist() == ["牡", "牝", "セ", "牝", "牡", "牝", "牡"]
    # R1=7月→summer, R2=1月→winter
    assert bk.loc["R1", "season"].unique().tolist() == ["summer"]
    assert bk.loc["R2", "season"].unique().tolist() == ["winter"]
    assert bk["season_sex"].iloc[0] == "summer_牡"


def test_rotation_and_dist_change(two_races):
    bk = buckets(two_races, ["rotation", "dist_change"])
    # interval: 14→naka1_3, 7→rentai, 200→kyuyoake, 21→naka1_3
    assert bk.loc["R1", "rotation"].tolist() == ["naka1_3", "rentai", "kyuyoake", "naka1_3"]
    assert bk.loc["R1", "dist_change"].tolist() == ["same", "short", "extend", "same"]


def test_weight_rank_within_race(two_races):
    bk = buckets(two_races, ["weight_rank"])
    # R1 体重 500/440/480/460 → 500=heavy, 440=light
    r1 = bk.loc["R1", "weight_rank"].tolist()
    assert r1[0] == "heavy" and r1[1] == "light"


def test_missing_column_is_na():
    df = pd.DataFrame({"馬番": [1, 2]}, index=["R", "R"])
    bk = buckets(df)  # ほとんどの因子は列不在 → na
    assert (bk["sex"] == NA).all()
    assert bk["umaban_parity"].tolist() == ["odd", "even"]


def test_score_is_weighted_sum(two_races):
    cfg = ManjiScorerConfig(
        points={
            "umaban_parity": {"odd": 1.0, "even": -1.0},
            "sex": {"牝": 2.0},  # 牝に+2、他は0
        },
        weights={"sex": 0.5},
    )
    sc = ManjiScorer(cfg).score(two_races)
    # 行1: odd(+1) + 牡(0) = 1.0 ; 行2: even(-1) + 0.5*牝(2)= -1+1 = 0.0
    assert sc.iloc[0] == pytest.approx(1.0)
    assert sc.iloc[1] == pytest.approx(0.0)


def test_select_applies_zone_and_topk(two_races):
    # 全馬 odd→+1/even→0 の単純得点。ゾーン odds[3,50], top_k=2。
    cfg = ManjiScorerConfig(
        points={"umaban_parity": {"odd": 1.0, "even": 0.0}},
        zone_odds=(3.0, 50.0),
        top_k=2,
    )
    chosen = ManjiScorer(cfg).select(two_races)
    # odds<3 (2.5) と odds>50 (60) は除外。R1で残るのは 8.0(uma2),20.0(uma3)。
    r1 = chosen[chosen["race_id"] == "R1"]
    assert set(r1["umaban"]) <= {2, 3}
    assert (chosen["odds"] >= 3.0).all() and (chosen["odds"] <= 50.0).all()


def test_race_min_score_filters_whole_race(two_races):
    cfg = ManjiScorerConfig(
        points={"umaban_parity": {"odd": 1.0, "even": 0.0}},
        zone_odds=(1.0, 100.0),
        race_min_score=5.0,  # どのレースも最高得点1.0 < 5.0 → 全除外
    )
    chosen = ManjiScorer(cfg).select(two_races)
    assert chosen.empty


def test_stake_formula_and_min_stake():
    cfg = ManjiScorerConfig(points={}, sizing_kappa=0.08, min_stake=100.0)
    sc = ManjiScorer(cfg)
    # 残高10万, odds5 → 100000*0.08/5 = 1600
    assert sc.stake(100_000, 5.0) == pytest.approx(1600.0)
    # 大穴 odds100 → 100000*0.08/100 = 80 < 100 → 不買(0)
    assert sc.stake(100_000, 100.0) == 0.0
    assert sc.stake(100_000, 0.0) == 0.0
