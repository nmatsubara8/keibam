"""app._rating_eval（レーティング効果検証ヘルパ）の純粋ロジック検証。"""

from __future__ import annotations

import pandas as pd

from app import _rating_eval as RE


def _featured():
    # 2レース×2頭。R1: h1(Elo高)が1着, R2: h3(Elo高)が1着 → Elo本命的中=2/2。
    rows = [
        {"race_id": "R1", "馬番": 1, "elo_rating": 1600.0, "elo_win_prob": 0.7,
         "rank_win": 1, "着順": 1, "単勝": 2.0},
        {"race_id": "R1", "馬番": 2, "elo_rating": 1400.0, "elo_win_prob": 0.3,
         "rank_win": 0, "着順": 2, "単勝": 5.0},
        {"race_id": "R2", "馬番": 1, "elo_rating": 1550.0, "elo_win_prob": 0.6,
         "rank_win": 1, "着順": 1, "単勝": 3.0},
        {"race_id": "R2", "馬番": 2, "elo_rating": 1450.0, "elo_win_prob": 0.4,
         "rank_win": 0, "着順": 2, "単勝": 2.5},
    ]
    return pd.DataFrame(rows).set_index("race_id")


def test_has_ratings():
    assert RE.has_ratings(_featured())
    assert not RE.has_ratings(pd.DataFrame({"x": [1]}))


def test_top_pick_hit_rates():
    hits = RE.top_pick_hit_rates(_featured())
    assert hits["n_races"] == 2
    assert hits["elo_hit"] == 2          # Elo最上位が両レースで1着
    assert hits["elo_rate"] == 1.0
    # 市場本命（単勝最小）: R1=h1(2.0)勝ち, R2=h2(2.5)負け → 1/2
    assert hits["fav_hit"] == 1


def test_rank_correlation_negative_when_rating_predicts():
    rho = RE.rank_correlation(_featured())
    assert rho < 0  # 高レーティングほど着順が小さい（好走）


def test_standalone_calibration_columns():
    calib = RE.standalone_calibration(_featured(), n_bins=4)
    assert list(calib.columns) == ["bin_mid", "mean_pred", "mean_actual", "count"]
    assert calib["count"].sum() == 4


def test_snapshot_ranking_min_races_filter():
    snap = {"h1": {"rating": 1700.0, "n_races": 5}, "h2": {"rating": 1600.0, "n_races": 1}}
    out = RE.snapshot_ranking(snap, top=10, min_races=2)
    assert list(out["horse_id"]) == ["h1"]   # h2 は出走数不足で除外


def test_empty_inputs_safe():
    assert RE.snapshot_ranking({}, top=5).empty
    assert RE.standalone_calibration(pd.DataFrame()).empty
    assert RE.rank_correlation(pd.DataFrame()) != RE.rank_correlation(pd.DataFrame())  # NaN
