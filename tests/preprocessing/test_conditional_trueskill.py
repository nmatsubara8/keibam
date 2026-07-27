"""条件別 TrueSkill（src/preprocessing/_conditional_trueskill.py）のユニットテスト。

バケッタ・条件別 as-of ウォーク・リーク無し・条件分離・merger 結合・ライブ snapshot。
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from src.constants._feature_cols import COND_DIMENSIONS
from src.constants._feature_cols import COND_TS_FEATURE_COLS
from src.constants._feature_cols import TS_MU
from src.constants._feature_cols import TS_SIGMA
from src.preprocessing._conditional_trueskill import compute_conditional_trueskill_history
from src.preprocessing._conditional_trueskill import distance_bucket
from src.preprocessing._conditional_trueskill import race_buckets
from src.preprocessing._conditional_trueskill import surface_bucket
from src.preprocessing._trueskill import conservative


# ──────────────────────────────────────────
# バケッタ
# ──────────────────────────────────────────


def test_surface_bucket():
    assert surface_bucket("芝") == "芝"
    assert surface_bucket("ダート") == "ダート"
    assert surface_bucket(None) is None
    assert surface_bucket(float("nan")) is None
    assert surface_bucket("  ") is None


@pytest.mark.parametrize(
    "course_len,expected",
    [
        (12, "sprint"),    # 1200m
        (13.9, "sprint"),
        (14, "mile"),      # 1400m
        (16, "mile"),      # 1600m
        (18, "middle"),    # 1800m
        (20, "middle"),
        (22, "long"),      # 2200m
        (34, "long"),      # 3400m
    ],
)
def test_distance_bucket(course_len, expected):
    assert distance_bucket(course_len) == expected


def test_distance_bucket_invalid():
    assert distance_bucket(None) is None
    assert distance_bucket(float("nan")) is None
    assert distance_bucket("x") is None


def test_race_buckets():
    row = pd.Series({"race_type": "芝", "course_len": 16.0, "around": "右"})
    b = race_buckets(row)
    assert b == {"surface": "芝", "distance": "mile", "around": "右"}


def test_race_buckets_missing_column():
    row = pd.Series({"race_type": "芝"})  # course_len / around 欠落
    b = race_buckets(row)
    assert b["surface"] == "芝"
    assert b["distance"] is None
    assert b["around"] is None


# ──────────────────────────────────────────
# compute_conditional_trueskill_history
# ──────────────────────────────────────────


def _make_cond_races(rows):
    """rows: list of dict(race_id,date,horse_id,馬番,着順,race_type,course_len,around)。"""
    return pd.DataFrame(rows).set_index("race_id")


def _race(rid, date, entrants, *, race_type="芝", course_len=16.0, around="右"):
    out = []
    for umaban, (hid, finish) in enumerate(entrants, start=1):
        out.append({"race_id": rid, "date": date, "horse_id": hid, "馬番": umaban,
                    "着順": finish, "race_type": race_type, "course_len": course_len,
                    "around": around})
    return out


def test_history_columns_and_index():
    rows = _race("R1", "2020-01-01", [("A", 1), ("B", 2), ("C", 3)])
    df = _make_cond_races(rows)
    feats, snapshot = compute_conditional_trueskill_history(df)
    assert list(feats.columns) == list(COND_TS_FEATURE_COLS)
    assert feats.index.equals(df.index)
    assert len(feats) == len(df)


def test_history_first_race_is_prior():
    df = _make_cond_races(_race("R1", "2020-01-01", [("A", 1), ("B", 2)]))
    feats, _ = compute_conditional_trueskill_history(df)
    prior_cons = conservative(TS_MU, TS_SIGMA)
    for dim in COND_DIMENSIONS:
        assert feats[f"ts_{dim}_conservative"].to_numpy() == pytest.approx(
            [prior_cons, prior_cons]
        )
        assert (feats[f"ts_{dim}_n_races"] == 0).all()
        assert feats[f"ts_{dim}_vs_field"].abs().max() == pytest.approx(0.0)


def test_history_condition_separation():
    """芝で勝ち続けてもダートのレーティングは prior のまま（条件分離）。"""
    rows = []
    for r in range(1, 6):
        rows += _race(f"R{r}", f"2020-01-0{r}", [("A", 1), ("B", 2), ("C", 3)],
                      race_type="芝")
    # 最後にダートのレースを 1 つ（A の出走前ダート評価を見る）
    rows += _race("R6", "2020-01-06", [("A", 1), ("B", 2), ("C", 3)], race_type="ダート")
    df = _make_cond_races(rows)
    feats, snapshot = compute_conditional_trueskill_history(df)

    # R6（ダート）の A の surface 条件 conservative は prior（ダート未経験）
    r6 = feats.loc["R6"]
    prior_cons = conservative(TS_MU, TS_SIGMA)
    a_idx = df.loc["R6"].reset_index().query("horse_id == 'A'").index[0]
    assert r6["ts_surface_conservative"].to_numpy()[a_idx] == pytest.approx(prior_cons)
    # snapshot: A は芝で高く、ダートは（1走後で）芝ほど分離していない
    assert snapshot["A"]["surface"]["芝"]["mu"] > TS_MU
    # 芝の n_races は 5、ダートは 1
    assert snapshot["A"]["surface"]["芝"]["n_races"] == 5
    assert snapshot["A"]["surface"]["ダート"]["n_races"] == 1


def test_history_consistent_winner_rises_per_condition():
    rows = []
    for r in range(1, 9):
        rows += _race(f"R{r}", f"2020-01-0{r}", [("A", 1), ("B", 2), ("C", 3)])
    df = _make_cond_races(rows)
    _, snapshot = compute_conditional_trueskill_history(df)
    surf = snapshot["A"]["surface"]["芝"]
    assert surf["mu"] > snapshot["B"]["surface"]["芝"]["mu"]
    assert surf["mu"] > snapshot["C"]["surface"]["芝"]["mu"]


def test_history_as_of_no_leak():
    """当該レースの着順を入れ替えても、そのレースの条件別 conservative は不変。"""
    rows_a = _race("R1", "2020-01-01", [("A", 1), ("B", 2)]) + _race(
        "R2", "2020-01-08", [("A", 1), ("B", 2)]
    )
    rows_b = _race("R1", "2020-01-01", [("A", 2), ("B", 1)]) + _race(
        "R2", "2020-01-08", [("A", 1), ("B", 2)]
    )
    fa, _ = compute_conditional_trueskill_history(_make_cond_races(rows_a))
    fb, _ = compute_conditional_trueskill_history(_make_cond_races(rows_b))
    col = "ts_surface_conservative"
    assert np.allclose(fa.loc["R1", col].to_numpy(), fb.loc["R1", col].to_numpy())
    assert not np.allclose(fa.loc["R2", col].to_numpy(), fb.loc["R2", col].to_numpy())


def test_history_missing_condition_column_is_prior():
    """条件列が無い次元は prior 値で出力され、クラッシュしない。"""
    rows = []
    for r in range(1, 4):
        for u, (h, f) in enumerate([("A", 1), ("B", 2)], start=1):
            rows.append({"race_id": f"R{r}", "date": f"2020-01-0{r}", "horse_id": h,
                         "馬番": u, "着順": f, "race_type": "芝"})  # course_len/around 無し
    df = pd.DataFrame(rows).set_index("race_id")
    feats, _ = compute_conditional_trueskill_history(df)
    prior_cons = conservative(TS_MU, TS_SIGMA)
    # distance / around は全行 prior
    assert np.allclose(feats["ts_distance_conservative"].to_numpy(), prior_cons)
    assert (feats["ts_around_n_races"] == 0).all()
    # surface は更新される
    assert feats["ts_surface_conservative"].nunique() > 1
def test_history_distance_separation():
    """短距離で強い馬の長距離評価は prior のまま。"""
    rows = []
    for r in range(1, 6):
        rows += _race(f"S{r}", f"2020-02-0{r}", [("A", 1), ("B", 2)], course_len=12.0)
    rows += _race("L1", "2020-02-06", [("A", 1), ("B", 2)], course_len=24.0)
    df = _make_cond_races(rows)
    feats, snapshot = compute_conditional_trueskill_history(df)
    assert snapshot["A"]["distance"]["sprint"]["mu"] > TS_MU
    # L1（long）の A の distance conservative は prior
    l1 = feats.loc["L1"]
    a_idx = df.loc["L1"].reset_index().query("horse_id == 'A'").index[0]
    assert l1["ts_distance_conservative"].to_numpy()[a_idx] == pytest.approx(
        conservative(TS_MU, TS_SIGMA)
    )
    assert not math.isnan(l1["ts_distance_conservative"].to_numpy()[a_idx])
