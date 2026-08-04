"""能力 Kalman（src/preprocessing/_ability_kalman.py）のユニットテスト。

正規スコア・2x2 Kalman・as-of リーク無し・成長トレンド・疲労 workload・休養で不確実性増・
merger 結合・ライブ snapshot。
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from src.constants._feature_cols import KF_FEATURE_COLS
from src.constants._feature_cols import KF_INIT_LEVEL
from src.preprocessing._ability_kalman import ability_win_probabilities
from src.preprocessing._ability_kalman import compute_ability_kalman_history
from src.preprocessing._ability_kalman import kalman_predict
from src.preprocessing._ability_kalman import kalman_update
from src.preprocessing._ability_kalman import normal_score


# ──────────────────────────────────────────
# normal_score
# ──────────────────────────────────────────


def test_normal_score_winner_high_loser_low():
    n = 10
    assert normal_score(1, n) > 0
    assert normal_score(n, n) < 0
    # 単調減少
    scores = [normal_score(r, n) for r in range(1, n + 1)]
    assert all(scores[i] > scores[i + 1] for i in range(n - 1))


def test_normal_score_symmetry():
    n = 9
    assert normal_score(1, n) == pytest.approx(-normal_score(9, n), abs=1e-9)
    assert normal_score(5, n) == pytest.approx(0.0, abs=1e-9)  # 中央


def test_normal_score_invalid():
    assert math.isnan(normal_score(float("nan"), 10))
    assert math.isnan(normal_score(1, 1))
    assert math.isnan(normal_score(1, 0))


# ──────────────────────────────────────────
# Kalman predict / update
# ──────────────────────────────────────────


def test_kalman_predict_level_advances_by_trend():
    lvl, trd, p00, p01, p11 = kalman_predict(1.0, 0.5, 1.0, 0.0, 0.1, q_level=0.0, q_trend=0.0)
    assert lvl == pytest.approx(1.5)       # level + trend
    assert trd == pytest.approx(0.5 * 0.9)  # ρ=0.9 既定
    assert p00 > 1.0                        # 不確実性は伝播で増える


def test_kalman_predict_variance_grows_with_q():
    _, _, p00_no_q, _, _ = kalman_predict(0, 0, 1.0, 0.0, 0.1, q_level=0.0, q_trend=0.0)
    _, _, p00_q, _, _ = kalman_predict(0, 0, 1.0, 0.0, 0.1, q_level=0.5, q_trend=0.0)
    assert p00_q > p00_no_q


def test_kalman_update_moves_toward_observation():
    # 予測 level=0、観測 y=2 → level は 0 と 2 の間へ
    lvl, trd, p00, p01, p11 = kalman_update(0.0, 0.0, 1.0, 0.0, 0.1, 2.0, r=1.0)
    assert 0.0 < lvl < 2.0
    assert p00 < 1.0  # 観測で不確実性が減る


def test_kalman_update_high_noise_small_move():
    near = kalman_update(0.0, 0.0, 1.0, 0.0, 0.1, 2.0, r=0.01)[0]
    far = kalman_update(0.0, 0.0, 1.0, 0.0, 0.1, 2.0, r=100.0)[0]
    assert near > far  # 観測ノイズが小さいほど観測に強く寄る


# ──────────────────────────────────────────
# ability_win_probabilities
# ──────────────────────────────────────────


def test_ability_win_probabilities_sum_and_order():
    probs = ability_win_probabilities([2.0, 1.0, 0.0])
    assert sum(probs) == pytest.approx(1.0)
    assert probs[0] > probs[1] > probs[2]


def test_ability_win_probabilities_equal_uniform():
    assert ability_win_probabilities([1.0] * 4) == pytest.approx([0.25] * 4)


# ──────────────────────────────────────────
# compute_ability_kalman_history（as-of）
# ──────────────────────────────────────────


def _make_races(rows):
    return pd.DataFrame(rows).set_index("race_id")


def _race(rid, date, entrants, *, n_horses=None, field_mean=None):
    n = n_horses if n_horses is not None else len(entrants)
    out = []
    for umaban, (hid, finish) in enumerate(entrants, start=1):
        row = {"race_id": rid, "date": date, "horse_id": hid, "馬番": umaban,
               "着順": finish, "n_horses": n}
        if field_mean is not None:
            row["ts_field_mean"] = field_mean
        out.append(row)
    return out


def test_history_columns_and_index():
    df = _make_races(_race("R1", "2020-01-01", [("A", 1), ("B", 2), ("C", 3)]))
    feats, snapshot = compute_ability_kalman_history(df)
    assert list(feats.columns) == list(KF_FEATURE_COLS)
    assert feats.index.equals(df.index)
    assert set(snapshot) == {"A", "B", "C"}
    assert set(snapshot["A"]) == {"level", "trend", "var_level", "workload", "n_races", "last_date"}


def test_history_first_race_is_prior():
    df = _make_races(_race("R1", "2020-01-01", [("A", 1), ("B", 2)]))
    feats, _ = compute_ability_kalman_history(df)
    assert feats["kf_level"].to_numpy() == pytest.approx([KF_INIT_LEVEL, KF_INIT_LEVEL])
    assert (feats["kf_workload"] == 0.0).all()
    assert feats["kf_level_vs_field"].abs().max() == pytest.approx(0.0)


def test_history_consistent_winner_level_rises():
    races = [(f"R{i}", f"2020-01-{i:02d}", [("A", 1), ("B", 2), ("C", 3)])
             for i in range(1, 11)]
    rows = []
    for rid, date, entrants in races:
        rows += _race(rid, date, entrants)
    df = _make_races(rows)
    _, snapshot = compute_ability_kalman_history(df)
    assert snapshot["A"]["level"] > snapshot["B"]["level"] > snapshot["C"]["level"]


def test_history_improving_horse_positive_trend():
    """着順がだんだん良くなる馬は成長率 trend が正になる。"""
    # A は 6→5→...→1 着と改善、相手は入れ替え
    rows = []
    for i, a_finish in enumerate(range(6, 0, -1), start=1):
        entrants = [("A", a_finish)] + [
            (f"X{i}_{j}", j if j < a_finish else j + 1) for j in range(1, 6)
        ]
        rows += _race(f"R{i}", f"2020-02-{i:02d}", entrants, n_horses=6)
    df = _make_races(rows)
    _, snapshot = compute_ability_kalman_history(df)
    assert snapshot["A"]["trend"] > 0


def test_history_workload_accumulates_with_frequent_racing():
    """連戦すると workload（疲労指標）が増える。"""
    rows = []
    for i in range(1, 6):
        rows += _race(f"R{i}", f"2020-03-0{i}", [("A", 1), ("B", 2)])  # 毎日出走
    df = _make_races(rows)
    feats, _ = compute_ability_kalman_history(df)
    # 出走前 workload は単調増加（最初は 0、後のレースほど大）
    a_rows = df.reset_index().query("horse_id == 'A'").index
    wl = feats["kf_workload"].to_numpy()[a_rows]
    assert wl[0] == pytest.approx(0.0)
    assert wl[-1] > wl[1]


def test_history_layoff_increases_uncertainty():
    """長期休養明けは予測の不確実性（kf_sigma）が大きい。"""
    # A: 連戦後に長期休養を挟む
    rows = _race("R1", "2020-01-01", [("A", 1), ("B", 2)])
    rows += _race("R2", "2020-01-08", [("A", 1), ("B", 2)])
    rows += _race("R3", "2021-06-01", [("A", 1), ("B", 2)])  # ~500 日後
    df = _make_races(rows)
    feats, _ = compute_ability_kalman_history(df)
    a_rows = df.reset_index().query("horse_id == 'A'").index
    sig = feats["kf_sigma"].to_numpy()[a_rows]
    # 休養明け R3 の不確実性は直前 R2 より大きい
    assert sig[2] > sig[1]


def test_history_as_of_no_leak():
    """当該レースの着順を入れ替えても、そのレースの kf_level は不変。"""
    a = _race("R1", "2020-01-01", [("A", 1), ("B", 2)]) + _race(
        "R2", "2020-01-08", [("A", 1), ("B", 2)]
    )
    b = _race("R1", "2020-01-01", [("A", 2), ("B", 1)]) + _race(
        "R2", "2020-01-08", [("A", 1), ("B", 2)]
    )
    fa, _ = compute_ability_kalman_history(_make_races(a))
    fb, _ = compute_ability_kalman_history(_make_races(b))
    assert np.allclose(fa.loc["R1", "kf_level"].to_numpy(), fb.loc["R1", "kf_level"].to_numpy())
    assert not np.allclose(fa.loc["R2", "kf_level"].to_numpy(), fb.loc["R2", "kf_level"].to_numpy())


def test_history_field_strength_raises_level():
    """強いフィールド（高 ts_field_mean）で勝つ方が能力評価が高くなる。"""
    weak = _make_races(_race("R1", "2020-01-01", [("A", 1), ("B", 2)], field_mean=0.0)
                       + _race("R2", "2020-01-08", [("A", 1), ("B", 2)], field_mean=0.0))
    strong = _make_races(_race("R1", "2020-01-01", [("A", 1), ("B", 2)], field_mean=5.0)
                         + _race("R2", "2020-01-08", [("A", 1), ("B", 2)], field_mean=5.0))
    _, snap_weak = compute_ability_kalman_history(weak)
    _, snap_strong = compute_ability_kalman_history(strong)
    assert snap_strong["A"]["level"] > snap_weak["A"]["level"]
