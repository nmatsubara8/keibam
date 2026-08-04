"""TrueSkill（src/preprocessing/_trueskill.py）のユニットテスト。

切断ガウス補正（v/w）・2 人/多頭順位の更新・as-of リーク無し・shuffle 検査・
merger 結合・ライブ snapshot を扱う。
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from src.constants._feature_cols import TS_BETA
from src.constants._feature_cols import TS_FEATURE_COLS
from src.constants._feature_cols import TS_MU
from src.constants._feature_cols import TS_SIGMA
from src.preprocessing._trueskill import _cdf
from src.preprocessing._trueskill import _pdf
from src.preprocessing._trueskill import compute_trueskill_history
from src.preprocessing._trueskill import conservative
from src.preprocessing._trueskill import trueskill_win_probabilities
from src.preprocessing._trueskill import update_ranking
from src.preprocessing._trueskill import v_win
from src.preprocessing._trueskill import w_win


# ──────────────────────────────────────────
# 正規分布ヘルパ
# ──────────────────────────────────────────


def test_pdf_cdf_basic():
    assert _pdf(0.0) == pytest.approx(1.0 / math.sqrt(2 * math.pi))
    assert _cdf(0.0) == pytest.approx(0.5)
    assert _cdf(-3.0) + _cdf(3.0) == pytest.approx(1.0)


# ──────────────────────────────────────────
# v/w 補正関数
# ──────────────────────────────────────────


def test_v_win_positive_and_decreasing():
    """v(t) > 0 で、t が大きい（楽勝）ほど補正は小さい。"""
    assert v_win(0.0, 0.0) > 0
    assert v_win(-2.0, 0.0) > v_win(0.0, 0.0) > v_win(2.0, 0.0)


def test_w_win_in_unit_interval():
    for t in (-3.0, -1.0, 0.0, 1.0, 3.0):
        w = w_win(t, 0.0)
        assert 0.0 <= w <= 1.0


def test_v_win_extreme_upset_no_blowup():
    """大番狂わせ（t が極端に負）でも v が発散しない。"""
    v = v_win(-40.0, 0.0)
    assert math.isfinite(v)
    assert v == pytest.approx(40.0, rel=0.05)


# ──────────────────────────────────────────
# update_ranking（2 人）
# ──────────────────────────────────────────


def test_update_ranking_winner_mu_up_loser_down():
    mus, sigmas = update_ranking([TS_MU, TS_MU], [TS_SIGMA, TS_SIGMA], [1, 2])
    assert mus[0] > TS_MU
    assert mus[1] < TS_MU
    # 対称な初期値なら μ 変化は対称
    assert (mus[0] - TS_MU) == pytest.approx(TS_MU - mus[1], rel=1e-6)


def test_update_ranking_sigma_decreases():
    """対戦すると不確かさ σ は（情報が増えるので）減少する。"""
    _, sigmas = update_ranking([TS_MU, TS_MU], [TS_SIGMA, TS_SIGMA], [1, 2])
    assert sigmas[0] < TS_SIGMA
    assert sigmas[1] < TS_SIGMA


def test_update_ranking_upset_moves_more():
    """格下が勝つと μ 変化が大きい。"""
    upset, _ = update_ranking([20.0, 30.0], [TS_SIGMA, TS_SIGMA], [1, 2])
    expected, _ = update_ranking([30.0, 20.0], [TS_SIGMA, TS_SIGMA], [1, 2])
    assert (upset[0] - 20.0) > (expected[0] - 30.0)


def test_update_ranking_draw_symmetric_no_mu_change():
    """同レーティングの同着は μ を動かさない（σ は減る）。"""
    mus, sigmas = update_ranking([TS_MU, TS_MU], [TS_SIGMA, TS_SIGMA], [1, 1])
    assert mus[0] == pytest.approx(mus[1])
    assert mus[0] == pytest.approx(TS_MU, abs=1e-9)
    assert sigmas[0] < TS_SIGMA


def test_update_ranking_draw_pulls_together():
    """異なる μ の同着は両者を引き寄せる。"""
    mus, _ = update_ranking([30.0, 20.0], [TS_SIGMA, TS_SIGMA], [1, 1])
    assert mus[0] < 30.0  # 上位は下がる
    assert mus[1] > 20.0  # 下位は上がる


def test_update_ranking_single_horse_noop():
    mus, sigmas = update_ranking([TS_MU], [TS_SIGMA], [1])
    assert mus == [TS_MU]
    assert sigmas == [TS_SIGMA]


# ──────────────────────────────────────────
# update_ranking（多頭・順序非依存）
# ──────────────────────────────────────────


def test_update_ranking_multi_preserves_order():
    """3 頭で μ の順位が結果順位と一致する。"""
    mus, _ = update_ranking([TS_MU] * 3, [TS_SIGMA] * 3, [1, 2, 3])
    assert mus[0] > mus[1] > mus[2]


def test_update_ranking_entrant_order_invariance():
    """entrant の並び順を変えても結果（馬ごとの更新）は不変。"""
    base_mu, base_sigma, finish = [24.0, 25.0, 26.0], [8.0, 8.3, 7.5], [2, 1, 3]
    m1, s1 = update_ranking(base_mu, base_sigma, finish)

    perm = [2, 0, 1]
    pm = [base_mu[i] for i in perm]
    ps = [base_sigma[i] for i in perm]
    pf = [finish[i] for i in perm]
    m2, s2 = update_ranking(pm, ps, pf)

    # perm を戻して比較
    inv = [perm.index(i) for i in range(3)]
    assert np.allclose([m2[i] for i in inv], m1, atol=1e-9)
    assert np.allclose([s2[i] for i in inv], s1, atol=1e-9)


def test_update_ranking_middle_horse_gains_certainty():
    """中位馬は上位・下位の 2 比較に参加し σ が減る。

    全頭同レーティングだと中位馬の勝ち（下位に）と負け（上位に）が相殺し μ は
    変わらない（隣接ペア分解の正しい性質）。両端のみ μ が動く。
    """
    mus, sigmas = update_ranking([TS_MU] * 4, [TS_SIGMA] * 4, [1, 2, 3, 4])
    assert sigmas[1] < TS_SIGMA           # 中位も情報を得て σ が減る
    assert mus[0] > mus[1]                # 1 着は上昇
    assert mus[1] == pytest.approx(mus[2])  # 中位 2 頭は相殺で不変
    assert mus[1] == pytest.approx(TS_MU)
    assert mus[2] > mus[3]                # 4 着は下降


# ──────────────────────────────────────────
# conservative / win prob
# ──────────────────────────────────────────


def test_conservative():
    assert conservative(25.0, 8.0, k=3.0) == pytest.approx(1.0)


def test_trueskill_win_probabilities_sum_and_order():
    probs = trueskill_win_probabilities([30.0, 25.0, 20.0], [TS_SIGMA] * 3, beta=TS_BETA)
    assert sum(probs) == pytest.approx(1.0)
    assert probs[0] > probs[1] > probs[2]


def test_trueskill_win_probabilities_equal_uniform():
    probs = trueskill_win_probabilities([25.0] * 4, [TS_SIGMA] * 4)
    assert probs == pytest.approx([0.25] * 4)


# ──────────────────────────────────────────
# compute_trueskill_history（as-of リーク無し）
# ──────────────────────────────────────────


def _make_races(finish_by_race):
    rows = []
    for race_id, date, entrants in finish_by_race:
        for umaban, (hid, finish) in enumerate(entrants, start=1):
            rows.append({"race_id": race_id, "date": date, "horse_id": hid,
                         "馬番": umaban, "着順": finish})
    return pd.DataFrame(rows).set_index("race_id")


def test_compute_trueskill_history_columns_and_index():
    df = _make_races([
        ("R1", "2020-01-01", [("A", 1), ("B", 2), ("C", 3)]),
        ("R2", "2020-01-08", [("A", 1), ("B", 2), ("C", 3)]),
    ])
    feats, snapshot = compute_trueskill_history(df)
    assert list(feats.columns) == list(TS_FEATURE_COLS)
    assert feats.index.equals(df.index)
    assert len(feats) == len(df)
    assert set(snapshot) == {"A", "B", "C"}
    assert set(snapshot["A"]) == {"mu", "sigma", "n_races", "last_date"}


def test_compute_trueskill_history_first_race_is_initial():
    df = _make_races([("R1", "2020-01-01", [("A", 1), ("B", 2)])])
    feats, _ = compute_trueskill_history(df)
    assert feats["ts_mu"].to_numpy() == pytest.approx([TS_MU, TS_MU])
    assert feats["ts_sigma"].to_numpy() == pytest.approx([TS_SIGMA, TS_SIGMA])
    assert (feats["ts_n_races"] == 0).all()
    assert feats["ts_vs_field"].abs().max() == pytest.approx(0.0)


def test_compute_trueskill_history_consistent_winner_rises():
    races = [(f"R{i}", f"2020-01-{i:02d}", [("A", 1), ("B", 2), ("C", 3)])
             for i in range(1, 11)]
    df = _make_races(races)
    _, snapshot = compute_trueskill_history(df)
    assert snapshot["A"]["mu"] > snapshot["B"]["mu"] > snapshot["C"]["mu"]
    # σ は対戦を重ねると初期値より縮む
    assert snapshot["A"]["sigma"] < TS_SIGMA
    assert snapshot["A"]["n_races"] == 10


def test_compute_trueskill_history_as_of_no_leak():
    """当該レースの着順を入れ替えても、そのレースで出力される ts_mu は不変。"""
    races_a = [
        ("R1", "2020-01-01", [("A", 1), ("B", 2)]),
        ("R2", "2020-01-08", [("A", 1), ("B", 2)]),
    ]
    races_b = [
        ("R1", "2020-01-01", [("A", 2), ("B", 1)]),
        ("R2", "2020-01-08", [("A", 1), ("B", 2)]),
    ]
    feats_a, _ = compute_trueskill_history(_make_races(races_a))
    feats_b, _ = compute_trueskill_history(_make_races(races_b))
    # R1 の as-of μ は当該結果に依存しない
    assert np.allclose(
        feats_a.loc["R1", "ts_mu"].to_numpy(), feats_b.loc["R1", "ts_mu"].to_numpy()
    )
    # R2 は R1 の結果に依存するため変わる
    assert not np.allclose(
        feats_a.loc["R2", "ts_mu"].to_numpy(), feats_b.loc["R2", "ts_mu"].to_numpy()
    )


def test_compute_trueskill_history_shuffle_destroys_signal():
    """着順をレース横断でシャッフルすると μ の分散が縮む（リーク検査の代理）。"""
    rng = np.random.default_rng(7)
    horses = [f"H{i}" for i in range(8)]
    structured_rows, shuffled_rows = [], []
    for r in range(60):
        date = f"2020-{1 + r // 28:02d}-{1 + r % 28:02d}"
        rid = f"R{r}"
        for umaban, hid in enumerate(horses, start=1):
            structured_rows.append({"race_id": rid, "date": date, "horse_id": hid,
                                    "馬番": umaban, "着順": horses.index(hid) + 1})
        order = rng.permutation(len(horses)) + 1
        for umaban, (hid, finish) in enumerate(zip(horses, order, strict=True), start=1):
            shuffled_rows.append({"race_id": rid, "date": date, "horse_id": hid,
                                  "馬番": umaban, "着順": int(finish)})
    _, snap_s = compute_trueskill_history(pd.DataFrame(structured_rows).set_index("race_id"))
    _, snap_x = compute_trueskill_history(pd.DataFrame(shuffled_rows).set_index("race_id"))
    spread_s = np.std([v["mu"] for v in snap_s.values()])
    spread_x = np.std([v["mu"] for v in snap_x.values()])
    assert spread_s > spread_x * 3
