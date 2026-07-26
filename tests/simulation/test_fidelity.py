"""忠実度メトリクスと sim ダイナミクス追跡のテスト。"""
from __future__ import annotations

import numpy as np

from src.simulation._agent_race import field_from_arrays, monte_carlo
from src.simulation._fidelity import (
    pace_backness_signal,
    pace_shape_corr,
    spearman,
)


def test_spearman_basic():
    assert spearman([1, 2, 3, 4], [1, 2, 3, 4]) > 0.99      # 完全単調増
    assert spearman([1, 2, 3, 4], [4, 3, 2, 1]) < -0.99     # 完全単調減
    assert np.isnan(spearman([1, 1, 1], [1, 2, 3]))         # 定数は nan


def test_pace_shape_corr():
    real = [1.0, 2.0, 3.0, 4.0, 5.0]
    sim = [0.5, 1.1, 1.4, 2.0, 2.6]                          # 同順
    assert pace_shape_corr(sim, real) > 0.9


def test_pace_backness_signal_detects_developmental_effect():
    # 仕込み: ハイペース群では追込(backness=1)が前(rank_norm小)、ロー群では無相関。
    rng = np.random.default_rng(0)
    back, rank, pace = [], [], []
    for race in range(60):
        hi = race % 2 == 0
        for _ in range(10):
            b = rng.random()
            if hi:
                r = 1 - b + rng.normal(0, 0.1)     # ハイ: backness大ほど rank小(前)
            else:
                r = rng.random()                    # ロー: 無相関
            back.append(b); rank.append(np.clip(r, 0, 1)); pace.append(1.0 if hi else 0.0)
    sig = pace_backness_signal(back, rank, pace)
    # corr_hi は負（backness大→rank小）、corr_lo≈0 → signal = lo - hi > 0
    assert sig["corr_hi"] < -0.3
    assert sig["signal"] > 0.3


def test_pace_backness_signal_null_when_no_effect():
    rng = np.random.default_rng(1)
    n = 400
    sig = pace_backness_signal(rng.random(n), rng.random(n), rng.random(n))
    assert abs(sig["signal"]) < 0.25                        # 効果なし→signal≈0


def test_monte_carlo_track_dynamics_shapes():
    f = field_from_arrays([1.0, 1.0, 1.0], ["front", "stalker", "closer"], stamina=[1, 1, 1])
    r = monte_carlo(f, n_sim=300, seed=1, track_dynamics=True)
    assert "early_pos_rank" in r and len(r["early_pos_rank"]) == 3
    assert "early_speed" in r and "late_speed" in r
    # 先行(front, idx0)は序盤の位置順位が最小（先頭寄り）、追込(idx2)は最大（後方）
    assert r["early_pos_rank"][0] < r["early_pos_rank"][2]


def test_track_dynamics_closer_starts_back():
    # 追込は序盤後方 → 序盤位置順位が先行より大きい（隊列が機構どおり）
    f = field_from_arrays([1.0] * 4, ["front", "front", "closer", "closer"], stamina=[1] * 4)
    r = monte_carlo(f, n_sim=500, seed=2, track_dynamics=True)
    front_pos = r["early_pos_rank"][:2].mean()
    closer_pos = r["early_pos_rank"][2:].mean()
    assert front_pos < closer_pos
