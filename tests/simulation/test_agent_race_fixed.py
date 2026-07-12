"""固定距離エンジン（time-to-D）＋騎手戦略確率モデルのコアテスト。

決定論性・確率整合に加え、時間箱エンジンが再現できなかった『前傾→差し有利』の符号を固定する。
固定距離では飛ばした先行がスタミナを削って総時間が延び、脚を溜めた差しに差される、が自然に出る。
"""
from __future__ import annotations

import numpy as np

from src.simulation._agent_race import field_from_arrays
from src.simulation._agent_race_fixed import SimConfigFixed, monte_carlo_fixed


def test_probabilities_consistent_and_deterministic():
    field = field_from_arrays([1.0, 1.0, 1.0, 1.0],
                              ["front", "stalker", "closer", "stalker"])
    a = monte_carlo_fixed(field, D=1600, n_sim=400, seed=1)
    assert np.isclose(a["win"].sum(), 1.0)
    assert (a["place"] >= a["win"] - 1e-9).all()
    b = monte_carlo_fixed(field, D=1600, n_sim=400, seed=1)
    assert np.array_equal(a["finish_counts"], b["finish_counts"])       # seed 固定で再現
    c = monte_carlo_fixed(field, D=1600, n_sim=400, seed=2)
    assert not np.array_equal(a["finish_counts"], c["finish_counts"])


def test_higher_ability_wins_more():
    field = field_from_arrays([1.25, 1.0, 0.8], ["stalker", "stalker", "stalker"],
                              stamina=[1.0, 1.0, 1.0], noise=[0.02, 0.02, 0.02])
    r = monte_carlo_fixed(field, D=1600, n_sim=1500, seed=3, ability_sigma=0.15)
    assert r["mean_rank"][0] < r["mean_rank"][1] < r["mean_rank"][2]


def test_front_pace_favors_closers_SIGN():
    # ★本命: 逃げ2頭+差し2頭・同能力。pace_intensity(場の積極性)を上げる=前傾にすると、
    # 固定距離では飛ばした逃げが失速し、脚を溜めた差しの勝率が上がるはず（時間箱では逆だった）。
    field = field_from_arrays(
        ability=[1.0, 1.0, 1.0, 1.0],
        style_names=["front", "front", "closer", "closer"],
        stamina=[1.0, 1.0, 1.0, 1.0], noise=[0.02, 0.02, 0.02, 0.02],
    )
    cfg = SimConfigFixed()
    slow = monte_carlo_fixed(field, D=1600, n_sim=3000, seed=11, cfg=cfg, pace_intensity=0.80)
    fast = monte_carlo_fixed(field, D=1600, n_sim=3000, seed=11, cfg=cfg, pace_intensity=1.25)
    closer_slow = slow["win"][2] + slow["win"][3]
    closer_fast = fast["win"][2] + fast["win"][3]
    # 前傾(fast)の方が差し勢の勝率が高い＝『前傾→差し有利』を符号として再現
    assert closer_fast > closer_slow, f"closer slow={closer_slow:.3f} fast={closer_fast:.3f}"
    # 逆に逃げ勢はスローの方が残る
    assert (slow["win"][0] + slow["win"][1]) > (fast["win"][0] + fast["win"][1])


def test_pace_intensity_raises_early_pace_shape():
    field = field_from_arrays([1.0, 1.0, 1.0, 1.0],
                              ["front", "front", "closer", "closer"])

    def _shape(it):
        r = monte_carlo_fixed(field, D=1600, n_sim=1500, seed=7,
                              pace_intensity=it, track_dynamics=True)
        e, l = r["early_speed"], r["late_speed"]
        return (e - l) / (e + l + 1e-9)

    assert _shape(1.25) > _shape(0.80)      # 高積極性ほど前傾（序盤が速い）
