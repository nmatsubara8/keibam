"""Phase1.5 2次元エンジンの機構テスト（隊列・発走速度・2次元位置取り）。"""
from __future__ import annotations

import numpy as np

from src.simulation._agent_race import field_from_arrays
from src.simulation._agent_race_2d import SimConfig2D, monte_carlo_2d


def test_probabilities_consistent():
    f = field_from_arrays([1.0, 1.0, 1.0, 1.0], ["front", "stalker", "closer", "stalker"])
    r = monte_carlo_2d(f, n_sim=400, seed=1)
    assert np.isclose(r["win"].sum(), 1.0)
    assert (r["place"] >= r["win"] - 1e-9).all()


def test_deterministic_with_seed():
    f = field_from_arrays([1.0, 0.9, 1.1], ["front", "stalker", "closer"])
    a = monte_carlo_2d(f, n_sim=200, seed=5)
    b = monte_carlo_2d(f, n_sim=200, seed=5)
    assert np.array_equal(a["finish_counts"], b["finish_counts"])


def test_break_speed_no_accel_from_zero():
    # 発走速度>0: 序盤巡航速度が正で、加速相を除いた early は late と同オーダー（0発進の相ではない）
    f = field_from_arrays([1.0] * 5, ["front", "stalker", "stalker", "closer", "closer"])
    r = monte_carlo_2d(f, n_sim=300, seed=2, track_dynamics=True)
    assert r["early_speed"] > 0.3
    assert r["late_speed"] > 0.0


def test_layout_front_settles_forward():
    # 位置ターゲット制御: 先行は序盤前・追込は序盤後方（隊列の再現）
    f = field_from_arrays([1.0] * 6, ["front", "front", "stalker", "stalker", "closer", "closer"])
    r = monte_carlo_2d(f, n_sim=500, seed=3, track_dynamics=True)
    front = r["early_pos_rank"][:2].mean()
    closer = r["early_pos_rank"][4:].mean()
    assert front < closer - 0.5      # 先行が明確に前


def test_style_position_correlation_positive():
    # 脚質コード(0前..2後) と 序盤位置 が正相関（1D より強い隊列再現を狙う）
    from src.simulation._fidelity import spearman
    styles = ["front"] * 3 + ["stalker"] * 3 + ["closer"] * 3
    f = field_from_arrays([1.0] * 9, styles)
    r = monte_carlo_2d(f, n_sim=400, seed=4, track_dynamics=True)
    c = spearman(f.style, r["early_pos_rank"])
    assert c > 0.5                   # 先行=前・追込=後ろ を明確に再現


def test_swing_spreads_lateral_positions():
    # 2次元: 詰まりで外に出す機構が働き、着順分布が退化しない（複数馬が勝ちうる）
    f = field_from_arrays([1.0] * 8, ["front"] * 8)   # 全員先行→前が詰まる
    r = monte_carlo_2d(f, n_sim=400, seed=6, cfg=SimConfig2D(stamina_cost=0.02))
    # 全員同条件でも勝ちが1頭に潰れない（横に散って複数が勝ちうる）
    assert (r["win"] > 0.02).sum() >= 4
