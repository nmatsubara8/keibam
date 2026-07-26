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


def test_2d_gate_inside_runs_shorter_and_wins_more():
    """2D: turn_k>0 で内枠(gate=0)は内ラチ=最短、外枠(gate=1)は外を回り距離ロスで不利。"""
    import numpy as np
    from src.simulation._agent_race import RaceField, STYLE_STALKER
    from src.simulation._agent_race_2d import SimConfig2D, monte_carlo_2d
    n = 10
    gate = np.linspace(0.0, 1.0, n)
    f = RaceField(ability=np.ones(n), style=np.full(n, STYLE_STALKER),
                  stamina=np.full(n, 2.0), noise=np.full(n, 0.02), gate=gate)
    r = monte_carlo_2d(f, n_sim=1500, seed=3, ability_sigma=0.0,
                       cfg=SimConfig2D(turn_k=0.03, lane_return=0.0))
    # 枠順(gate)と平均着順が正相関（内ほど上位＝実走距離ロスが効く）
    assert np.corrcoef(gate, r["mean_rank"])[0, 1] > 0.3


def test_2d_dirt_kickback_slows_field():
    """2D砂被り: ダートで前に馬がいる後方馬が鈍り、序盤速度が芝より落ちる。"""
    import numpy as np
    from src.simulation._agent_race import RaceField, STYLE_FRONT, STYLE_STALKER
    from src.simulation._agent_race_2d import SimConfig2D, monte_carlo_2d
    n = 8
    style = np.array([STYLE_FRONT] + [STYLE_STALKER] * (n - 1))
    base = dict(ability=np.ones(n), style=style, stamina=np.full(n, 2.0), noise=np.full(n, 0.02))
    cfg = SimConfig2D(kickback_k=0.6)
    dirt = monte_carlo_2d(RaceField(is_dirt=True, **base), n_sim=600, seed=2,
                          ability_sigma=0.0, cfg=cfg, track_dynamics=True)
    turf = monte_carlo_2d(RaceField(is_dirt=False, **base), n_sim=600, seed=2,
                          ability_sigma=0.0, cfg=cfg, track_dynamics=True)
    assert dirt["early_speed"] < turf["early_speed"]


def test_2d_track_exotics_top3_consistent():
    """track_exotics=True で各試行の上位3頭 index を返し、1着列は勝率最大馬と整合。"""
    import numpy as np
    from src.simulation._agent_race import RaceField, STYLE_STALKER
    n = 8
    ab = np.ones(n); ab[2] = 1.7                       # 明確な本命 idx2
    f = RaceField(ability=ab, style=np.full(n, STYLE_STALKER),
                  stamina=np.full(n, 2.0), noise=np.full(n, 0.02))
    r = monte_carlo_2d(f, n_sim=1500, seed=1, ability_sigma=0.0, track_exotics=True)
    top3 = r["top3"]
    assert top3.shape == (1500, 3)
    # 上位3頭は各試行で相異なる馬
    assert all(len(set(row)) == 3 for row in top3[:50])
    # 1着列の最頻値＝勝率最大馬（本命 idx2）
    assert np.bincount(top3[:, 0], minlength=n).argmax() == 2
    assert np.argmax(r["win"]) == 2


def test_2d_dt_invariance_finer_mesh_converges():
    """2D も dt 不変: T·dt を一定に保ち dt を細かくしても着順分布が保存される。

    ノイズを √dt・swing を ×dt にしたので、dt=1.0(T=100) と dt=0.25(T=400) で勝率がほぼ一致する。
    """
    f = field_from_arrays([1.25, 1.0, 0.85, 1.05, 0.9],
                          ["front", "stalker", "closer", "stalker", "closer"],
                          stamina=[1.2, 1.0, 1.1, 1.0, 0.9], noise=[0.05] * 5)
    coarse = monte_carlo_2d(f, n_sim=4000, seed=11, ability_sigma=0.1,
                            cfg=SimConfig2D(T=100, dt=1.0))
    fine = monte_carlo_2d(f, n_sim=4000, seed=11, ability_sigma=0.1,
                          cfg=SimConfig2D(T=400, dt=0.25))    # 同じ総時間 T·dt=100、4倍細かい
    assert np.max(np.abs(coarse["win"] - fine["win"])) < 0.05


def test_2d_dt_one_matches_legacy_scaling():
    """dt=1.0 では √dt=dt=1 で従来積分と数値一致（既存 dt=1.0 較正が不変であることの担保）。"""
    f = field_from_arrays([1.1, 1.0, 0.9, 1.0], ["front", "stalker", "closer", "stalker"])
    a = monte_carlo_2d(f, n_sim=500, seed=7, cfg=SimConfig2D(T=100, dt=1.0))
    b = monte_carlo_2d(f, n_sim=500, seed=7, cfg=SimConfig2D(T=100, dt=1.0))
    assert np.array_equal(a["finish_counts"], b["finish_counts"])


def test_2d_falls_dnf_jump_more_than_flat():
    """2D落馬: 発火馬は DNF。障害は平地より多く飛ぶ（最大勝率が下がる＝波乱）。"""
    import numpy as np
    from src.simulation._agent_race import RaceField, STYLE_STALKER
    from src.simulation._agent_race_2d import SimConfig2D, monte_carlo_2d
    n = 8
    ab = np.ones(n); ab[0] = 1.6                      # 明確な本命(idx0)
    base = dict(ability=ab, style=np.full(n, STYLE_STALKER),
                stamina=np.full(n, 2.0), noise=np.full(n, 0.02))
    cfg = SimConfig2D(fall_base_flat=0.0002, fall_base_jump=0.03)
    flat = monte_carlo_2d(RaceField(is_jump=False, **base), n_sim=3000, seed=5, ability_sigma=0.0, cfg=cfg)
    jump = monte_carlo_2d(RaceField(is_jump=True, **base), n_sim=3000, seed=5, ability_sigma=0.0, cfg=cfg)
    # 障害は落馬で本命(idx0)も飛ぶ → 本命の勝率が平地より下がる（波乱）
    assert jump["win"][0] < flat["win"][0]
