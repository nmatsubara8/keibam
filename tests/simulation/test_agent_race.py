"""エージェントベース競馬シミュレーション＋モンテカルロのコアテスト。

決定論性（seed固定）・確率の整合（勝率和=1・複勝≥勝率）・単調性（能力↑→勝率↑）・
機構の効き（スタミナ枯渇でハイペース逃げが差しに沈む）を固定する。
"""
from __future__ import annotations

import numpy as np

from src.simulation._agent_race import (
    SimConfig,
    field_from_arrays,
    monte_carlo,
)


def test_probabilities_are_consistent():
    field = field_from_arrays([1.0, 1.0, 1.0, 1.0], ["front", "stalker", "closer", "stalker"])
    r = monte_carlo(field, n_sim=500, seed=1)
    assert np.isclose(r["win"].sum(), 1.0)             # 勝率は1レースで1頭 → 和=1
    assert (r["place"] >= r["win"] - 1e-9).all()       # 複勝率 ≥ 勝率
    assert r["finish_counts"].sum() == 500 * 4          # 全 sim×全馬の着順が記録される


def test_deterministic_with_seed():
    field = field_from_arrays([1.0, 0.9, 1.1], ["front", "stalker", "closer"])
    a = monte_carlo(field, n_sim=300, seed=7)
    b = monte_carlo(field, n_sim=300, seed=7)
    assert np.array_equal(a["finish_counts"], b["finish_counts"])
    # 異なる seed では（ほぼ確実に）異なる
    c = monte_carlo(field, n_sim=300, seed=8)
    assert not np.array_equal(a["finish_counts"], c["finish_counts"])


def test_higher_ability_wins_more():
    # 能力チャンネルを分離するためスタミナは潤沢（枯渇が支配しない）にする。
    # （実運用では ability と stamina は特徴量から相関して推定される＝強い馬は持続もする）
    field = field_from_arrays([1.3, 1.0, 0.7], ["stalker", "stalker", "stalker"],
                              stamina=[5.0, 5.0, 5.0], noise=[0.02, 0.02, 0.02])
    r = monte_carlo(field, n_sim=1500, seed=3)
    # 平均着順が能力順に単調（勝率は上位馬に集中しやすく 0 同士の比較になり得るので mean_rank を使う）
    assert r["mean_rank"][0] < r["mean_rank"][1] < r["mean_rank"][2]
    assert r["win"][0] >= r["win"][1] >= r["win"][2] and r["win"][0] > 0.5


def test_stamina_lets_closer_beat_burning_frontrunner():
    # ハイペースを作るため逃げ2頭 + 追込1頭。逃げはスタミナ低め・追込は高め。
    # 機構が効けば「前半飛ばした逃げがバテ、同能力の追込が差す」＝追込の勝率が逃げ1頭を上回る。
    field = field_from_arrays(
        ability=[1.0, 1.0, 1.0],
        style_names=["front", "front", "closer"],
        stamina=[0.6, 0.6, 1.4],       # 逃げは早枯れ、追込は余力
        noise=[0.03, 0.03, 0.03],
    )
    cfg = SimConfig(stamina_cost=0.03)  # 消耗を効かせる
    r = monte_carlo(field, n_sim=2000, seed=5, cfg=cfg)
    # 追込(idx2)の勝率が、各逃げ(idx0,1)の勝率より高い＝展開（スタミナ切れ）の再現
    assert r["win"][2] > r["win"][0]
    assert r["win"][2] > r["win"][1]


def test_field_from_arrays_defaults():
    f = field_from_arrays([1.0, 1.0], ["front", "closer"])
    assert f.n == 2
    assert list(f.style) == [0, 2]
    assert (f.stamina == 1.0).all()
