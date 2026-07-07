"""効率的市場における RL/マルコフ手法の無効性シミュレータの不変条件テスト。

核心は「効率市場では、いかなる学習器も市場を破れず、最適方策は abstain（賭けない）」。
対照群として stale（非効率）市場では**同じアルゴリズム**が勝つことを固定し、無効性の
原因がアルゴリズムではなく市場効率であることを担保する。
"""

from __future__ import annotations

import numpy as np

from src.simulation._efficient_market import (
    ACT_ABSTAIN,
    ACT_FAVORITE,
    MODE_EFFICIENT,
    MODE_STALE,
    ContextualBandit,
    MarketConfig,
    Races,
    action_net,
    bet_net,
    market_baseline,
    regime_posterior,
    regime_strategy,
    run_futility_experiment,
    simulate_market,
    transition_matrix,
)


def _rng(seed=0):
    return np.random.default_rng(seed)


# --- 市場生成の基本 ----------------------------------------------------------


def test_transition_matrix_rows_sum_to_one():
    tm = transition_matrix(0.9)
    assert np.allclose(tm.sum(axis=1), 1.0)
    assert np.isclose(tm[0, 0], 0.9) and np.isclose(tm[0, 1], 0.1)


def test_simulate_market_shapes_and_bad_mode():
    cfg = MarketConfig(n_horses=8)
    races = simulate_market(50, cfg, MODE_EFFICIENT, _rng())
    assert len(races) == 50
    assert races.p_true.shape == (50, 8)
    assert races.odds.shape == (50, 8)
    assert np.allclose(races.p_true.sum(axis=1), 1.0)
    try:
        simulate_market(10, cfg, "nonsense", _rng())
        raise AssertionError("unknown mode should raise")
    except ValueError:
        pass


def test_efficient_market_every_horse_ev_is_one_minus_takeout():
    """効率市場では p_market·odds = 1−控除率 が全馬で厳密に成り立つ（EV=1−t）。"""
    cfg = MarketConfig(n_horses=8, takeout=0.20)
    races = simulate_market(200, cfg, MODE_EFFICIENT, _rng())
    # p_market == p_true なので、真の期待払戻 = p_true·odds も 1−t
    ev_gross = races.p_true * races.odds
    assert np.allclose(ev_gross, 0.80)
    assert np.allclose(races.p_market, races.p_true)


def test_stale_market_has_exploitable_gap():
    """stale 市場は odds 構成上 p_market·odds=1−t だが p_true≠p_market。

    レジーム=1 の優遇馬は真の勝率 > 市場評価 ⇒ p_true·odds > 1 の馬が存在する（正の期待値）。
    """
    cfg = MarketConfig(n_horses=8, takeout=0.20, regime_bonus=1.6)
    races = simulate_market(400, cfg, MODE_STALE, _rng())
    assert np.allclose(races.p_market * races.odds, 0.80)
    reg1 = races.regime == 1
    assert reg1.any()
    true_ev = races.p_true * races.odds  # >1 なら正の期待値
    # レジーム=1 のレースに期待値プラスの馬が実在する
    assert (true_ev[reg1] > 1.0).any()


# --- ベットの純損益の算術 ----------------------------------------------------


def _tiny_races(odds_row, winner, n=8):
    T = 1
    odds = np.array([odds_row], dtype=float)
    return Races(
        regime=np.zeros(T, dtype=int),
        p_true=np.full((T, len(odds_row)), 1.0 / len(odds_row)),
        p_market=np.full((T, len(odds_row)), 1.0 / len(odds_row)),
        odds=odds,
        signal=np.zeros(T),
        winner=np.array([winner]),
        favorite_idx=np.array([0]),
        regime_pick_idx=np.array([0]),
        noise=np.zeros(T),
        takeout=0.20,
        mode=MODE_EFFICIENT,
    )


def test_bet_net_hit_and_miss():
    races = _tiny_races([3.0, 2.0, 5.0], winner=0)
    assert bet_net(races, 0, 0) == 2.0  # 払戻3.0 − 元本1.0
    assert bet_net(races, 0, 1) == -1.0  # 外れ


def test_action_net_abstain_is_zero():
    races = _tiny_races([3.0, 2.0, 5.0], winner=0)
    assert action_net(races, 0, ACT_ABSTAIN) == 0.0
    assert action_net(races, 0, ACT_FAVORITE) == 2.0  # favorite_idx=0, winner=0


# --- HMM 前向きフィルタ ------------------------------------------------------


def test_regime_posterior_tracks_signal():
    cfg = MarketConfig(signal_noise=0.5, p_stay=0.9)
    high = regime_posterior(np.ones(30), cfg)
    low = regime_posterior(np.zeros(30), cfg)
    assert high[-1] > 0.8  # 信号がずっと1 → レジーム1と判断
    assert low[-1] < 0.2  # 信号がずっと0 → レジーム0と判断


# --- 効率市場: 素朴ベットは控除率ぶん負ける -------------------------------


def test_efficient_baseline_loses_takeout():
    cfg = MarketConfig(takeout=0.20)
    races = simulate_market(3000, cfg, MODE_EFFICIENT, _rng())
    res = market_baseline(races)
    assert res["mean_net"] < -0.03  # 明確に負け
    assert res["recovery_rate"] < 1.0


# --- 効率市場: RL は勝てない（粗い文脈 → abstain に収束）-------------------


def test_efficient_coarse_bandit_abstains_and_does_not_profit():
    cfg = MarketConfig(takeout=0.20)
    rng = _rng(0)
    train = simulate_market(1500, cfg, MODE_EFFICIENT, rng)
    test = simulate_market(1500, cfg, MODE_EFFICIENT, rng)
    bandit = ContextualBandit(n_noise_bins=5).train(train)
    oos = bandit.evaluate(test)
    base = market_baseline(test)
    # 大半のレースで賭けない
    assert oos["abstain_fraction"] > 0.5
    # 利益は出せない（OOS の平均純損益は 0 近傍以下）
    assert oos["mean_net"] <= 0.05
    # 賭けを控えるぶん素朴ベースラインよりはマシ（損失を避ける）
    assert oos["mean_net"] >= base["mean_net"]


# --- 効率市場: 文脈を細かく切ると in-sample だけ利益（300%の機序）---------


def test_efficient_overfit_bandit_profit_vanishes_out_of_sample():
    cfg = MarketConfig(takeout=0.20)
    rng = _rng(0)
    train = simulate_market(1500, cfg, MODE_EFFICIENT, rng)
    test = simulate_market(1500, cfg, MODE_EFFICIENT, rng)
    overfit = ContextualBandit(n_noise_bins=40).train(train)
    is_ = overfit.evaluate(train)
    oos = overfit.evaluate(test)
    # in-sample は見かけ上プラス（偽の文脈へ過学習）
    assert is_["mean_net_per_bet"] > 0.0
    # OOS では消えてマイナス（＝backtest の嘘）
    assert oos["mean_net_per_bet"] < 0.0
    assert is_["mean_net_per_bet"] > oos["mean_net_per_bet"]


# --- 効率市場: HMM レジーム検出は無価値 ------------------------------------


def test_efficient_regime_strategy_does_not_profit():
    cfg = MarketConfig(takeout=0.20)
    races = simulate_market(3000, cfg, MODE_EFFICIENT, _rng())
    res = regime_strategy(races, cfg, threshold=0.6)
    assert res["n_bets"] > 0
    # レジームは価格済み → 賭けても利益は出ない（0 近傍以下）
    assert res["mean_net_per_bet"] <= 0.05


# --- 対照群: 同じ手法が stale（非効率）市場では勝つ -------------------------


def test_control_regime_strategy_profits_only_when_market_inefficient():
    """無効性の原因が市場効率であることの核心テスト。

    効率市場では負ける HMM レジーム戦略が、stale 市場では勝つ。差が edge の源泉。
    """
    r = run_futility_experiment(seed=0)
    eff = r["efficient"]["regime"]["mean_net_per_bet"]
    stale = r["stale"]["regime"]["mean_net_per_bet"]
    assert stale > 0.15  # 非効率なら明確に勝つ
    assert eff <= 0.05  # 効率なら勝てない
    assert (stale - eff) > 0.20  # 効率↔非効率で成績が反転する


def test_control_bandit_profits_in_stale_market():
    r = run_futility_experiment(seed=0)
    stale_oos = r["stale"]["bandit"]["oos"]["mean_net"]
    eff_oos = r["efficient"]["bandit"]["oos"]["mean_net"]
    assert stale_oos > 0.05  # 非効率市場では RL も真の edge を掴む
    assert stale_oos > eff_oos


# --- 再現性 ------------------------------------------------------------------


def test_run_is_reproducible():
    a = run_futility_experiment(seed=42)
    b = run_futility_experiment(seed=42)
    assert a["efficient"]["baseline"]["mean_net"] == b["efficient"]["baseline"]["mean_net"]
    assert (
        a["stale"]["regime"]["mean_net_per_bet"]
        == b["stale"]["regime"]["mean_net_per_bet"]
    )
