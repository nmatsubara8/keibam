"""意思決定システム backtest の純ロジック（KLブレンド/Dirichlet分散/Kelly/選定/精算）テスト。"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np

_MOD = Path(__file__).resolve().parents[2] / "scripts" / "decision_system_backtest.py"
_spec = importlib.util.spec_from_file_location("decision_system_backtest", _MOD)
d = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(d)


def test_kl_blend_endpoints_and_normalization():
    p = np.array([0.6, 0.3, 0.1])
    q = np.array([0.2, 0.3, 0.5])
    # λ=0 → p̂ そのもの、λ=1 → q そのもの
    assert np.allclose(d.kl_blend(p, q, 0.0), p)
    assert np.allclose(d.kl_blend(p, q, 1.0), q)
    b = d.kl_blend(p, q, 0.5)
    assert abs(b.sum() - 1.0) < 1e-12 and b[0] > b[2]


def test_dirichlet_var_shrinks_with_c():
    p = np.array([0.5, 0.3, 0.2])
    hi = d.dirichlet_var(p, 10.0)
    lo = d.dirichlet_var(p, 100.0)
    assert np.all(lo < hi)                      # c 大 → 分散小
    assert np.allclose(hi, p * (1 - p) / 11.0)


def test_kelly_zero_when_no_edge():
    p = np.array([0.10, 0.30])
    o = np.array([2.0, 5.0])                     # EV: 0.2<1, 1.5>1
    f = d.kelly_fraction(p, o, kappa=1.0, f_max=1.0)
    assert f[0] == 0.0 and f[1] > 0.0            # 負けEVは0


def test_select_and_size_filters_and_budget_cap():
    p = np.array([0.5, 0.5])                     # 2頭とも高EV
    o = np.array([3.0, 3.0])
    var = np.array([0.01, 0.01])
    params = {"tau_edge": 0.0, "tau_var": 0.02, "o_min": 1.0, "o_max": 100.0,
              "kappa": 1.0, "f_max": 1.0, "f_race": 0.1}
    f = d.select_and_size(p, o, var, params)
    assert abs(f.sum() - 0.1) < 1e-9            # 合計は F_race に按分


def test_settle_race_pays_final_odds():
    f = np.array([0.1, 0.0])
    o_final = np.array([4.0, 2.0])
    won = np.array([1.0, 0.0])
    g, staked, payoff, nb = d.settle_race(f, o_final, won)
    assert abs(staked - 0.1) < 1e-9 and abs(payoff - 0.4) < 1e-9
    assert abs(g - 0.1 * (4.0 - 1.0)) < 1e-9 and nb == 1
