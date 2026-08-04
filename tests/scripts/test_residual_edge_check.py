"""市場残差エッジ検定の純ロジック（within-race q・logit・残差binning）単体テスト。"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

_MOD = Path(__file__).resolve().parents[2] / "scripts" / "residual_edge_check.py"
_spec = importlib.util.spec_from_file_location("residual_edge_check", _MOD)
r = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(r)


def test_within_race_q_normalizes_per_race():
    odds = pd.Series([2.0, 4.0, 4.0, 5.0, 5.0])
    rids = pd.Series(["A", "A", "A", "B", "B"])
    q = r.within_race_q(odds, rids)
    # A: 1/2,1/4,1/4 → .5,.25,.25（Σ1）
    assert abs(q.iloc[0] - 0.5) < 1e-9 and abs(q.iloc[:3].sum() - 1.0) < 1e-9
    assert abs(q.iloc[3:].sum() - 1.0) < 1e-9


def test_within_race_q_ignores_nonpositive():
    odds = pd.Series([2.0, 0.0])
    rids = pd.Series(["A", "A"])
    q = r.within_race_q(odds, rids)
    assert abs(q.iloc[0] - 1.0) < 1e-9 and q.iloc[1] == 0.0


def test_logit_monotone_and_clip():
    assert r.logit(np.array([0.5]))[0] == 0.0
    assert r.logit(np.array([0.9]))[0] > r.logit(np.array([0.1]))[0]
    # 0/1 でも発散しない（clip）
    assert np.isfinite(r.logit(np.array([0.0, 1.0]))).all()


def test_residual_bin_stats_edelta_and_bins():
    n = 100
    rng = np.arange(n)
    delta = rng / n - 0.5              # 単調増加
    won = (rng >= 90).astype(float)    # 上位のみ勝ち
    q = np.full(n, 0.05)
    stats = r.residual_bin_stats(delta, won, q, n_bins=10)
    assert len(stats) == 10
    # 最上位ビンは realized 高く E[Δ]>0、最下位は realized 0 で E[Δ]<0
    assert stats.iloc[-1]["E_delta"] > 0
    assert stats.iloc[0]["E_delta"] < 0
