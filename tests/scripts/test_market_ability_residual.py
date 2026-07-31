"""Δ_ability ゲートの純ロジック（過去soten集約の leak-safe性・直交残差・decile）テスト。"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

_MOD = Path(__file__).resolve().parents[2] / "scripts" / "market_ability_residual.py"
_spec = importlib.util.spec_from_file_location("market_ability_residual", _MOD)
m = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(m)


def test_build_hist_soten_is_leak_safe():
    # 1頭・4走。s_last は必ず「前走」で、当該走 soten を含めない。
    df = pd.DataFrame({
        "horse_id": ["h1"] * 4,
        "race_id": ["202401", "202402", "202403", "202404"],
        "soten": [50.0, 60.0, 70.0, 80.0],
    })
    out = m.build_hist_soten(df).sort_values("race_id")
    # 1走目は過去なし→NaN。2走目 s_last=50(前走)。当該走(80)は s_last に出ない。
    assert np.isnan(out["s_last"].iloc[0])
    assert out["s_last"].iloc[1] == 50.0 and out["s_last"].iloc[3] == 70.0
    assert out["s_max5"].iloc[3] == 70.0        # 過去最大(50,60,70)=70、当該80は含まぬ
    # s_trend = s_last - mean(s_{t-2}, s_{t-3}); 4走目 = 70 - mean(60,50)=70-55=15
    assert abs(out["s_trend"].iloc[3] - 15.0) < 1e-9


def test_orth_residual_removes_market_component():
    rng = np.random.default_rng(0)
    n = 500
    mkt = rng.normal(size=(n, 1))
    myspeed = 3.0 * mkt[:, 0] + rng.normal(scale=0.1, size=n)   # ほぼ市場で説明可能
    fit = np.ones(n, dtype=bool)
    resid = m.orth_residual(myspeed, mkt, fit)
    # 残差は市場成分をほぼ除去 → 元より分散が大幅に小さい
    assert resid.std() < myspeed.std() * 0.2


def test_decile_realized_monotone_for_sorted_signal():
    n = 1000
    delta = np.arange(n) / n
    won = (np.arange(n) >= 500).astype(float)   # 上位ほど実現率↑
    dec = m.decile_realized(delta, won, n=10)
    assert dec[0] == 0.0 and dec[-1] == 1.0
    assert np.all(np.diff(dec) >= -1e-9)
