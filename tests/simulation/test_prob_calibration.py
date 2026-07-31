"""確率校正（レース単位 temperature scaling）のテスト。"""
from __future__ import annotations

import math
import random

from src.simulation._prob_calibration import (
    apply_temperature,
    ece_top,
    fit_temperature,
    nll,
    reliability_top,
)


def test_apply_temperature_normalizes_and_flattens():
    p = [0.7, 0.2, 0.1]
    hot = apply_temperature(p, 1.0)
    assert abs(sum(hot) - 1.0) < 1e-9
    flat = apply_temperature(p, 3.0)                    # T>1 は平坦化
    assert abs(sum(flat) - 1.0) < 1e-9
    assert flat[0] < hot[0] and flat[2] > hot[2]        # 過信を緩める
    sharp = apply_temperature(p, 0.5)                   # T<1 は先鋭化
    assert sharp[0] > hot[0]


def test_fit_temperature_recovers_overconfidence():
    # 過信データ: 予測は尖っているが勝者は一様に近い → T>1 が NLL を下げるはず。
    rng = random.Random(0)
    races = []
    for _ in range(400):
        p = [0.8, 0.1, 0.06, 0.04]
        w = rng.randrange(4)                            # 実際はほぼ一様に勝つ（予測と乖離）
        races.append((p, w))
    T = fit_temperature(races)
    assert T > 1.2                                       # 過信を検出して平坦化方向
    assert nll(races, T) <= nll(races, 1.0) + 1e-9      # 校正で NLL 改善


def test_reliability_and_ece():
    races = [([0.9, 0.05, 0.05], 1), ([0.85, 0.1, 0.05], 2), ([0.8, 0.15, 0.05], 0)]
    rel = reliability_top(races, T=1.0)
    assert rel and all(0.0 <= r["act"] <= 1.0 for r in rel)
    assert 0.0 <= ece_top(races) <= 1.0


def test_nll_empty():
    assert math.isinf(nll([]))
    assert fit_temperature([]) == 1.0
