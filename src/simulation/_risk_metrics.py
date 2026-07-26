"""リスク指標（Step5 評価）— Max Drawdown / Expected Shortfall(CVaR)。

Kelly は E[log W]（幾何成長）最大化だがドローダウンには敏感で、競馬は数百連敗が普通に
起こる。**評価だけは必ずしておく**（目的関数には入れない — 入れると Kelly の理論的性質が
壊れるため。悪ければ max_race_fraction 等の制約側で対処する）。純粋計算のみ。
"""
from __future__ import annotations

from typing import Sequence

import numpy as np


def max_drawdown(wealth_path: Sequence[float]) -> float:
    """資産経路の最大ドローダウン max_t (1 − W_t / max_{s≤t} W_s) ∈ [0,1]。空は 0。"""
    if not wealth_path:
        return 0.0
    w = np.asarray(wealth_path, dtype=float)
    peak = np.maximum.accumulate(w)
    with np.errstate(divide="ignore", invalid="ignore"):
        dd = np.where(peak > 0, 1.0 - w / peak, 0.0)
    return float(np.max(dd))


def expected_shortfall(returns: Sequence[float], alpha: float = 0.05) -> float:
    """Expected Shortfall（CVaR_α）＝ 下位 α 分位より悪い損益率の平均の符号反転（損失は正）。

    returns はレース単位の資産比の純増減（evaluate_pnl の ret_frac）。標本が α 分位に
    満たない場合は最悪値を返す。空は 0。
    """
    r = np.asarray([float(x) for x in returns], dtype=float)
    if len(r) == 0:
        return 0.0
    var = np.quantile(r, alpha)
    tail = r[r <= var]
    if len(tail) == 0:
        tail = np.array([r.min()])
    return float(-tail.mean())
