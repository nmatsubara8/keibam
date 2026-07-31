"""物理シム勝率の確率校正（レース単位 temperature scaling）。

実測で sim の勝率は大幅に過信（予測≥0.5 でも実勝率 ~0.19）。p1≥0.5 や p×odds>閾値・joint 上位
など確率値を使う規則は、この壊れた尺度の上では無効。まず確率を校正し、予測0.5→実勝率0.5 に
近づけてから確率ベース戦略を評価する。

安全な出発点＝レース単位 temperature scaling（レース内で確率和1を保つ）:
    p~_i = p_i^(1/T) / Σ_j p_j^(1/T)
T>1 で尖った分布を平坦化（過信を緩和）、T<1 で先鋭化。T は学習期間の勝者 NLL 最小で1つ推定し、
翌年へ固定（walk-forward）。単純な馬ごと二値校正と違いレース内正規化が保たれる。

races: 各レース (p_array, winner_idx)。p_array は馬順の勝率（Σ≒1 でなくてよい・内部で正規化）、
winner_idx は勝ち馬の位置。
"""
from __future__ import annotations

import math

_EPS = 1e-12


def apply_temperature(p_array, T: float):
    """レース内 temperature scaling: p~_i = p_i^(1/T) / Σ_j p_j^(1/T)（リスト返し）。"""
    inv = 1.0 / max(T, _EPS)
    powered = [max(float(p), 0.0) ** inv for p in p_array]
    s = sum(powered)
    if s <= 0:
        n = len(p_array)
        return [1.0 / n] * n if n else []
    return [x / s for x in powered]


def nll(races, T: float = 1.0) -> float:
    """温度 T での勝者平均 NLL（低いほど良い）。空は inf。"""
    total = 0.0
    n = 0
    for p_array, w in races:
        if not (0 <= w < len(p_array)):
            continue
        pw = apply_temperature(p_array, T)[w]
        total += -math.log(max(pw, _EPS))
        n += 1
    return total / n if n else float("inf")


def fit_temperature(races, *, lo: float = 0.3, hi: float = 12.0, iters: int = 60) -> float:
    """勝者 NLL を最小化する温度 T を黄金分割探索で1つ推定する（学習期間で fit → 翌年固定）。"""
    if not races:
        return 1.0
    gr = (math.sqrt(5) - 1) / 2
    a, b = lo, hi
    c = b - gr * (b - a)
    d = a + gr * (b - a)
    fc, fd = nll(races, c), nll(races, d)
    for _ in range(iters):
        if fc < fd:
            b, d, fd = d, c, fc
            c = b - gr * (b - a)
            fc = nll(races, c)
        else:
            a, c, fc = c, d, fd
            d = a + gr * (b - a)
            fd = nll(races, d)
    return (a + b) / 2


def reliability_top(races, *, T: float = 1.0, bins=None) -> list[dict]:
    """各レースの最上位予測馬について、予測勝率帯別の (件数, 平均予測, 実勝率) を返す。

    S9 の p1 閾値が「強い軸」を選べているかの確認。T を渡すと校正後で評価する。
    """
    if bins is None:
        bins = [(0.0, 0.2), (0.2, 0.3), (0.3, 0.4), (0.4, 0.5), (0.5, 0.6), (0.6, 0.8),
                (0.8, 1.01)]
    top = []
    for p_array, w in races:
        if not p_array:
            continue
        pc = apply_temperature(p_array, T)
        j = max(range(len(pc)), key=lambda k: pc[k])
        top.append((pc[j], 1 if j == w else 0))
    out = []
    for lo, hi in bins:
        sub = [(p, y) for p, y in top if lo <= p < hi]
        if not sub:
            continue
        out.append({"lo": lo, "hi": hi, "n": len(sub),
                    "pred": sum(p for p, _ in sub) / len(sub),
                    "act": sum(y for _, y in sub) / len(sub)})
    return out


def ece_top(races, *, T: float = 1.0, n_bins: int = 10) -> float:
    """最上位予測馬の Expected Calibration Error（|予測−実測| の件数加重平均・低いほど良い）。"""
    top = []
    for p_array, w in races:
        if not p_array:
            continue
        pc = apply_temperature(p_array, T)
        j = max(range(len(pc)), key=lambda k: pc[k])
        top.append((pc[j], 1 if j == w else 0))
    if not top:
        return 0.0
    tot = len(top)
    err = 0.0
    for b in range(n_bins):
        lo, hi = b / n_bins, (b + 1) / n_bins
        sub = [(p, y) for p, y in top if (lo <= p < hi or (b == n_bins - 1 and p == 1.0))]
        if not sub:
            continue
        pred = sum(p for p, _ in sub) / len(sub)
        act = sum(y for _, y in sub) / len(sub)
        err += (len(sub) / tot) * abs(pred - act)
    return err
