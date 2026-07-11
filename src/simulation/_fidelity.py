"""忠実度(fidelity)検証の中核メトリクス（純関数）。

sim を「予測器」でなく「物理現象のモデル」として評価する。**創発ダイナミクスが実測分布と
一致するか**を無次元(相関・順位)で測る。絶対値(秒・m)は sim と実で単位が違うので使わない。

主要メトリクス:
- pace_shape_corr : レースごとの「sim ペース(序盤−終盤速度)」と「実ペース(前半−上がり由来)」の
  順位相関。sim が『どのレースが前傾か』を再現できるか。
- pace_backness_signal : 「ハイペース→追込有利」という展開機構が、実測とsimの双方に現れるか。
  レースを pace 中央値で hi/lo に分け、各群で corr(backness, 相対着順) を測り、
  signal = corr_lo − corr_hi（正＝ハイペースで後方脚質が相対的に前に来る＝展開の再現）。
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def spearman(a, b) -> float:
    """順位相関（欠損・定数は nan）。"""
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    m = np.isfinite(a) & np.isfinite(b)
    if int(m.sum()) < 3:
        return float("nan")
    ra = pd.Series(a[m]).rank().to_numpy()
    rb = pd.Series(b[m]).rank().to_numpy()
    if ra.std() == 0 or rb.std() == 0:
        return float("nan")
    return float(np.corrcoef(ra, rb)[0, 1])


def pace_shape_corr(sim_pace, real_pace) -> float:
    """レース単位: sim ペースと実ペースの順位相関（正＝どのレースが速いかを再現）。"""
    return spearman(sim_pace, real_pace)


def pace_backness_signal(backness, rank_norm, race_pace) -> dict:
    """展開機構の忠実度: ハイペースで後方脚質(backness大)が相対的に前(rank_norm小)に来るか。

    backness   : 0=先行 … 1=追込（実は leg_type_binary、sim は style/2）
    rank_norm  : 相対着順 0=1着 … 1=最下位
    race_pace  : 各行のレースペース（前傾ほど大）。中央値で hi/lo 分割。

    signal = corr_lo − corr_hi。正なら「ハイペースほど後方脚質が前」＝展開が効いている。
    実測と sim で同じ関数を通し、signal の符号・大きさを比較する。
    """
    backness = np.asarray(backness, dtype=float)
    rank_norm = np.asarray(rank_norm, dtype=float)
    race_pace = np.asarray(race_pace, dtype=float)
    m = np.isfinite(backness) & np.isfinite(rank_norm) & np.isfinite(race_pace)
    if int(m.sum()) < 10:
        return {"corr_hi": float("nan"), "corr_lo": float("nan"), "signal": float("nan")}
    b, r, p = backness[m], rank_norm[m], race_pace[m]
    med = np.median(p)
    hi = p >= med
    lo = ~hi
    corr_hi = spearman(b[hi], r[hi])
    corr_lo = spearman(b[lo], r[lo])
    signal = (corr_lo - corr_hi) if (np.isfinite(corr_hi) and np.isfinite(corr_lo)) else float("nan")
    return {"corr_hi": corr_hi, "corr_lo": corr_lo, "signal": signal}
