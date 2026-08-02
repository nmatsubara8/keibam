"""情報量ベースの edge 源泉測定（純関数）＝市場を条件づけた条件付き相互情報量。

Residual Learning の中核: 市場が既に知っている情報は edge にならない。JRDB 特徴 X が勝敗 Y に
持つ情報のうち、**市場 M を知った後でも残る**分だけが edge の源泉になる。

  I(X;Y)      : 特徴が勝敗に持つ生の情報（市場と重複していてもよい）
  I(X;Y|M)    : 市場を条件づけた後に残る情報（＝市場に無い直交情報・edge 候補）
  I(X;Y) − I(X;Y|M) : 市場と重複する分（冗長）

すべて plug-in（ビン化した頻度）推定。連続 X/M は分位ビン。小標本ではビン数を上げると上方バイアスが
出るため、ビン数は控えめ既定＋件数ガードを置く。単位は bit（log2）。sklearn 非依存。
"""
from __future__ import annotations

import numpy as np


def quantile_bin(x, bins: int):
    """連続値を分位ビンの整数ラベルへ。NaN は -1。ユニーク値が少なければビン数を縮める。"""
    x = np.asarray(x, dtype=float)
    out = np.full(x.shape, -1, dtype=int)
    finite = np.isfinite(x)
    if finite.sum() == 0:
        return out
    xf = x[finite]
    nuniq = len(np.unique(xf))
    b = int(min(bins, max(1, nuniq)))
    if b <= 1:
        out[finite] = 0
        return out
    qs = np.quantile(xf, np.linspace(0, 1, b + 1))
    edges = np.unique(qs)
    if len(edges) <= 2:
        out[finite] = 0
        return out
    lab = np.digitize(xf, edges[1:-1], right=False)
    out[finite] = lab
    return out


def _mi_from_joint(joint) -> float:
    """2次元頻度表 (nx, ny) から相互情報量 I(X;Y)[bit] を plug-in 推定。"""
    joint = np.asarray(joint, dtype=float)
    n = joint.sum()
    if n <= 0:
        return 0.0
    pxy = joint / n
    px = pxy.sum(axis=1, keepdims=True)
    py = pxy.sum(axis=0, keepdims=True)
    denom = px * py
    mask = (pxy > 0) & (denom > 0)
    return float(np.sum(pxy[mask] * np.log2(pxy[mask] / denom[mask])))


def _joint_counts(xb, yb, nx=None, ny=None):
    """整数ラベル配列 (xb, yb) から頻度表を作る（負ラベル=欠測は除外）。"""
    xb = np.asarray(xb, dtype=int)
    yb = np.asarray(yb, dtype=int)
    m = (xb >= 0) & (yb >= 0)
    xb, yb = xb[m], yb[m]
    if len(xb) == 0:
        return np.zeros((1, 1))
    nx = nx if nx is not None else xb.max() + 1
    ny = ny if ny is not None else yb.max() + 1
    j = np.zeros((nx, ny))
    np.add.at(j, (xb, yb), 1)
    return j


def mutual_information(x, y, *, x_bins: int = 8) -> float:
    """I(X;Y)[bit]。x=連続(分位ビン化)、y=離散(0/1 等・そのままラベル)。NaN 行は除外。"""
    xb = quantile_bin(x, x_bins)
    yb = np.asarray(y, dtype=float)
    yok = np.isfinite(yb)
    yb2 = np.where(yok, yb, np.nan)
    # y を整数ラベルへ（0/1 前提だが一般の離散も可）
    yint = np.full(yb2.shape, -1, dtype=int)
    uy = np.unique(yb2[yok])
    for i, v in enumerate(uy):
        yint[yb2 == v] = i
    return _mi_from_joint(_joint_counts(xb, yint))


def conditional_mi(x, y, m, *, x_bins: int = 5, m_bins: int = 5, min_per_stratum: int = 30) -> float:
    """I(X;Y|M)[bit] = Σ_m P(M=m)·I(X;Y|M=m)。x,m=連続(分位ビン)、y=離散。

    各 M ストラタムで頻度が min_per_stratum 未満なら寄与を落とす（小標本の上方バイアス抑制）。
    """
    xb = quantile_bin(x, x_bins)
    mb = quantile_bin(m, m_bins)
    yb = np.asarray(y, dtype=float)
    yok = np.isfinite(yb)
    yint = np.full(yb.shape, -1, dtype=int)
    uy = np.unique(yb[yok])
    for i, v in enumerate(uy):
        yint[yb == v] = i
    valid = (xb >= 0) & (mb >= 0) & (yint >= 0)
    if valid.sum() == 0:
        return 0.0
    n_total = int(valid.sum())
    cmi = 0.0
    for mval in np.unique(mb[valid]):
        sel = valid & (mb == mval)
        n_m = int(sel.sum())
        if n_m < min_per_stratum:
            continue
        i_m = _mi_from_joint(_joint_counts(xb[sel], yint[sel]))
        cmi += (n_m / n_total) * i_m
    return float(cmi)


def edge_decomposition(x, y, m, *, x_bins: int = 5, m_bins: int = 5,
                       min_per_stratum: int = 30) -> dict:
    """特徴 X の情報を市場 M で分解する。返す {mi, cmi, redundant, edge_ratio}。

    mi=I(X;Y), cmi=I(X;Y|M)=市場を知った後に残る情報(edge候補),
    redundant=mi−cmi(市場と重複), edge_ratio=cmi/mi（1に近いほど市場と直交＝純edge）。
    """
    mi = mutual_information(x, y, x_bins=x_bins)
    cmi = conditional_mi(x, y, m, x_bins=x_bins, m_bins=m_bins, min_per_stratum=min_per_stratum)
    red = mi - cmi
    return {"mi": mi, "cmi": cmi, "redundant": red,
            "edge_ratio": (cmi / mi) if mi > 1e-12 else 0.0}
