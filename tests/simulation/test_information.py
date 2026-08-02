"""条件付き相互情報量(edge 源泉測定)の単体テスト。

市場 M を条件づけたとき、特徴 X が勝敗 Y に持つ情報が「市場と重複(冗長)」か「市場に無い(edge)」かを
分解できることを、決定的な合成データで検証する（乱数不使用）。
"""
from __future__ import annotations

from src.simulation._information import (
    conditional_mi,
    edge_decomposition,
    mutual_information,
    quantile_bin,
)


def _blocks(*vals_counts):
    out = []
    for v, c in vals_counts:
        out += [v] * c
    return out


def test_mutual_information_perfect_and_independent():
    # Y=X（完全依存）→ MI≈1bit
    x = _blocks((0, 200), (1, 200))
    y = list(x)
    assert mutual_information(x, y, x_bins=2) > 0.99
    # X と Y が独立 → MI≈0
    x2 = [0, 1] * 200                       # 交互
    y2 = _blocks((0, 200), (1, 200))        # 前半0後半1（X の交互と無相関）
    assert mutual_information(x2, y2, x_bins=2) < 0.02


def test_conditional_mi_redundant_vs_orthogonal():
    # 冗長: M=X=Y → I(X;Y)>0 だが M を知れば X は無情報 → CMI≈0
    x = _blocks((0, 200), (1, 200))
    y = list(x)
    m = list(x)
    d = edge_decomposition(x, y, m, x_bins=2, m_bins=2, min_per_stratum=30)
    assert d["mi"] > 0.99
    assert d["cmi"] < 0.02                  # 市場を知ると edge は消える
    assert d["edge_ratio"] < 0.05

    # 直交: M は X と独立、Y=X → M を知っても X はまだ Y を決める → CMI≈MI
    x2 = [0, 1] * 200
    y2 = list(x2)
    m2 = _blocks((0, 200), (1, 200))        # X の交互と無相関な市場
    d2 = edge_decomposition(x2, y2, m2, x_bins=2, m_bins=2, min_per_stratum=30)
    assert d2["mi"] > 0.99
    assert d2["cmi"] > 0.9                  # 市場に無い純情報が残る
    assert d2["edge_ratio"] > 0.9


def test_quantile_bin_handles_nan_and_low_cardinality():
    import numpy as np
    xb = quantile_bin([1.0, 2.0, np.nan, 3.0, 4.0], bins=4)
    assert xb[2] == -1                      # NaN は -1（欠測）
    assert set(xb[xb >= 0]) <= set(range(4))
    # 単一値はビン0に潰れる（クラッシュしない）
    assert set(quantile_bin([5.0, 5.0, 5.0], bins=4)) == {0}


def test_conditional_mi_small_stratum_dropped():
    # min_per_stratum 未満のストラタムは寄与しない（上方バイアス抑制）
    x = _blocks((0, 200), (1, 200))
    y = list(x)
    m = _blocks((0, 395), (1, 5))           # M=1 は 5 件のみ→落とす
    cmi = conditional_mi(x, y, m, x_bins=2, m_bins=2, min_per_stratum=30)
    assert cmi >= 0.0                        # クラッシュせず有限
