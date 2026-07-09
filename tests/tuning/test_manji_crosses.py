"""因子クロス（相互作用）の選別と残差化の単体テスト。

- _residualize_crosses: クロス点から加法成分(point_A+point_B)を引き、純粋な相互作用だけを残す。
  加法で説明できるクロスは消える／単独因子は無改変／クロス無しなら no-op。
- calibrate_points(residualize=…): 実データのクロスで残差化ON/OFFの差を検証。
- screen_crosses: 「単独では弱いが組合せで強い」交互作用を仕込み、上位に拾えることを検証。

合成データの仕掛け: 4頭立て・単勝一律4.0。勝者は base 重みに (奇数馬番 かつ 牡) のとき
だけブーストが乗る。性別はランダムなので単独の「奇数」「牡」は希釈され弱いが、クロス
「奇数|牡」は強い回収率を持つ＝加法では表せない相互作用。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.tuning._manji_calibration import _residualize_crosses, calibrate_points
from src.tuning._manji_crosses import screen_crosses


def _interaction_df(n_races=500, boost=6.0, seed=0):
    rng = np.random.default_rng(seed)
    rows, idx = [], []
    day0 = pd.Timestamp("2020-01-01")
    for i in range(n_races):
        rid = f"R{i:05d}"
        day = day0 + pd.Timedelta(days=i)
        sexes = [str(rng.choice(["牡", "牝"])) for _ in range(4)]  # 馬番1..4
        w = np.ones(4)
        for u in range(4):
            umaban = u + 1
            if umaban % 2 == 1 and sexes[u] == "牡":   # 奇数×牡 だけブースト
                w[u] += boost
        w /= w.sum()
        winner = int(rng.choice(range(4), p=w))
        for u in range(4):
            rows.append({"馬番": u + 1, "着順": 1 if u == winner else u + 2,
                         "単勝": 4.0, "性別": sexes[u], "date": day})
            idx.append(rid)
    return pd.DataFrame(rows, index=idx)


def test_residualize_crosses_removes_additive_part():
    pts = {
        "A": {"a1": 0.5, "a2": -0.3},
        "B": {"b1": 0.2, "b2": -0.1},
        # a1|b1 は加法(0.7)より上振れ=相互作用あり。他は加法通り=相互作用なし。
        "A*B": {"a1|b1": 0.9, "a1|b2": 0.4, "a2|b1": -0.1, "a2|b2": -0.4},
    }
    r = _residualize_crosses(pts)
    assert r["A"] == {"a1": 0.5, "a2": -0.3}          # 単独は無改変
    assert list(r["A*B"]) == ["a1|b1"]                # 相互作用バケットだけ残る
    assert r["A*B"]["a1|b1"] == pytest.approx(0.2)    # 0.9 − (0.5+0.2)
    # 加法通りのバケットは残差≈0 → 脱落
    for b in ("a1|b2", "a2|b1", "a2|b2"):
        assert b not in r["A*B"]


def test_residualize_crosses_drops_pure_additive_cross_entirely():
    pts = {"A": {"a1": 0.5}, "B": {"b1": 0.2}, "A*B": {"a1|b1": 0.7}}
    r = _residualize_crosses(pts)
    assert "A*B" not in r                              # 相互作用ゼロのクロスは丸ごと脱落
    assert r["A"] == {"a1": 0.5}


def test_residualize_crosses_noop_without_crosses():
    pts = {"A": {"a1": 0.5}}
    assert _residualize_crosses(pts) is pts            # クロス無しは同一オブジェクト返却


def test_calibrate_points_residualizes_real_cross():
    df = _interaction_df(seed=1)
    factors = ["umaban_parity", "sex"]
    cross = "umaban_parity*sex"
    raw = calibrate_points(df, factors + [cross], min_n=30,
                           universality_slices=1, residualize=False)
    res = calibrate_points(df, factors + [cross], min_n=30,
                           universality_slices=1, residualize=True)
    # 生のクロス点 odd|牡 は強い正。残差はそこから単独の和を引いた値で、生とは異なる。
    assert raw[cross]["odd|牡"] > 0
    add = raw["umaban_parity"].get("odd", 0.0) + raw["sex"].get("牡", 0.0)
    assert res[cross]["odd|牡"] == pytest.approx(raw[cross]["odd|牡"] - add, abs=1e-9)
    # 相互作用は本物なので残差も正に残る（加法だけでは説明できない上振れ）
    assert res[cross]["odd|牡"] > 0
    # 単独因子は残差化で無改変
    assert res["umaban_parity"] == raw["umaban_parity"]


def test_screen_crosses_surfaces_planted_interaction():
    df = _interaction_df(seed=2)
    top = screen_crosses(df, ["umaban_parity", "sex"], top_n=3,
                         min_coverage=100, min_n=30)
    assert "umaban_parity*sex" in top                  # 仕込んだ相互作用が拾われる
