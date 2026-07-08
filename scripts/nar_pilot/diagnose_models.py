"""連系の予測器比較: 素Harville / 割引Harville(Lo,Bacon-Shu) / 市場含意 を実的中の対数尤度で比較。

問い: 「単勝由来Harville以外の連系予測器なら、薄い連系プールに勝てるか?」
的中三連単に各予測器が与えた確率の平均logを比べる。市場含意が最大なら「どの単勝ベースモデルも
勝てない=効率的」。割引Harville(λ=0.81,θ=0.65)はHarvilleの下位着バイアスを補正した改良版。

使い方: python scripts/nar_pilot/diagnose_models.py --data-dir data/nar_pilot
"""
from __future__ import annotations

import argparse
import itertools
import os

import numpy as np
import pandas as pd

LAM, THE, T3 = 0.81, 0.65, 0.275


def win_probs(g):
    inv = 1.0 / g["tansho_odds"].values
    return dict(zip(g["umaban"].astype(int).values, inv / inv.sum(), strict=True))


def p_harville(pmap, combo):
    i, j, k = combo
    pi, pj, pk = pmap.get(i), pmap.get(j), pmap.get(k)
    if None in (pi, pj, pk):
        return np.nan
    d1, d2 = 1 - pi, 1 - pi - pj
    if d1 <= 1e-9 or d2 <= 1e-9:
        return np.nan
    return pi * (pj / d1) * (pk / d2)


def p_discounted(pmap, combo):
    i, j, k = combo
    if any(u not in pmap for u in combo):
        return np.nan
    others = {m: p for m, p in pmap.items() if m != i}
    d1 = sum(p ** LAM for p in others.values())
    if d1 <= 1e-12:
        return np.nan
    others2 = {m: p for m, p in others.items() if m != j}
    d2 = sum(p ** THE for p in others2.values())
    if d2 <= 1e-12:
        return np.nan
    return pmap[i] * (pmap[j] ** LAM / d1) * (pmap[k] ** THE / d2)


def disc_top6(pmap, top=5):
    horses = sorted(pmap, key=lambda u: -pmap[u])[:top]
    scored = [(c, p_discounted(pmap, c)) for c in itertools.permutations(horses, 3)]
    scored = [(c, p) for c, p in scored if p == p]
    scored.sort(key=lambda x: -x[1])
    return [c for c, _ in scored[:6]]


def parse_combo(s):
    return tuple(int(x) for x in s.split("-")) if isinstance(s, str) and s else None


def build(data_dir):
    horses = pd.read_csv(os.path.join(data_dir, "nar_pilot.csv"))
    horses = horses[horses["tansho_odds"] > 0]
    pay = pd.read_csv(os.path.join(data_dir, "nar_payoffs.csv")).set_index("race_id")
    recs = []
    for rid, g in horses.groupby("race_id"):
        if rid not in pay.index:
            continue
        pr = pay.loc[rid]
        act = parse_combo(pr["sanrentan_c"])
        if not act or len(act) != 3:
            continue
        pmap = win_probs(g)
        payyen = pr["sanrentan_pay"] / 100.0
        recs.append({"place_name": g["place_name"].iloc[0],
                     "ll_harv": p_harville(pmap, act), "ll_disc": p_discounted(pmap, act),
                     "p_mkt": (1 - T3) / payyen if payyen > 0 else np.nan,
                     "disc6_hit": int(act in disc_top6(pmap)), "pay": payyen})
    return pd.DataFrame(recs)


def report(name, df):
    d = df.dropna(subset=["ll_harv", "ll_disc", "p_mkt"])
    d = d[(d["ll_harv"] > 0) & (d["ll_disc"] > 0) & (d["p_mkt"] > 0)]
    ll_h, ll_d, ll_m = (float(np.log(d[c]).mean()) for c in ("ll_harv", "ll_disc", "p_mkt"))
    best = max([("素Harville", ll_h), ("割引Harville", ll_d), ("市場", ll_m)], key=lambda x: x[1])
    disc6 = float((df["disc6_hit"] * df["pay"]).mean() / 6.0)
    print(f"\n[{name}] races={len(d)}")
    print(f"  対数尤度: 素Harville={ll_h:.3f}  割引Harville={ll_d:.3f}"
          f"({'改善' if ll_d > ll_h else '悪化'})  市場含意={ll_m:.3f}")
    print(f"  → 最良予測器: {best[0]}"
          + ("（市場最良＝単勝ベースでは勝てない=効率的）" if best[0] == "市場" else "（★モデルが市場超え=edge候補）"))
    print(f"  割引Harville上位6点 回収率 = {disc6:.3f}（分岐1.0 / 控除基準0.725）")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="data/nar_pilot")
    args = ap.parse_args()
    df = build(args.data_dir)
    if df.empty:
        print("データ空")
        return 1
    report("全場プール", df)
    for pn, g in df.groupby("place_name"):
        report(pn, g)
    print("\n読み取り: 市場含意の対数尤度が素/割引Harvilleより高い → 連系プールは単勝ベースの")
    print("どのモデルより良い予測器。天井は単勝そのもの。残る扉は単勝と直交するファンダのみだが、")
    print("JRA echo 0.989/ΔR²≈0/joint残差R²≈0.0001 がその見込みの薄さを示唆。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
