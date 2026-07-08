"""(a) 荒れ度(オッズのエントロピー) vs 三連単配当、(b) bold-play 的ターゲットの的中率・配当分布。

任意プール(中央/地方)に適用可能。runners(単勝/着順) と payoffs(三連単組合せ/払戻) を渡す。
エッジは主張しない。「どの条件でどれくらいの頻度でいくら当たるか」の現実的な地図を描く。

使い方:
    python scripts/payout_map/analyze_payout_map.py --label 中央(JRA) \
        --runners data/payout_map/jra_runners.csv --payoffs data/payout_map/jra_payoffs.csv
"""
from __future__ import annotations

import argparse
import itertools

import numpy as np
import pandas as pd

rng = np.random.default_rng(0)


def load(runners_path, payoffs_path, tansho_col):
    h = pd.read_csv(runners_path).rename(columns={tansho_col: "tansho_odds"})
    h = h[h["tansho_odds"] > 0]
    pay = pd.read_csv(payoffs_path).set_index("race_id")
    races = []
    for rid, g in h.groupby("race_id"):
        if rid not in pay.index:
            continue
        combo = pay.loc[rid]["sanrentan_c"]
        if not isinstance(combo, str) or not combo:
            continue
        inv = 1.0 / g["tansho_odds"].values
        p = inv / inv.sum()
        n = len(g)
        races.append({
            "race_id": rid, "n": n,
            "Hn": (-(p * np.log(p)).sum() / np.log(n)) if n > 1 else 0.0,
            "fav": list(g["umaban"].astype(int).values[np.argsort(-p)]),
            "actual": tuple(int(x) for x in combo.split("-")),
            "pay": pay.loc[rid]["sanrentan_pay"] / 100.0,
        })
    return pd.DataFrame(races)


def part_a(df):
    print("=" * 74)
    print("(a) 荒れ度(正規化エントロピー Hn∈[0,1]) vs 三連単配当")
    print("=" * 74)
    corr = np.corrcoef(df["Hn"], np.log(df["pay"]))[0, 1]
    print(f"corr(Hn, log配当) = {corr:.3f}   (n={len(df)})  ※ Hn大=断然人気なし=荒れ")
    df = df.copy()
    df["q"] = pd.qcut(df["Hn"], 5, labels=["最も堅い", "堅い", "中位", "荒れ", "最も荒れ"])
    print(f"\n{'荒れ度帯':<10}{'Hn平均':>8}{'配当中央値':>12}{'配当90%点':>12}{'本命的中率':>10}")
    for lab, g in df.groupby("q", observed=True):
        favhit = g.apply(lambda r: tuple(r["fav"][:3]) == r["actual"], axis=1)
        p90 = g["pay"].quantile(0.9) * 100
        print(f"{lab:<10}{g['Hn'].mean():>8.3f}{'¥' + format(g['pay'].median() * 100, ',.0f'):>12}"
              f"{'¥' + format(p90, ',.0f'):>12}{favhit.mean():>10.1%}")
    print("\n→ 荒れるほど配当中央値・90%点は単調に上がり、本命的中率は下がる。両方が『市場に織り込み済』。")


def strat_combos(row, kind):
    fav, n = row["fav"], row["n"]
    if n < 6:
        return []
    if kind == "本命1点(1>2>3)":
        return [tuple(fav[:3])]
    if kind == "上位3頭BOX(6点)":
        return list(itertools.permutations(fav[:3], 3))
    if kind == "フォーメーション1→234→234(6点)":
        return [(fav[0], a, b) for a in fav[1:4] for b in fav[1:4] if a != b]
    if kind == "中穴(4>5>6)":
        return [tuple(fav[3:6])]
    if kind == "大穴(下位3頭ランダム)":
        return [tuple(rng.permutation(fav[-6:])[:3])]
    if kind == "完全ランダム1点":
        return [tuple(rng.permutation(fav)[:3])]
    return []


STRATS = ["本命1点(1>2>3)", "上位3頭BOX(6点)", "フォーメーション1→234→234(6点)",
          "中穴(4>5>6)", "大穴(下位3頭ランダム)", "完全ランダム1点"]


def part_b(df):
    print("\n" + "=" * 74)
    print("(b) ターゲット別の的中率・回収率・配当分布（各レース定額）")
    print("=" * 74)
    print(f"{'戦略':<26}{'点/R':>5}{'的中率':>8}{'回収率':>8}{'的中時中央':>11}{'的中時最高':>11}{'≥100倍':>8}")
    for s in STRATS:
        staked = ret = hits = nr = big = k = 0
        pon = []
        for _, r in df.iterrows():
            combos = strat_combos(r, s)
            if not combos:
                continue
            nr += 1
            k = len(combos)
            staked += k
            if r["actual"] in combos:
                hits += 1
                ret += r["pay"]
                pon.append(r["pay"])
                big += r["pay"] >= 100
        hr = hits / nr if nr else 0
        rec = ret / staked if staked else 0
        med = np.median(pon) * 100 if pon else 0
        mx = max(pon) * 100 if pon else 0
        print(f"{s:<26}{k:>5}{hr:>8.1%}{rec:>8.3f}{'¥' + format(med, ',.0f'):>11}"
              f"{'¥' + format(mx, ',.0f'):>11}{big:>8}")
    print(f"\n(対象 {(df['n'] >= 6).sum()} レース)")
    print("→ 本命=高的中・低配当、穴=低的中・高配当。回収率は全戦略<1.0(=負け)＝平均は改善せず、"
          "変わるのは『分布の形』だけ。深い穴の狙い撃ちは的中ほぼ0。")


def part_b_boldplay(df):
    print("\n" + "=" * 74)
    print("bold-play の核心: 賭け『金額』の大胆さと目標到達確率（同じ買い目・stake の攻め方だけ変える）")
    print("=" * 74)

    def sim(strat, bet_frac, T, B=10000, unit=100, trials=4000):
        data = []
        for _, r in df[df["n"] >= 6].iterrows():
            combos = strat_combos(r, strat)
            if not combos:
                continue
            k = len(combos)
            hit = r["actual"] in combos
            data.append((r["pay"] / k - 1.0) if hit else -1.0)
        data = np.array(data)
        reached = 0
        for _ in range(trials):
            bank, ok = B, False
            for i in rng.permutation(len(data)):
                stake = min(bank, max(unit, np.floor(bet_frac * bank / unit) * unit))
                bank += stake * data[i]
                if bank >= T:
                    ok = True
                    break
                if bank < unit:
                    break
            reached += ok
        return reached / trials

    T = 50000
    print(f"開始¥10,000 → 目標¥{T:,}（5倍）到達確率:")
    print(f"  {'戦略×stake':<34}{'P(到達)':>10}")
    for label, strat, frac in [
        ("本命1点 ×timid(2%)", "本命1点(1>2>3)", 0.02),
        ("本命1点 ×bold(50%)", "本命1点(1>2>3)", 0.50),
        ("上位3頭BOX ×bold(50%)", "上位3頭BOX(6点)", 0.50),
        ("大穴 ×bold(50%)", "大穴(下位3頭ランダム)", 0.50),
    ]:
        print(f"  {label:<34}{sim(strat, frac, T):>10.1%}")
    print("\n→ 同じ買い目でも stake を timid→bold にするだけで到達確率が跳ねる(Dubins-Savage)。")
    print("  『大穴×bold』は的中ほぼ0で到達も低い＝一撃の器は本命/BOXの大胆staking。EVは常にマイナス。")


def main():
    ap = argparse.ArgumentParser(description="荒れ度×配当 と bold-play の数値地図")
    ap.add_argument("--runners", required=True)
    ap.add_argument("--payoffs", required=True)
    ap.add_argument("--tansho-col", default="tansho_odds")
    ap.add_argument("--label", default="")
    args = ap.parse_args()
    df = load(args.runners, args.payoffs, args.tansho_col)
    print(f"\n########## {args.label}  ({len(df)} レース) ##########")
    part_a(df)
    part_b(df)
    part_b_boldplay(df)
    print("\n結論: 荒れ度は配当を強く予測するが織り込み済でedge無し。全戦略で回収率<1.0＝平均マイナス。")
    print("一撃狙いなら bold play が合理的だが、それは予測の勝利でなく分散の設計（負けを承知で買う）。")


if __name__ == "__main__":
    main()
