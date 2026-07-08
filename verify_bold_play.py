"""一撃(big score)を狙う分散設計の数値デモ（データ不要）。損失最小化の双対。

EV は常に −控除率（分散を変えても平均は不変）。操作できるのは「分布の形＝到達確率」だけ。
市場が fair-but-for-takeout(o=(1−t)/p) の前提で、元手 B→目標 T へ届く確率を設計別に比較する。

使い方:
    python verify_bold_play.py --bankroll 10000 --target 1000000
    python verify_bold_play.py --bankroll 10000 --target 50000 --takeout 0.20
"""
from __future__ import annotations

import argparse
import sys

import numpy as np

from src.policies._bold_play import (
    BoldPlayDesign,
    bet_to_target_stake,
    fair_win_prob,
    single_shot_reach_prob,
)


def _rule(t):
    print("\n" + "=" * 72 + f"\n{t}\n" + "=" * 72)


def sim_single_shot(B, T, t, trials, rng):
    """1点集中: 目標倍率 m のオッズに全額。当たり(p=(1-t)/m)で T、外れで 0。"""
    m = T / B
    p = single_shot_reach_prob(m, t)
    win = rng.random(trials) < p
    final = np.where(win, T, 0.0)
    return final


def sim_parlay(B, T, t, legs, trials, rng):
    """k 脚パーレイ: 等倍 o=m^(1/k) を都度 let-it-ride。全脚勝ちで T、途中で外すと 0。"""
    m = T / B
    o = m ** (1.0 / legs)
    p = fair_win_prob(o, t)
    wins = (rng.random((trials, legs)) < p).all(axis=1)
    return np.where(wins, B * o ** legs, 0.0)


def sim_sequential_bold(B, T, t, odds, trials, rng, max_steps=200):
    """逐次 bold: 固定オッズ o で bet-to-target を繰り返す（当たりで T、外れたら残高から続行）。"""
    p = fair_win_prob(odds, t)
    out = np.empty(trials)
    for i in range(trials):
        bank = B
        for _ in range(max_steps):
            stake = bet_to_target_stake(bank, T, odds, unit=100)
            if stake <= 0 or bank < 100:
                break
            if rng.random() < p:
                bank += (odds - 1) * stake
            else:
                bank -= stake
            if bank >= T:
                break
        out[i] = bank
    return out


def sim_timid(B, T, t, trials, rng, max_steps=100000):
    """堅実: 単勝 1.25 倍(p=0.64)相当を毎回 ¥100 フラット。broke か T か窓上限まで。"""
    odds = (1 - t) / 0.64  # 的中0.64の券のオッズ（≈1.25）
    p = 0.64
    out = np.empty(trials)
    for i in range(trials):
        bank = B
        for _ in range(max_steps):
            if bank < 100 or bank >= T:
                break
            bank += (odds - 1) * 100 if rng.random() < p else -100
        out[i] = bank
    return out


def summarize(name, final, B, T):
    p_reach = float((final >= T).mean())
    p_broke = float((final < 100).mean())
    e_final = float(final.mean())
    print(f"{name:<28} P(目標到達)={p_reach:7.3%}  P(ほぼ全損)={p_broke:6.1%}  "
          f"E[最終]/元手={e_final / B:5.3f}")
    return p_reach, e_final / B


def main() -> int:
    ap = argparse.ArgumentParser(description="一撃を狙う分散設計の数値デモ")
    ap.add_argument("--bankroll", type=float, default=10000.0)
    ap.add_argument("--target", type=float, default=1_000_000.0)
    ap.add_argument("--takeout", type=float, default=0.20)
    ap.add_argument("--trials", type=int, default=200000)
    ap.add_argument("--plot", default=None, help="分布グラフの保存先 PNG")
    args = ap.parse_args()
    B, T, t = args.bankroll, args.target, args.takeout
    rng = np.random.default_rng(0)

    _rule(f"一撃を狙う分散設計 — 元手 ¥{B:,.0f} → 目標 ¥{T:,.0f}（控除 {t:.0%}）")
    d = BoldPlayDesign(B, T, t)
    r = d.report()
    print(f"目標倍率 m = T/B = {r['multiple']:.1f} 倍")
    print(f"1点集中に必要なオッズ ≈ {r['required_odds_single_shot']:.1f} 倍")
    print("\n閉形式の到達確率（market fair-but-for-takeout 前提）:")
    print(f"  1点集中(1発)   P = (1−t)/m = {r['single_shot_reach_prob']:.3%}")
    print(f"  2脚パーレイ     P = (1−t)²/m = {r['parlay2_reach_prob']:.3%}")
    print(f"  4脚パーレイ     P = (1−t)⁴/m = {r['parlay4_reach_prob']:.3%}")
    print(f"  → 最適脚数 = {r['optimal_legs']}（脚を増やすほど控除が累乗で効き不利）")
    print(f"  期待損益率 = {r['expected_pnl_rate']:.0%}（どの設計でも不変。平均は変えられない）")

    _rule("モンテカルロ（閉形式の確認＋分布の形）")
    ss = sim_single_shot(B, T, t, args.trials, rng)
    pl = sim_parlay(B, T, t, 4, args.trials, rng)
    sb = sim_sequential_bold(B, T, t, 6.0, min(args.trials, 40000), rng)
    tm = sim_timid(B, T, t, min(args.trials, 20000), rng)
    rows = [
        ("1点集中(o=m)", summarize("1点集中(o=m)", ss, B, T)),
        ("4脚パーレイ", summarize("4脚パーレイ", pl, B, T)),
        ("逐次bold(o=6固定)", summarize("逐次bold(o=6固定)", sb, B, T)),
        ("堅実(単勝1.25倍¥100)", summarize("堅実(単勝1.25倍¥100)", tm, B, T)),
    ]

    _rule("結論")
    print("・一撃の最適は『1点集中・目標倍率ちょうどのオッズ』。到達確率 = (1−t)/m。")
    print("・脚/回数を増やすほど控除が累乗で効き、到達確率も E[最終] も下がる。堅実は事実上到達不能。")
    print("・E[P&L] はどの設計でも −控除率。分散（到達確率↔全損確率）だけを設計している。")
    print("・これは損失最小化の双対: 損失最小化は E[損失] 最小（賭けない）。一撃設計は"
          " P[目標到達] 最大（大胆に1発）。両方とも EV は負で、目的関数が違うだけ。")

    if args.plot:
        _make_plot(rows, B, T, t, args.plot)
        print(f"\nグラフ: {args.plot}")
    return 0


def _make_plot(rows, B, T, t, path):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    labels = ["single-shot\n(o=m)", "4-leg\nparlay", "sequential\nbold(o=6)", "timid\n(flat ¥100)"]
    preach = [r[1][0] for r in rows]
    efinal = [r[1][1] for r in rows]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(12, 5))
    a1.bar(labels, preach, color=["#F58518", "#4C78A8", "#54A24B", "#B279A2"])
    a1.axhline(single_shot_reach_prob(T / B, t), color="#E45756", ls="--",
               label=f"(1-t)/m = {single_shot_reach_prob(T / B, t):.2%}")
    for i, p in enumerate(preach):
        a1.text(i, p, f"{p:.2%}", ha="center", va="bottom", fontsize=9)
    a1.set_ylabel("P(reach target)")
    a1.set_title(f"Prob. of reaching ¥{T:,.0f} from ¥{B:,.0f}\n(fewer/bolder bets = higher)")
    a1.legend()
    a2.bar(labels, efinal, color=["#F58518", "#4C78A8", "#54A24B", "#B279A2"])
    a2.axhline(1 - t, color="#E45756", ls="--", label=f"1-t = {1 - t:.2f}")
    a2.set_ylabel("E[final] / bankroll")
    a2.set_title("Expected capital preserved\n(all <= 1-t; more bets = lose takeout more times)")
    a2.legend()
    fig.suptitle("Bold play (variance design for one big score): EV is always -takeout; "
                 "only the distribution shape changes", y=1.02)
    fig.tight_layout()
    fig.savefig(path, dpi=130, bbox_inches="tight")


if __name__ == "__main__":
    sys.exit(main())
