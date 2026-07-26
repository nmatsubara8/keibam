"""物理MC三連単 Stage B — 実配当 ROI。物理の順序edge(ΔNLL−0.046)は控除を超えるか。

Stage A で物理MC(市場アンカーplace強度)が Harville を ΔNLL−0.046 で有意に上回り(placebo確認・
線形JRDBの2倍)。だが較正改善≠ROI。本スクリプトは実三連単配当で ROI を測り、控除27.5%超えを判定。

市場アンカー: P(1着)=市場単勝、2/3着=物理place強度（prob_trifecta_place_strength）。
  市場の三連単確率 ≈ Harville(単勝)。EV=model/market×(1−takeout)。baseline(Harville)は
  EV<1で賭け0の帰無。物理込みだけが「市場より順序を高く見た買い目」を賭ける。
戦略: ev(EV>閾値) / topk(モデル確率上位K点)。placebo(物理place強度シャッフル)＋時系列OOS。

要データ: JRDB(KYI/SED) ＋ return_tables(三連単配当)。物理MCは全レースで sim を回す(重い)。

実行: python physics_trifecta_roi.py --jrdb-dir data/jrdb_txt --n-sim 1500 --strategy topk
"""
from __future__ import annotations

import argparse
from itertools import permutations

import numpy as np
import pandas as pd

from src.constants._local_paths import LocalPaths
from src.policies._harville import prob_trifecta, prob_trifecta_place_strength
from src.tuning._payoffs import trifecta_payoff_lookup
from physics_trifecta_test import load, sim_place_strength


def candidate_combos(q, top_m):
    top = [u for u, _ in sorted(q.items(), key=lambda kv: -kv[1])[:top_m]]
    return list(permutations(top, 3))


def precompute_strengths(races, a_scale, beta, n_sim):
    """各レースの物理 place 強度を先に計算してキャッシュ（sim は1回だけ）。"""
    cache = {}
    for i, r in enumerate(races):
        ps, _ = sim_place_strength(r, a_scale, n_sim, beta, seed=i)
        cache[r["rid"]] = ps
    return cache


def backtest(races, strengths, payoffs, *, use_physics, strategy, takeout, ev_threshold,
             top_m, max_bets, placebo=False, seed=0):
    rng = np.random.default_rng(seed)
    stake = ret = 0.0
    n_bets = hit = 0
    for r in races:
        rid = r["rid"]
        if rid not in payoffs:
            continue
        q = r["q"]
        if use_physics:
            ps = dict(strengths[rid])
            if placebo:
                keys = list(ps)
                vals = [ps[u] / q[u] for u in keys]
                rng.shuffle(vals)
                ps = {u: q[u] * v for u, v in zip(keys, vals, strict=False)}
        win_combo, payoff = payoffs[rid]
        scored = []
        for combo in candidate_combos(q, top_m):
            mp = prob_trifecta(q, *combo)
            if mp <= 0:
                continue
            pp = prob_trifecta_place_strength(q, ps, *combo) if use_physics else mp
            if strategy == "ev":
                ev = pp / mp * (1.0 - takeout)
                if ev > ev_threshold:
                    scored.append((ev, combo))
            else:
                scored.append((pp, combo))
        scored.sort(reverse=True)
        for _, combo in scored[:max_bets]:
            n_bets += 1
            stake += 1.0
            if tuple(combo) == tuple(win_combo):
                ret += payoff / 100.0
                hit += 1
    return {"roi": ret / stake if stake else 0.0, "n_bets": n_bets, "hit": hit,
            "profit": ret - stake}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--jrdb-dir", default="/tmp/jrdb_all")
    ap.add_argument("--returns", default=LocalPaths.RAW_RETURN_TABLES_PATH)
    ap.add_argument("--n-sim", type=int, default=1500)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--strategy", choices=("ev", "topk"), default="topk")
    ap.add_argument("--takeout", type=float, default=0.275)
    ap.add_argument("--ev", type=float, default=1.0)
    ap.add_argument("--top-m", type=int, default=7)
    ap.add_argument("--max-bets", type=int, default=3)
    ap.add_argument("--train-frac", type=float, default=0.5)
    args = ap.parse_args()

    import os
    if not os.path.exists(args.returns):
        raise SystemExit(f"return_tables がありません: {args.returns}")
    payoffs = trifecta_payoff_lookup(pd.read_pickle(args.returns))
    races = load(args.jrdb_dir, limit=args.limit)
    n_tr = int(len(races) * args.train_frac)
    tr, te = races[:n_tr], races[n_tr:]
    te_hit = [r for r in te if r["rid"] in payoffs]
    print(f"レース {len(races):,}（test {len(te):,}・配当照合 {len(te_hit):,}）n_sim={args.n_sim}")

    # 較正（train）: a_scale（市場勝率一致）→ beta（三連単NLL最小）
    best_a = 0.8
    best_b, best_n = 0.0, 1e18
    for b in (-0.3, -0.15, 0.0, 0.15, 0.3):
        tot = 0.0
        for r in tr[:250]:
            ps, _ = sim_place_strength(r, best_a, args.n_sim, b)
            tot -= np.log(max(prob_trifecta_place_strength(r["q"], ps, *r["top3"]), 1e-12))
        if tot < best_n:
            best_n, best_b = tot, b
    print(f"較正: a_scale={best_a} beta={best_b}")

    print("物理MC 全testレースの place 強度を計算中…（重い）")
    strengths = precompute_strengths(te, best_a, best_b, args.n_sim)

    kw = dict(strategy=args.strategy, takeout=args.takeout, ev_threshold=args.ev,
              top_m=args.top_m, max_bets=args.max_bets)
    base = backtest(te, strengths, payoffs, use_physics=False, **kw)
    phys = backtest(te, strengths, payoffs, use_physics=True, **kw)
    plac = backtest(te, strengths, payoffs, use_physics=True, placebo=True, **kw)
    print(f"\n== Stage B: 物理MC三連単 実配当 ROI（strategy={args.strategy}・控除{args.takeout*100:.1f}%）==")
    print(f"  baseline(Harville): 賭け{base['n_bets']:>6}点 的中{base['hit']} ROI={base['roi']:.3f}")
    print(f"  物理MC込み        : 賭け{phys['n_bets']:>6}点 的中{phys['hit']} ROI={phys['roi']:.3f}"
          f" 損益{phys['profit']:+.0f}単位")
    print(f"  placebo(物理破壊) : 賭け{plac['n_bets']:>6}点 ROI={plac['roi']:.3f}")
    edge_ref = max(base["roi"], plac["roi"])
    print("  判定: " + ("★控除超え＝exploitable edge（要 別期間・別券種で再確認）"
                        if phys["roi"] > 1.0 and phys["roi"] > edge_ref + 0.05
                        else f"控除を超えず（baseline {base['roi']:.3f}/placebo {plac['roi']:.3f} 比でも優位薄）"))


if __name__ == "__main__":
    main()
