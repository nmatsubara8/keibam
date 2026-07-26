"""全券種 ROI テスト — ワイド/馬連/馬単/三連複/三連単 で JRDB順序edgeが控除を超えるか。

これまで単勝(帰無)と三連単(帰無・控除27.5%)のみ検証。だが券種で控除が違う（馬連/ワイド22.5% /
馬単・三連複25% / 三連単27.5%）。特に**ワイド**(2頭共にtop3・低分散・低控除)は、見つけた
「順序/不確実性のedge＝top3構造」が最も直接効く。三連単で帰無でも別券種は別、を検証する。

各券種で: 市場≈Harville(単勝), モデル=JRDB place強度(goal_juni/ichi_idx)。
EV=model/market×(1−takeout[券種])。topk/ev 戦略＋placebo＋実配当決済＋券種別控除。
baseline(Harville)は EV<1 で賭け0（ev戦略の帰無）。

要データ: JRDB(KYI/SED)＋return_tables。実行:
  python multibet_roi_test.py --jrdb-dir data/jrdb_txt --strategy ev
"""
from __future__ import annotations

import argparse
from itertools import combinations, permutations

import numpy as np
import pandas as pd

from src.constants._bet_types import BetType
from src.constants._local_paths import LocalPaths
from src.constants._takeout import TAKEOUT
from src.policies._harville import combo_probability, combo_probability_place_strength
from src.tuning._payoffs import multi_bet_payoff_lookup
from trifecta_jrdb_test import KYI_SIGNALS, _place_probs, fit_coef, load_races

BET_TYPES = [BetType.WIDE, BetType.UMAREN, BetType.UMATAN,
             BetType.SANRENPUKU, BetType.SANRENTAN]
_UNORDERED = {BetType.UMAREN, BetType.WIDE, BetType.SANRENPUKU}
_SIZE = {BetType.WIDE: 2, BetType.UMAREN: 2, BetType.UMATAN: 2,
         BetType.SANRENPUKU: 3, BetType.SANRENTAN: 3}


def candidates(q, bet_type, top_m):
    top = [u for u, _ in sorted(q.items(), key=lambda kv: -kv[1])[:top_m]]
    sz = _SIZE[bet_type]
    gen = combinations if bet_type in _UNORDERED else permutations
    return [tuple(c) for c in gen(top, sz)]


def backtest(races, coef, payoffs, bet_type, *, strategy, top_m, max_bets,
             ev_threshold=1.0, placebo=False, seed=0):
    takeout = TAKEOUT.get(bet_type, 0.2)
    rng = np.random.default_rng(seed)
    stake = ret = 0.0
    n_bets = hit = 0
    for r in races:
        rid = r["rid"]
        wins = payoffs.get(rid)
        if not wins:
            continue
        q = r["q"]
        sig = r["sig"]
        if placebo and coef:
            keys = list(sig)
            vals = [sig[u] for u in keys]
            rng.shuffle(vals)
            sig = dict(zip(keys, vals, strict=False))
        plc = _place_probs(q, sig, coef) if coef else q
        win_map = {tuple(sorted(c)) if bet_type in _UNORDERED else tuple(c): p for c, p in wins}
        scored = []
        for combo in candidates(q, bet_type, top_m):
            mp = combo_probability(bet_type, q, combo)
            if mp <= 0:
                continue
            pp = combo_probability_place_strength(bet_type, q, plc, combo) if coef else mp
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
            key = tuple(sorted(combo)) if bet_type in _UNORDERED else tuple(combo)
            if key in win_map:
                ret += win_map[key] / 100.0
                hit += 1
    return {"roi": ret / stake if stake else 0.0, "n_bets": n_bets, "hit": hit,
            "takeout": takeout}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--jrdb-dir", default="/tmp/jrdb_all")
    ap.add_argument("--returns", default=LocalPaths.RAW_RETURN_TABLES_PATH)
    ap.add_argument("--with-tyb", action="store_true")
    ap.add_argument("--strategy", choices=("ev", "topk"), default="ev")
    ap.add_argument("--top-m", type=int, default=6)
    ap.add_argument("--max-bets", type=int, default=3)
    ap.add_argument("--train-frac", type=float, default=0.6)
    args = ap.parse_args()

    import os
    if not os.path.exists(args.returns):
        raise SystemExit(f"return_tables がありません: {args.returns}")
    rt = pd.read_pickle(args.returns)
    races, signals = load_races(args.jrdb_dir, with_tyb=args.with_tyb)
    n_tr = int(len(races) * args.train_frac)
    tr, te = races[:n_tr], races[n_tr:]
    coef = fit_coef(tr, signals)
    print(f"レース {len(races):,}（test {len(te):,}）signals={signals}")
    print(f"係数(train三連単NLL最小): { {k: round(v, 2) for k, v in coef.items()} }")
    print(f"\n{'券種':<8}{'控除':>6}{'baseline':>10}{'JRDB込み':>10}{'placebo':>9}{'的中':>7}{'点数':>7}  判定")
    print("-" * 68)
    for bt in BET_TYPES:
        pay = multi_bet_payoff_lookup(rt, bt)
        kw = dict(strategy=args.strategy, top_m=args.top_m, max_bets=args.max_bets)
        base = backtest(te, None, pay, bt, **kw)
        jr = backtest(te, coef, pay, bt, **kw)
        pl = backtest(te, coef, pay, bt, placebo=True, **kw)
        edge = jr["roi"] > 1.0 and jr["roi"] > max(base["roi"], pl["roi"]) + 0.03
        name = {BetType.WIDE: "ワイド", BetType.UMAREN: "馬連", BetType.UMATAN: "馬単",
                BetType.SANRENPUKU: "三連複", BetType.SANRENTAN: "三連単"}[bt]
        print(f"{name:<8}{jr['takeout']*100:>5.1f}%{base['roi']:>10.3f}{jr['roi']:>10.3f}"
              f"{pl['roi']:>9.3f}{jr['hit']:>7}{jr['n_bets']:>7}  "
              + ("★控除超え" if edge else "帰無"))
    print("\n※ROI>1.0 かつ baseline/placebo を明確に上回る券種のみ edge 候補。三連単で帰無でも")
    print("  低控除・低分散の券種(ワイド等)は別判定。--strategy topk / --with-tyb も試す。")


if __name__ == "__main__":
    main()
