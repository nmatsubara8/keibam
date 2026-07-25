"""三連単 Stage B — 実配当 ROI バックテスト。JRDBの順序edgeは控除(約27.5%)を超えるか。

Stage A で「JRDBの展開予想が三連単の順序(2/3着)に有意な較正改善(ΔNLL−0.023・placebo確認)」を
確認。だが較正改善≠ROI。本スクリプトは実際の三連単配当で ROI を測り、生存線(控除超え)を判定する。

市場アンカー方式（単勝の枠組みを連系へ）:
  市場の三連単確率 ≈ Harville(確定単勝)  … 群衆の連系価格づけの代理
  モデルの三連単確率 = 位置特化トリフェクタ（2/3着を JRDB 展開指数で調整）
  EV(combo) = model_prob / market_prob × (1−takeout)
  買い: EV > ev_threshold の combo（各レース top-M 頭の順列に限定して計算）
  決済: 当選 combo を買っていれば実配当(payoff/100)、外れは −1（1点=100円単位）

**baseline(model=Harville) は EV=1×(1−takeout)<1 で自動的に賭け0＝綺麗な帰無。**
JRDB込みだけが「市場より順序を高く見た買い目」を賭ける。ROI が 1.0(控除後の損益均衡)を
超えれば、本調査で初の exploitable edge。placebo(signalシャッフル)で消えることを要確認。

要データ: JRDB(KYI/SED/TYB) ＋ return_tables(三連単配当)。時系列OOSで評価。

実行: python trifecta_roi_test.py --jrdb-dir /tmp/jrdb_all
      （return_tables は LocalPaths から自動読込。--takeout/--ev/--top-m/--max-bets 調整可）
"""
from __future__ import annotations

import argparse
from itertools import permutations

import numpy as np
import pandas as pd

from src.constants._local_paths import LocalPaths
from src.policies._harville import prob_trifecta, prob_trifecta_place_strength
from src.tuning._payoffs import trifecta_payoff_lookup
from trifecta_jrdb_test import _place_probs, fit_coef, load_races


def candidate_combos(q: dict, top_m: int) -> list[tuple[int, int, int]]:
    """勝率上位 top_m 頭の順列（三連単候補）。全 n(n-1)(n-2) を top_m に限定して計算量を抑える。"""
    top = [u for u, _ in sorted(q.items(), key=lambda kv: -kv[1])[:top_m]]
    return list(permutations(top, 3))


def backtest(races, coef, payoffs, *, strategy="ev", takeout=0.275, ev_threshold=1.0,
             top_m=7, max_bets=3, placebo=False, seed=0):
    """三連単の買い目を実配当で決済し ROI を返す。2戦略:

    strategy="ev": EV=model/market×(1−takeout)>閾値 を買う（market=Harville単勝）。
        coef=None は baseline（model=market）→ EV<1 → 賭け0（綺麗な帰無）。
    strategy="topk": model 確率上位 max_bets 点を買う（市場prob不要・実配当を直接使う）。
        JRDB込み vs Harville(coef=None) の ROI 差が「順序edgeが配当価値に化けるか」を測る。
    placebo は signal をレース内シャッフル。
    """
    rng = np.random.default_rng(seed)
    stake = ret = 0.0
    n_bets = hit = 0
    for r in races:
        rid = r["rid"]
        if rid not in payoffs:
            continue
        q = r["q"]
        sig = r["sig"]
        if placebo and coef:
            keys = list(sig)
            vals = [sig[u] for u in keys]
            rng.shuffle(vals)
            sig = dict(zip(keys, vals, strict=False))
        plc = _place_probs(q, sig, coef) if coef else q
        win_combo, payoff = payoffs[rid]
        scored = []  # (score, combo)
        for combo in candidate_combos(q, top_m):
            mp = prob_trifecta(q, *combo)
            if mp <= 0:
                continue
            pp = prob_trifecta_place_strength(q, plc, *combo) if coef else mp
            if strategy == "ev":
                ev = pp / mp * (1.0 - takeout)
                if ev > ev_threshold:
                    scored.append((ev, combo))
            else:  # topk: モデル確率で順位付け
                scored.append((pp, combo))
        scored.sort(reverse=True)
        for _, combo in scored[:max_bets]:
            n_bets += 1
            stake += 1.0
            if tuple(combo) == tuple(win_combo):
                ret += payoff / 100.0
                hit += 1
    roi = ret / stake if stake > 0 else 0.0
    return {"roi": roi, "n_bets": n_bets, "hit": hit,
            "hit_rate": hit / n_bets if n_bets else 0.0,
            "profit_units": ret - stake, "stake_units": stake}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--jrdb-dir", default="/tmp/jrdb_all")
    ap.add_argument("--returns", default=LocalPaths.RAW_RETURN_TABLES_PATH)
    ap.add_argument("--with-tyb", action="store_true")
    ap.add_argument("--strategy", choices=("ev", "topk"), default="ev",
                    help="ev=EV>閾値(市場=Harville) / topk=モデル確率上位を買う")
    ap.add_argument("--takeout", type=float, default=0.275)
    ap.add_argument("--ev", type=float, default=1.0)
    ap.add_argument("--top-m", type=int, default=7)
    ap.add_argument("--max-bets", type=int, default=3)
    ap.add_argument("--train-frac", type=float, default=0.6)
    args = ap.parse_args()

    import os
    if not os.path.exists(args.returns):
        raise SystemExit(f"return_tables がありません: {args.returns}")
    payoffs = trifecta_payoff_lookup(pd.read_pickle(args.returns))
    print(f"三連単配当: {len(payoffs):,}レース")

    races, signals = load_races(args.jrdb_dir, with_tyb=args.with_tyb)
    n_tr = int(len(races) * args.train_frac)
    tr, te = races[:n_tr], races[n_tr:]
    te_hit = [r for r in te if r["rid"] in payoffs]
    print(f"完全順序レース: {len(races):,}（test {len(te):,}・配当照合 {len(te_hit):,}）"
          f"  signals={signals}")
    if len(te_hit) < 200:
        print("警告: 配当照合レースが少ない（JRDBとreturn_tablesの期間重なりを確認）")

    coef = fit_coef(tr, signals)
    print(f"train最適係数: { {k: round(v, 2) for k, v in coef.items()} }")

    kw = dict(strategy=args.strategy, takeout=args.takeout, ev_threshold=args.ev,
              top_m=args.top_m, max_bets=args.max_bets)
    base = backtest(te, None, payoffs, **kw)
    jrdb = backtest(te, coef, payoffs, **kw)
    plac = backtest(te, coef, payoffs, placebo=True, **kw)
    print(f"\n== Stage B: 三連単 実配当 ROI（strategy={args.strategy}・控除 {args.takeout*100:.1f}%）==")
    print(f"  baseline(Harville) : 賭け{base['n_bets']:>6}点 的中{base['hit']} ROI={base['roi']:.3f}"
          + ("（EV<1で0＝帰無）" if args.strategy == "ev" else "（単勝Harville順位）"))
    print(f"  JRDB込み           : 賭け{jrdb['n_bets']:>6}点 的中{jrdb['hit']}（{jrdb['hit_rate']*100:.2f}%）"
          f" ROI={jrdb['roi']:.3f} 損益{jrdb['profit_units']:+.0f}単位")
    print(f"  placebo(signal破壊): 賭け{plac['n_bets']:>6}点 ROI={plac['roi']:.3f}")
    edge_ref = max(base["roi"], plac["roi"])
    verdict = ("★控除超え＝exploitable edge（要 別期間・別券種で再確認）"
               if jrdb["roi"] > 1.0 and jrdb["roi"] > edge_ref + 0.05
               else f"控除を超えず（baseline {base['roi']:.3f} / placebo {plac['roi']:.3f} 比でも優位薄）")
    print(f"  判定: {verdict}")
    print("\n両 strategy（--strategy ev / topk）と --with-tyb, --max-bets, --top-m を振って")
    print("サブセット（本命寄り/穴寄り・点数）ごとの ROI も確認推奨。")


if __name__ == "__main__":
    main()
