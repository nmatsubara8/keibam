"""rank_gain × threshold を同時最適化し max_t ROI(gain,t) で比較する（投資モデルの3層評価）。

Tier1(採否): 各 gain の最大 ROI = max_t ROI(gain, t)。← これで rank_gain の真価を測る。
Tier2(診断): その最適 t での 的中率/買い目数/平均オッズ/最大DD(円)/Sharpe。
Tier3(参考): log-loss(sim/market)・AUC・Brier（threshold 非依存）。log-loss は採否の主指標にしない。

効率: 物理シムは rank_gain のみに依存（確率生成）、threshold は購入判定だけ→ gain ごとにシム1回・
threshold は安く sweep。gain 間はシム seed 列を固定して paired 比較（差は rank_gain だけ）。

⚠ featured の rank_bonus は単一スナップ全期間＝leak（過去 ROI 探索用・live(as-of)には transfer しない）。
先に scripts/build_rank_bonus.py で rank_bonus 付き featured を作り、--featured で渡す。

使い方:
  python scripts/build_rank_bonus.py --featured data/featured_jrdb.pkl --out data/featured_rankbonus.pkl
  python scripts/sim_rank_gain_sweep.py --featured data/featured_rankbonus.pkl \
    --rank-gains "-0.5,-0.3,-0.1,0,0.1,0.3,0.5" --thresholds "1.0,1.05,1.1,1.15,1.2,1.3" \
    --limit 6000 --n-sim 400
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _floats(s: str) -> list[float]:
    return [float(x) for x in s.replace(" ", "").split(",") if x != ""]


def _simulate_gain(featured, order, *, rank_gain, n_sim, T, ability_spread, ability_sigma, seed):
    """1つの rank_gain で全レースをシムし races=[(p_sim, odds, winner_idx)] を返す（paired seed）。"""
    import numpy as np
    import pandas as pd

    from src.constants._results_cols import ResultsCols
    from src.simulation._agent_race import SimConfig, monte_carlo
    from src.simulation._sim_params import field_from_featured

    cfg = SimConfig(T=T)
    rng = np.random.default_rng(seed)          # gain 間で同一 seed 列＝paired
    races = []
    for rid in order:
        rd = featured.loc[[rid]] if not isinstance(featured.loc[rid], pd.DataFrame) else featured.loc[rid]
        if len(rd) < 2:
            continue
        odds = pd.to_numeric(rd[ResultsCols.TANSHO_ODDS], errors="coerce").to_numpy()
        rank = pd.to_numeric(rd[ResultsCols.RANK], errors="coerce").to_numpy()
        seed_i = int(rng.integers(1 << 30))     # このレースの seed（gain 間で同一）
        if not np.isfinite(odds).all() or np.nanmin(odds) <= 0:
            continue
        winner = np.where(rank == 1)[0]
        if len(winner) != 1:
            continue
        field = field_from_featured(rd, ability_spread=ability_spread, rank_gain=rank_gain)
        p_sim = monte_carlo(field, n_sim=n_sim, cfg=cfg, seed=seed_i,
                            ability_sigma=ability_sigma)["win"]
        races.append((p_sim, odds, int(winner[0])))
    return races


def main() -> int:
    from src.simulation._bet_eval import best_threshold, ev_bet_metrics, quality_metrics

    ap = argparse.ArgumentParser(description="rank_gain × threshold 同時最適化（3層評価）")
    ap.add_argument("--featured", default=None, help="rank_bonus 付き featured（既定=本番 featured）")
    ap.add_argument("--rank-gains", default="-0.5,-0.3,-0.1,0,0.1,0.3,0.5")
    ap.add_argument("--thresholds", default="1.0,1.05,1.10,1.15,1.20,1.30")
    ap.add_argument("--limit", type=int, default=6000)
    ap.add_argument("--max-year", type=int, default=None)
    ap.add_argument("--n-sim", type=int, default=400)
    ap.add_argument("--T", type=int, default=100)
    ap.add_argument("--ability-spread", type=float, default=0.20)
    ap.add_argument("--ability-sigma", type=float, default=0.35)
    ap.add_argument("--min-odds", type=float, default=1.0)
    ap.add_argument("--max-odds", type=float, default=100.0)
    ap.add_argument("--min-bets", type=int, default=50, help="この買い目未満の threshold は採用しない")
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    import pandas as pd

    from app._model_eval import load_featured_data

    featured = load_featured_data(args.featured) if args.featured else load_featured_data()
    if featured is None or featured.empty:
        print("featured がありません", file=sys.stderr)
        return 1
    if "rank_bonus" not in featured.columns:
        print("[warn] featured に rank_bonus 列がありません → rank_gain は無効（先に build_rank_bonus.py）",
              file=sys.stderr)

    date = pd.to_datetime(featured["date"]).groupby(level=0).first().sort_values()
    order = list(date.index)
    if args.max_year:
        order = [r for r in order if str(r)[:4].isdigit() and int(str(r)[:4]) <= args.max_year]
    if args.limit and len(order) > args.limit:
        order = order[-args.limit:]
    featured = featured.loc[order]

    gains = _floats(args.rank_gains)
    thresholds = _floats(args.thresholds)
    print(f"[rank_gain sweep] {len(order):,}レース / n_sim={args.n_sim} / gains={gains} / "
          f"thresholds={thresholds}")
    print("⚠ rank_bonus は leak（過去ROI探索・live 非transfer）。Tier1=ROI で採否、log-loss は参考。\n")
    print(f"{'gain':>6} | {'maxROI':>7} {'@t':>5} {'bets':>6} {'hit':>6} {'avgO':>6} "
          f"{'maxDD':>8} {'Shrp':>6} | {'LLsim':>7} {'LLmkt':>7} {'AUC':>6} {'Brier':>6}")
    print("-" * 96)

    rows = []
    for g in gains:
        races = _simulate_gain(featured, order, rank_gain=g, n_sim=args.n_sim, T=args.T,
                               ability_spread=args.ability_spread, ability_sigma=args.ability_sigma,
                               seed=args.seed)
        if not races:
            print(f"{g:>6.2f} | 有効レースなし")
            continue
        b = best_threshold(races, thresholds, min_odds=args.min_odds, max_odds=args.max_odds,
                           min_bets=args.min_bets)
        q = quality_metrics(races)
        rows.append((g, b, q))
        sh = f"{b['sharpe']:.3f}" if b["sharpe"] is not None else "  -  "
        auc = f"{q['auc']:.3f}" if q["auc"] is not None else "  -  "
        print(f"{g:>6.2f} | {b['roi']:>7.3f} {b['threshold']:>5.2f} {b['n_bets']:>6,} "
              f"{b['hit_rate']:>6.3f} {b['avg_odds']:>6.2f} {b['max_dd']:>8,.0f} {sh:>6} | "
              f"{q['logloss_sim']:>7.4f} {q['logloss_market']:>7.4f} {auc:>6} {q['brier']:>6.4f}")

    if rows:
        base = next((r for r in rows if r[0] == 0.0), rows[0])
        best = max(rows, key=lambda r: r[1]["roi"])
        print("-" * 96)
        print(f"基準 gain={base[0]:+.2f} maxROI={base[1]['roi']:.3f} / "
              f"最良 gain={best[0]:+.2f} maxROI={best[1]['roi']:.3f}"
              f"（Δ={best[1]['roi'] - base[1]['roi']:+.3f}）")
        print("判定: 最良 gain の maxROI が基準(gain=0)を有意に上回るなら『ランクは過去ROIを動かす』"
              "（ただし leak 値＝live には出ない）。ほぼ同じなら『既存特徴＋市場に織込み済み』の再確認。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
