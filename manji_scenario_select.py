"""卍①.5 Step4: 補正シナリオを OOS 回収率で選抜する（②学習＋baseline/placebo 対照）。

各シナリオで「①features ⊕ manji_score ⊕ 因子one-hot」を作り、発走日順 walk-forward で
② を学習 → EV=勝率×単勝オッズ で買い → OOS 単勝回収率を集計。manji 列を除いた baseline と、
manji_score を shuffle した placebo と比較し、lift（=scenario−baseline）降順で最良を選ぶ。

共有成果物（factor_table・block_posteriors）は1回だけ作って全シナリオで使い回す（重いのは
②再学習のみ）。最良シナリオ確定後、そのシナリオで本番 KeibaAI（較正込み）を1回学習する運用。

実行:
  # 既定の全シナリオを軽量LGBで評価（placebo 20回）
  python manji_scenario_select.py --folds 5 --placebo 20
  # 一部シナリオ・買い閾値・複勝決済
  python manji_scenario_select.py --scenarios value_jinba recent_form --ev-threshold 1.1
  python manji_scenario_select.py --bet-type fukusho   # payoffs.pkl の複勝で決済
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))


def main() -> None:
    from src.constants._logging_config import setup_logging
    setup_logging()

    ap = argparse.ArgumentParser(description="卍補正シナリオの OOS 回収率選抜")
    ap.add_argument("--scenarios", nargs="+", default=None, help="評価するシナリオ名（既定=全部）")
    ap.add_argument("--limit", type=int, default=None, metavar="N",
                    help="直近Nレースに絞る（大規模データのメモリ/時間対策。まず小さく試すのを推奨）")
    ap.add_argument("--folds", type=int, default=5)
    ap.add_argument("--n-blocks", type=int, default=8, help="as-of 事後の時系列ブロック数")
    ap.add_argument("--ev-threshold", type=float, default=1.0, help="買う EV 閾値（prob×オッズ）")
    ap.add_argument("--placebo", type=int, default=20, help="manji_score shuffle の試行数")
    ap.add_argument("--bet-type", choices=["tansho", "fukusho"], default="tansho")
    ap.add_argument("--payoffs", default=None, help="複勝決済用 payoffs.pkl（--bet-type fukusho）")
    ap.add_argument("--tune-lgb", type=int, default=0, metavar="N",
                    help="①.5が十分進んだ後、最良シナリオで②LightGBMのOptuna探索をN試行"
                         "（既定100推奨）。各trialは早期停止つき、Optuna枝刈り(MedianPruner)あり")
    ap.add_argument("--early-stopping", type=int, default=50,
                    help="②学習の early_stopping_rounds（Early termination）")
    ap.add_argument("--num-boost-round", type=int, default=1000)
    ap.add_argument("--min-adopted", type=int, default=20,
                    help="①.5『十分進んだ』判定: 最新ブロックの採用バケット数の下限")
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    import pandas as pd

    from app._model_eval import load_featured_data
    featured = load_featured_data()
    if featured is None or featured.empty:
        print("featured_data がありません（先に rebuild-featured を実行）")
        return
    total_races = featured.index.astype(str).nunique()
    if args.limit and total_races > args.limit:
        race_date = pd.to_datetime(featured["date"], errors="coerce").groupby(level=0).first().sort_values()
        keep = set(str(r) for r in race_date.index[-args.limit:])
        featured = featured[featured.index.astype(str).isin(keep)]
        print(f"[--limit] 直近 {args.limit:,} レースに限定（全 {total_races:,} 中 / {len(featured):,} 行）")
    else:
        print(f"[data] {total_races:,} レース / {len(featured):,} 行")

    payoffs = None
    if args.bet_type == "fukusho":
        from src.constants._local_paths import LocalPaths
        from src.tuning._payoffs import load_payoffs, single_horse_payoff_lookup
        pp = args.payoffs or str(Path(LocalPaths.RAW_DIR) / "payoffs.pkl")
        payoffs = single_horse_payoff_lookup(load_payoffs(pp), "fukusho")
        if not payoffs:
            print(f"複勝払戻が空です（{pp}）。import_archive_odds.py で作成してください")
            return
        print(f"[--bet-type fukusho] 複勝払戻 {len(payoffs):,} 件をロード")

    # ①.5 共有成果物（factor_table・block_posteriors＝ベイズ更新）を1回だけ作る
    import time
    from src.tuning._manji_scenario import prepare_shared, scenario_factor_union
    fu = scenario_factor_union(args.scenarios)
    print(f"①.5 共有成果物を生成中（{len(fu)} 因子 / {args.n_blocks} ブロック）...", flush=True)
    t0 = time.time()
    shared = prepare_shared(featured, factor_names=fu, n_blocks=args.n_blocks, progress=True)
    print(f"①.5 完了（{time.time() - t0:.0f}s）。②評価を開始...", flush=True)

    from src.tuning._manji_scenario_eval import evaluate_scenarios
    t1 = time.time()
    table = evaluate_scenarios(
        featured, args.scenarios, shared=shared, n_blocks=args.n_blocks, folds=args.folds,
        ev_threshold=args.ev_threshold, payoffs=payoffs, n_placebo=args.placebo, seed=args.seed,
    )
    print(f"②評価 完了（{time.time() - t1:.0f}s）", flush=True)

    print("\n" + "=" * 78)
    print("卍補正シナリオ OOS 選抜（lift 降順 / 券種=%s）" % args.bet_type)
    print("-" * 78)
    print(f"  {'シナリオ':<16}{'回収率':>8}{'baseline':>10}{'lift':>8}{'placebo%':>10}{'買い目':>8}")
    for _, r in table.iterrows():
        star = " ★" if (r["lift"] > 0 and r["placebo_pct"] >= 0.95) else ""
        print(f"  {r['scenario']:<16}{r['roi']:>8.3f}{r['baseline_roi']:>10.3f}"
              f"{r['lift']:>+8.3f}{r['placebo_pct']:>10.2f}{int(r['n_bets']):>8}{star}")
    print("=" * 78)
    best = table.iloc[0]
    print(f"\n最良シナリオ: {best['scenario']}（lift {best['lift']:+.3f} / placebo%% {best['placebo_pct']:.2f}）")
    if best["lift"] <= 0 or best["placebo_pct"] < 0.95:
        print("※ lift>0 かつ placebo%%≥0.95 を満たさず。manji 補正の OOS 寄与は有意でない可能性。")

    # ①.5 のベイズ更新が十分に進んだ後、最良シナリオで ② の Optuna 探索を実施
    if args.tune_lgb > 0:
        from src.tuning._manji_scenario import SCENARIOS, build_scenario_training_data
        from src.tuning._manji_scenario_eval import posterior_ready, tune_lgb_optuna
        _, block_posteriors = shared
        if not posterior_ready(block_posteriors, min_adopted=args.min_adopted):
            print(f"\n※ ①.5 のベイズ更新がまだ十分でない（採用バケット < {args.min_adopted}）。"
                  "② Optuna はスキップ。データ期間を延ばすか --min-adopted を下げてください。")
        else:
            scn = SCENARIOS[best["scenario"]]
            sdf = build_scenario_training_data(
                featured, scn, factor_table=shared[0], block_posteriors=block_posteriors)
            print(f"\n② LightGBM Optuna 探索: {args.tune_lgb} 試行 / early_stopping="
                  f"{args.early_stopping} / MedianPruner（最良シナリオ {best['scenario']} 上）...")
            res = tune_lgb_optuna(
                sdf, n_trials=args.tune_lgb, early_stopping_rounds=args.early_stopping,
                num_boost_round=args.num_boost_round, folds=args.folds,
                ev_threshold=args.ev_threshold, payoffs=payoffs, seed=args.seed,
            )
            print(f"  完了: OOS回収率 {res['value']:.3f} / 枝刈り {res['n_pruned']}/{res['n_trials']} 試行")
            print(f"  best_params: {res['best_params']}")
            print("次段: この best_params で本番 KeibaAI（較正込み）を1回学習 → ③EV → ④選定/サイジング。")
            return
    print("次段: 最良シナリオで本番 KeibaAI（較正込み）を1回学習 → ③EV → ④選定/サイジング。")


if __name__ == "__main__":
    main()
