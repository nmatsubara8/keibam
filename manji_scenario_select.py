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
    ap.add_argument("--folds", type=int, default=5)
    ap.add_argument("--n-blocks", type=int, default=8, help="as-of 事後の時系列ブロック数")
    ap.add_argument("--ev-threshold", type=float, default=1.0, help="買う EV 閾値（prob×オッズ）")
    ap.add_argument("--placebo", type=int, default=20, help="manji_score shuffle の試行数")
    ap.add_argument("--bet-type", choices=["tansho", "fukusho"], default="tansho")
    ap.add_argument("--payoffs", default=None, help="複勝決済用 payoffs.pkl（--bet-type fukusho）")
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    from app._model_eval import load_featured_data
    featured = load_featured_data()
    if featured is None or featured.empty:
        print("featured_data がありません（先に rebuild-featured を実行）")
        return

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

    from src.tuning._manji_scenario_eval import evaluate_scenarios
    table = evaluate_scenarios(
        featured, args.scenarios, n_blocks=args.n_blocks, folds=args.folds,
        ev_threshold=args.ev_threshold, payoffs=payoffs, n_placebo=args.placebo, seed=args.seed,
    )

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
    print("次段: 最良シナリオで本番 KeibaAI（較正込み）を1回学習 → ③EV → ④選定/サイジング。")


if __name__ == "__main__":
    main()
