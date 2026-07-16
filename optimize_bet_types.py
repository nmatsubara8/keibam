"""券種別 EV パラメータの Optuna(TPE) 最適化（規律版）— 実 featured + return_tables で実走。

Phase 2。券種ごとに EV 閾値 × 温度 × 確率較正(prob_scale) を Optuna(TPE) で探索する。

規律（本プロジェクトの一貫方針）: in-sample の生 ROI 最大化は**万馬券の分散に過適合**するため、
  1. 時系列 train/val 分割（train で最適化・val で汎化確認）、
  2. 頑健目的（既定 trimmed_return_rate＝最大払戻1本を除いた回収率／sharpe も可）、
  3. **val で既定パラメータを out-of-sample で上回った券種のみ採用**（上回らなければ過適合＝既定に戻す）
で判断する。市場エッジは存在しない（§10）前提の、リスク管理（どの買い目を捨てるか）の最適化。

実行例:
  python optimize_bet_types.py --since-year 2016 --n-trials 80
  python optimize_bet_types.py --objective sharpe_ratio --bet-types umaren wide --save
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))


def main():
    ap = argparse.ArgumentParser(description="券種別 EV パラメータの Optuna 最適化（時系列val・頑健目的）")
    ap.add_argument("--version", default=None, help="モデルバージョン（既定=最新）")
    ap.add_argument("--since-year", type=int, default=None, help="指定年以降の featured のみ使用（メモリ節約）")
    ap.add_argument("--no-rating-features", action="store_true",
                    help="retrain --no-rating-features のモデルと列を一致（Elo 9列を featured から除外）")
    ap.add_argument("--bet-types", nargs="*", default=None, help="対象券種（既定=最適化対象全券種）")
    ap.add_argument("--n-trials", type=int, default=60)
    ap.add_argument("--objective", default="trimmed_return_rate",
                    choices=["trimmed_return_rate", "sharpe_ratio", "return_rate"])
    ap.add_argument("--min-bets", type=int, default=30, help="train でこの買い目数未満の点は忌避")
    ap.add_argument("--val-frac", type=float, default=0.3)
    ap.add_argument("--max-races", type=int, default=None,
                    help="探索を直近 N レースに限定（3連系の組合せ爆発を抑え高速化）。例: --max-races 6000")
    ap.add_argument("--takeout", type=float, default=0.2)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--save", action="store_true",
                    help="採用（val で既定超え）した券種の best を models/bet_type_params.json に保存")
    args = ap.parse_args()

    import pandas as pd

    from app._bet_type_optimizer import BET_TYPE_LABELS
    from app._bet_type_optimizer import optimize_bet_type_tpe
    from app._data_loader import list_model_versions
    from app._data_loader import load_model_by_version
    from app._model_eval import _load_return_processor
    from app._model_eval import load_featured_data
    from src.policies._bet_type_params import OPTIMIZABLE_BET_TYPES
    from src.policies._bet_type_params import bet_type_params_path
    from src.policies._bet_type_params import default_params
    from src.policies._bet_type_params import save_bet_type_params

    featured = load_featured_data()
    if featured is None or featured.empty:
        print("featured_data がありません")
        return
    if args.since_year:
        yr = pd.to_numeric(featured.index.astype(str).str[:4], errors="coerce")
        before = len(featured)
        featured = featured[yr >= args.since_year]
        print(f"[since-year] {args.since_year}: {before:,}→{len(featured):,} 行")

    # --no-rating-features: retrain --no-rating-features で学習したモデル（Elo 9列を除いた列数）と
    # featured の列を一致させる（backtest --no-rating-features と同じ列揃え）。付けないと Elo 込み
    # featured を Elo 抜きモデルに食わせて LightGBM の feature 数不一致で落ちる。
    if args.no_rating_features:
        from src.constants._feature_cols import ELO_FEATURE_COLS
        rating_cols = ELO_FEATURE_COLS + [f"{c}_z" for c in ELO_FEATURE_COLS]
        present = [c for c in rating_cols if c in featured.columns]
        featured = featured.drop(columns=present, errors="ignore")
        print(f"[no-rating-features] Elo 由来 {len(present)} 列を除外: {present}")

    version = args.version
    if version is None:
        vs = list_model_versions()
        if not vs:
            print("モデルが無い。retrain を先に実行してください。")
            return
        version = vs[0].get("version") or vs[0].get("version_name")
    ai = load_model_by_version(version)
    rp = _load_return_processor()

    targets = args.bet_types or list(OPTIMIZABLE_BET_TYPES)
    print(f"[model] {version} / objective={args.objective} / n_trials={args.n_trials} / "
          f"min_bets={args.min_bets} / val_frac={args.val_frac}")
    print("=" * 84)
    print("規律: val(最適化) が val(既定) を out-of-sample で上回った券種のみ採用（他は既定に据置）")
    print("-" * 84)
    print(f"{'券種':<8}{'train':>10}{'val(最適)':>12}{'val(既定)':>12}{'Δval':>9}  採用  best(ev/temp/scale)")
    print("-" * 84)

    params_map: dict = {}
    metrics_map: dict = {}
    adopted = 0
    for bt in targets:
        res = optimize_bet_type_tpe(
            ai, featured, rp, bt,
            n_trials=args.n_trials, objective=args.objective, min_bets=args.min_bets,
            val_frac=args.val_frac, max_races=args.max_races, takeout=args.takeout, seed=args.seed,
        )
        label = BET_TYPE_LABELS.get(bt, bt)
        bp = res.get("best_params")
        if bp is None:
            params_map[bt] = default_params(bt)
            metrics_map[bt] = {}
            print(f"{label:<8}{'—':>10}{'—':>12}{'—':>12}{'—':>9}  既定  (min_bets 未達で探索不成立)")
            continue
        tr, vo, vd = res["train_metric"], res["val_metric"], res["val_metric_default"]
        beats = vo > vd                                  # out-of-sample で既定超えか
        mark = "✓採用" if beats else "×既定"
        if beats:
            params_map[bt] = bp
            adopted += 1
        else:
            params_map[bt] = default_params(bt)          # 過適合＝既定に戻す
        metrics_map[bt] = res.get("val_summary", {})
        print(f"{label:<8}{tr:>10.3f}{vo:>12.3f}{vd:>12.3f}{vo - vd:>+9.3f}  {mark}  "
              f"({bp.ev_threshold:.2f}/{bp.temperature:.2f}/{bp.prob_scale:.2f})")

    print("-" * 84)
    print(f"採用 {adopted}/{len(targets)} 券種（val で既定を上回ったもののみ）。"
          "0 なら『最適化は out-of-sample で効かず＝過適合』が結論。")
    print(f"※目的={args.objective}・val は held-out。市場エッジの話ではなくリスク管理（買い目選別）の最適化。")
    if args.save:
        path = bet_type_params_path("models")
        save_bet_type_params(params_map, path, objective=args.objective, metrics=metrics_map)
        print(f"保存: {path}（採用券種は最適化値・非採用は既定値）")
    else:
        print("※ --save 未指定のため保存せず（判定のみ）。")
    print("=" * 84)


if __name__ == "__main__":
    main()
