"""JRDB42 の **in-sample 参考値**（development 2015-2024・rolling-origin CV・非証拠）。

⚠ これは confirmation ではない。development_known(2015-2024) の**内部** rolling-origin CV で、
JRDB41 特徴が market 直交の signal を持つかの**当たり**を見るだけ。selection の域を出ず、researcher
過適合を含みうる。**独立一般化証拠にはしない**（それは JRDB42_RESIDUAL_2027_CONFIRM が 2027 で一度だけ）。

leak 安全: 各 fold は train=[2015, eval_year) / test=eval_year（過去→未来のみ・同年混在なし）。
2025 以降は一切使わない（`assert_selection_only_on_known` で fail-closed）。特徴/L2 は凍結仕様と同一
（features=41・l2=1.0）。ΔNLL は nats/race。featured はローカルの完全 augment 成果物。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.run_jrdb42_confirm import FROZEN  # noqa: E402


def rolling_folds(years, first_eval_year):
    """development_known 内の rolling-origin fold 列を返す [(train_years, eval_year), ...]。

    train=[2015, eval_year) の全既知年、test=eval_year。**全 fold で eval_year<=2024**（selection 域）。
    純関数（年集合のみ）。
    """
    ys = sorted({int(y) for y in years if 2015 <= int(y) <= 2024})
    folds = []
    for ey in ys:
        if ey < first_eval_year:
            continue
        tr = [y for y in ys if y < ey]
        if tr:
            folds.append((tr, ey))
    return folds


def main() -> int:
    import numpy as np
    from scripts.run_residual_head_2027 import _load_featured, fit_and_eval
    from src.simulation._model_compare import block_bootstrap_ci
    from src.training._temporal_split import assert_selection_only_on_known

    ap = argparse.ArgumentParser(
        description="JRDB42 in-sample 参考値（development 2015-2024 rolling-origin・非証拠）")
    ap.add_argument("--featured", default=None)
    ap.add_argument("--first-eval-year", type=int, default=2018,
                    help="rolling-origin の最初の評価年（既定2018＝2015-2017 を最小 train に）")
    ap.add_argument("--n-boot", type=int, default=5000)
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    print("=" * 84)
    print("JRDB42 in-sample 参考値（development 2015-2024・rolling-origin CV・⚠非証拠/selection域）")
    try:
        feat = _load_featured(args.featured)
    except RuntimeError as e:
        print(f"[エラー] {e}", file=sys.stderr)
        return 2
    from scripts.run_jrdb42_confirm import _build_records
    records, feat_cols = _build_records(feat)

    # 2025 以降を completely 排除（selection 域のみ）。record 年で fail-closed。
    dev_records = [r for r in records if r["year"] and 2015 <= int(r["year"]) <= 2024]
    used_years = sorted({int(r["year"]) for r in dev_records})
    try:
        assert_selection_only_on_known(used_years)
    except ValueError as e:
        print(f"[STOP] selection 域外の年が混入: {e}", file=sys.stderr)
        return 6
    missing = [c for c in FROZEN["features"] if c not in feat.columns]
    if missing:
        hint = ("（jrdb_ms_* は MySpeed＝jrdb_build_features.py に **--with-myspeed** を付けて再 build）"
                if any(str(c).startswith("jrdb_ms_") for c in missing)
                else "（完全 augment build を先に実行）")
        print(f"[STOP] 凍結特徴が featured に未実体化 {len(missing)} 列{hint}: {missing[:8]}",
              file=sys.stderr)
        return 3

    folds = rolling_folds(used_years, args.first_eval_year)
    if not folds:
        print(f"[STOP] fold が作れない（used_years={used_years}）。", file=sys.stderr)
        return 3
    print(f"[設定] features={len(FROZEN['features'])} l2={FROZEN['l2']} "
          f"folds={[(f'{min(tr)}-{max(tr)}', ey) for tr, ey in folds]}")

    all_dnll, all_blocks, fold_means = [], [], []
    print(f"\n  {'eval年':>6}{'train年数':>9}{'test races':>11}{'ΔNLL(nats)':>13}{'95%CI':>26}")
    for tr_years, ey in folds:
        train = [r for r in dev_records if int(r["year"]) in set(tr_years)]
        test = [r for r in dev_records if int(r["year"]) == ey]
        if not test:
            continue
        res = fit_and_eval(train, test, feat_cols, l2=FROZEN["l2"], n_boot=args.n_boot, seed=args.seed)
        bb = res["bb"]
        all_dnll.extend(res["dnll"]); all_blocks.extend(res["blocks"])
        fold_means.append(float(bb["mean"]))
        print(f"  {ey:>6}{len(tr_years):>9}{res['n_dnll']:>11,}{bb['mean']:>+13.6f}"
              f"  [{bb['lo']:+.5f},{bb['hi']:+.5f}]")

    if not all_dnll:
        print("[STOP] 評価 race が無い。", file=sys.stderr)
        return 3
    pooled = block_bootstrap_ci(all_dnll, all_blocks, n_boot=max(2000, args.n_boot), seed=args.seed)
    n_neg = sum(1 for m in fold_means if m < 0)
    print(f"\n[pooled 参考値] rolling-origin 全 fold プール（venue×日 block・非証拠）")
    print(f"  ΔNLL={pooled['mean']:+.6f} nats/race  95%CI[{pooled['lo']:+.6f},{pooled['hi']:+.6f}]  "
          f"p_improve={pooled.get('p_improve', float('nan')):.4f}  races={len(all_dnll):,}")
    print(f"  fold 平均の単純平均={float(np.mean(fold_means)):+.6f}  "
          f"改善(負)fold={n_neg}/{len(fold_means)}（プール推定を優先・fold 一致は安定性の目安）")

    print("\n" + "=" * 84)
    print("⚠ これは in-sample 参考値（selection 域・development 2015-2024）であり **独立証拠ではない**。")
    print("  過適合を含みうる。採否判定は JRDB42_RESIDUAL_2027_CONFIRM が 2027 で一度だけ（Holm m=2）。")
    print("  ここで良く見えても freeze 仕様は変えない（見て特徴/L2 を選び直すと汚染）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
