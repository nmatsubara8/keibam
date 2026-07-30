"""複勝本命(損失最小)ポリシーの運用点スイープ — min_score(見送り閾値)を実測で最適化する。

「回収率重視＝損失最小化」の実装解の運用点を確定する。mainline のバックテスト経路
(app._model_compare.simulate_model → BetPolicyFukushoHonmei)をそのまま使い、
StdScorePolicy の z-score 閾値 min_score を振って return_rate/hit_rate/n_bets を測る。
min_score↑で見送りが増え、盲目複勝(~0.80)から損失最小(~0.90)へ改善するはずの運用曲線を出す。

注: 払戻照合は ReturnProcessor(netkeiba 払戻テーブル)。JRDB SED 由来の検証済み ~0.90 とは
別ソースなので、n_covered が低ければ払戻データ欠損を疑う（診断に出す）。

使い方:
  python scripts/loss_min_operating_point.py --version baseline_jrdb_seirei --jra-only
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.constants._model_category import central_index_mask  # noqa: E402


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="複勝本命(損失最小) 運用点スイープ")
    ap.add_argument("--version", default="baseline_jrdb_seirei")
    ap.add_argument("--featured-path", default=None)
    ap.add_argument("--jra-only", action="store_true")
    ap.add_argument("--test-frac", type=float, default=0.2)
    ap.add_argument("--min-scores", type=float, nargs="+",
                    default=[-1.0, -0.5, 0.0, 0.5, 1.0, 1.5, 2.0], help="見送り z 閾値の候補")
    args = ap.parse_args(argv if argv is not None else sys.argv[1:])

    from app._data_loader import load_model_from_path, load_win_head_for
    from app._model_compare import recent_race_slice, simulate_model
    from src.constants._local_paths import LocalPaths
    from src.pipeline._ingestion import load_raw
    from src.pipeline.commands._evaluate import _resolve_backtest_model_path

    path = _resolve_backtest_model_path(args.version)
    place_ai = load_model_from_path(path)
    win_ai = load_win_head_for(path)
    ai = win_ai or place_ai   # place(top3)確率で本命を選ぶ。Win ヘッドがあれば併用可
    print(f"[opt] モデル {Path(path).name}")

    featured = load_raw(args.featured_path or LocalPaths.FEATURED_DATA_PATH)
    if args.jra_only:
        featured = featured[central_index_mask(featured.index)]
    holdout = recent_race_slice(featured, args.test_frac)
    if holdout.empty:
        print("holdout が空。", file=sys.stderr)
        return 1
    print(f"[opt] holdout {len(holdout):,} 頭 / {holdout.index.nunique():,} レース\n")

    label = "複勝本命(損失最小)"
    print(f"  {'min_score':>10}{'return_rate':>12}{'hit_rate':>10}{'n_bets':>9}{'n_covered':>11}")
    best = (None, -1.0)
    for ms in args.min_scores:
        summary, per_race, diag = simulate_model(ai, holdout, label, ms)
        rr = float(summary.get("return_rate", float("nan")))
        hr = float(summary.get("hit_rate", float("nan")))
        nb = diag.get("n_matched_races", 0)
        nc = diag.get("n_covered_races", 0)
        print(f"  {ms:>10.2f}{rr:>12.4f}{hr:>10.4f}{nb:>9}{nc:>11}")
        if nc >= 100 and rr > best[1]:
            best = (ms, rr)
    print("\n  min_score↑で見送り増→回収率が控除損を圧縮（損失最小化）。top_n=1 が最も損失小。")
    if best[0] is not None:
        print(f"  実測の損失最小運用点: min_score={best[0]:.2f} で return_rate={best[1]:.4f}"
              f"（n_covered≥100 の範囲）。これを ②ポリシー層の既定閾値に固定推奨。")
    print("  ※ ROI<1 は不変（公開データにエッジ無し）。本ポリシーは負けを最小化する運用点。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
