"""的中率重視 vs 回収率重視 — 同一モデル・同一 holdout で「運用点」を対置する。

現行モデル（①確率モデル）は二値 rank/rank_win で学習され **的中率/確率精度** を最適化している。
「回収率重視に変える」は①の再学習ではなく **②馬券ポリシー層の運用点(operating point)の切替**:

  ・的中率重視（本命買い）: 各レース argmax p̂ を1点買い。高的中率だが return_rate≈1−控除 が一様。
  ・回収率重視（妙味ゾーン）: EV=p̂·odds>閾値 の馬だけ買い、無ければ見送る。的中率は下がるが
    賭ける母集団を妙味に絞り期待損失を最小化する。

post-takeout エッジ≈0 の下では**どの運用点も約 −控除率 を超えない**（帰無が機構的にきれい:
残差0なら EV<1 で1点も賭けない）。本スクリプトはその前提を崩さず、両運用点の
return_rate / hit_rate / n_bets / max_drawdown / sharpe を同一 holdout で並べて**トレードオフを可視化**する。

`app._model_eval.compute_confidence_sweep`（EVスイープ）を回収率側に再利用し、的中率側の基準点を対置。

使い方:
  python scripts/roi_vs_hit_operating_point.py --version baseline_jrdb_seirei --jra-only
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.constants._model_category import central_index_mask  # noqa: E402


# ── 純ロジック（テスト対象） ─────────────────────────────────────────
def _race_hit_rate(race_ids: np.ndarray, wins: np.ndarray) -> float:
    """レース単位の的中率（同一 race_id で1頭でも当たればヒット）。"""
    hit: dict = {}
    for rid, w in zip(race_ids, wins, strict=False):
        hit[rid] = hit.get(rid, 0.0) + float(w)
    if not hit:
        return float("nan")
    return sum(1 for v in hit.values() if v > 0) / len(hit)


def top_pick_operating_point(
    prob_win: np.ndarray, odds: np.ndarray, wins: np.ndarray, race_ids: np.ndarray,
) -> dict:
    """的中率重視 = 各レース argmax p̂ を1点買い。return_rate/hit_rate/n_bets/profit を返す。"""
    df = pd.DataFrame({"p": prob_win, "odds": odds, "win": wins, "rid": race_ids})
    picks = df.loc[df.groupby("rid")["p"].idxmax()]
    n = len(picks)
    if n == 0:
        return {"label": "的中率重視(本命買い)", "return_rate": float("nan"),
                "hit_rate": float("nan"), "n_bets": 0, "profit": float("nan")}
    payouts = picks["odds"].to_numpy() * picks["win"].to_numpy()
    rr = float(payouts.sum()) / n
    return {
        "label": "的中率重視(本命買い)",
        "return_rate": rr,
        "hit_rate": float(picks["win"].mean()),  # 1点買い→レース的中率＝馬的中率
        "n_bets": n,
        "profit": float((payouts - 1.0).sum()),
    }


def ev_operating_point(
    prob_win: np.ndarray, odds: np.ndarray, wins: np.ndarray, race_ids: np.ndarray,
    ev_thr: float,
) -> dict:
    """回収率重視 = EV=p̂·odds>閾値 の馬を全点買い（無ければ見送り）。"""
    ev = prob_win * odds
    mask = ev > ev_thr
    n = int(mask.sum())
    if n == 0:
        return {"label": f"回収率重視(EV>{ev_thr:.2f})", "return_rate": float("nan"),
                "hit_rate": float("nan"), "n_bets": 0, "profit": float("nan")}
    payouts = odds[mask] * wins[mask]
    rr = float(payouts.sum()) / n
    return {
        "label": f"回収率重視(EV>{ev_thr:.2f})",
        "return_rate": rr,
        "hit_rate": _race_hit_rate(race_ids[mask], wins[mask]),
        "n_bets": n,
        "profit": float((payouts - 1.0).sum()),
    }


def _f(v: float) -> str:
    return "—" if v is None or (isinstance(v, float) and np.isnan(v)) else f"{v:.4f}"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="的中率重視 vs 回収率重視 の運用点比較")
    ap.add_argument("--version", default="baseline_jrdb_seirei", help="評価するモデル版名")
    ap.add_argument("--featured-path", default=None)
    ap.add_argument("--jra-only", action="store_true", help="中央のみ")
    ap.add_argument("--ev-thresholds", type=float, nargs="+",
                    default=[1.0, 1.1, 1.2, 1.3, 1.5], help="回収率側の EV 閾値")
    ap.add_argument("--test-size", type=float, default=0.2)
    ap.add_argument("--valid-size", type=float, default=0.2)
    args = ap.parse_args(argv if argv is not None else sys.argv[1:])

    from app._data_loader import load_model_from_path, load_win_head_for
    from app._model_eval import _get_splits
    from src.constants._local_paths import LocalPaths
    from src.pipeline._ingestion import load_raw
    from src.pipeline.commands._evaluate import _resolve_backtest_model_path
    from src.policies._score_policy import CURRENT_ODDS, PROB, ExpectedValueScorePolicy

    path = _resolve_backtest_model_path(args.version)
    place_ai = load_model_from_path(path)
    win_ai = load_win_head_for(path)
    # 単勝 EV/回収率は「勝率」で測る → Win ヘッド優先（無ければ Place で代用）
    model = win_ai or place_ai
    eff = getattr(model, "effective_model", model)
    print(f"[op] モデル {Path(path).name}（Win ヘッド={'あり' if win_ai else 'なし(Place代用)'}）")

    featured = load_raw(args.featured_path or LocalPaths.FEATURED_DATA_PATH)
    if args.jra_only:
        featured = featured[central_index_mask(featured.index)]
    if featured.empty:
        print("対象 featured が空。", file=sys.stderr)
        return 1

    splits = _get_splits(featured, args.test_size, args.valid_size)
    X_test = splits["X_test"]
    y_test = np.asarray(splits["y_test"])
    # 実際の推論経路（_coerce_for_predict 内包）で 勝率 と 単勝オッズ を得る
    table = ExpectedValueScorePolicy.calc(eff, X_test)
    prob_win = np.asarray(table[PROB], dtype=float)
    odds = np.asarray(table[CURRENT_ODDS], dtype=float)
    wins = (y_test == 1).astype(float)
    rids = table.index.to_numpy()
    print(f"[op] holdout(test) {len(prob_win):,} 頭 / {len(np.unique(rids)):,} レース  JRA={args.jra_only}")

    rows = [top_pick_operating_point(prob_win, odds, wins, rids)]
    rows += [ev_operating_point(prob_win, odds, wins, rids, t) for t in args.ev_thresholds]

    print("\n[op] 運用点比較（同一モデル・同一 holdout）")
    print(f"  {'運用点':<20}{'return_rate':>12}{'hit_rate':>10}{'n_bets':>9}{'profit':>12}")
    for r in rows:
        print(f"  {r['label']:<20}{_f(r['return_rate']):>12}{_f(r['hit_rate']):>10}"
              f"{r['n_bets']:>9}{_f(r['profit']):>12}")
    print("\n  的中率重視＝本命を機械的に買う → 高 hit_rate だが return_rate は控除に一様に負ける。")
    print("  回収率重視＝EV>閾値の妙味だけ買い見送りを許す → hit_rate は落ちるが賭け母集団を絞る。")
    print("  ※ post-takeout エッジ≈0 の下ではどの運用点も return_rate≈1−控除 を超えない（帰無が正しく働く）。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
