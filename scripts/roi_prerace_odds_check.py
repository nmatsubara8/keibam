"""前売りオッズ選定 vs 最終オッズ選定 — 「回収率>1」が最終オッズ先読みの産物かを判定する。

roi_vs_hit_operating_point.py で EV=p̂·(最終単勝) 選択が return_rate>1（利益は中穴〜大穴帯・
p̂プラセボで消える＝モデル依存）を示した。ただし選定にも精算にも**最終確定オッズ**を使っており、
実運用では「賭けるのは締切前」＝bet時オッズ≠最終オッズ。長オッズ帯ほどドリフトが大きく、
最終オッズで EV>1 を選ぶと「締切後に伸びた馬」を後付けで拾う先読みバイアスが乗りうる。

本スクリプトは JRDB **OZ 前売り単勝**を使い、同一レース集合で:
  ・EV_final = p̂ · 最終単勝  で選択（先読みあり＝従来）
  ・EV_pre   = p̂ · OZ前売り  で選択（bet時に実在した情報のみ）
の2通りを走らせ、**精算は常に最終単勝**（pari-mutuel は確定オッズ払戻）で行う。
EV_pre が <1 に崩れれば、その ROI は先読み由来＝実運用では消える。EV_pre でも >1 が残れば
「締切前に取れる情報でも市場に勝つ」＝要さらに OOS 年またぎ検証、の本物候補。

使い方（OZ .txt を展開したフォルダを指定）:
  python scripts/roi_prerace_odds_check.py --version baseline_jrdb_seirei \
      --jra-only --odds-dir data/jrdb_txt/oz_check
"""
from __future__ import annotations

import argparse
import glob
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.constants._model_category import central_index_mask  # noqa: E402
from src.jrdb._odds import parse_odds  # noqa: E402


# ── 純ロジック（テスト対象） ─────────────────────────────────────────
def build_prerace_records(
    pred_by_key: dict, oz_tansho_by_race: dict,
) -> list[dict]:
    """test 予測（key=(race_id,馬番)→(p̂,最終odds,won)）と OZ 前売り単勝を突き合わせる。

    OZ に存在し予測にも存在する馬だけを残す（＝OZ 被覆レースの評価対象）。
    """
    recs = []
    for rid, tan in oz_tansho_by_race.items():
        for uma, o_pre in tan.items():
            key = (str(rid), int(uma))
            if key not in pred_by_key or not o_pre or o_pre <= 0:
                continue
            p, o_final, won = pred_by_key[key]
            if not o_final or o_final <= 0:
                continue
            recs.append({"rid": str(rid), "uma": int(uma), "p": float(p),
                         "o_final": float(o_final), "o_pre": float(o_pre), "won": float(won)})
    return recs


def ev_settle(recs: list[dict], sel_key: str, ev_thr: float) -> dict:
    """sel_key（'o_final' or 'o_pre'）で EV=p̂·odds_sel>閾値 を選び、精算は常に最終単勝。

    return_rate / hit_rate / n_bets / profit を返す。
    """
    bets = [r for r in recs if r["p"] * r[sel_key] > ev_thr]
    n = len(bets)
    if n == 0:
        return {"return_rate": float("nan"), "hit_rate": float("nan"), "n_bets": 0,
                "profit": float("nan")}
    payouts = np.array([r["o_final"] * r["won"] for r in bets])
    return {"return_rate": float(payouts.sum()) / n,
            "hit_rate": float(np.mean([r["won"] for r in bets])),
            "n_bets": n, "profit": float((payouts - 1.0).sum())}


def _f(v) -> str:
    return "—" if v is None or (isinstance(v, float) and np.isnan(v)) else f"{v:.4f}"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="前売り選定 vs 最終選定（回収率>1 の先読み検証）")
    ap.add_argument("--version", default="baseline_jrdb_seirei")
    ap.add_argument("--featured-path", default=None)
    ap.add_argument("--jra-only", action="store_true")
    ap.add_argument("--odds-dir", default="data/jrdb_txt/oz_check",
                    help="OZ の .txt を置いたフォルダ")
    ap.add_argument("--ev-thresholds", type=float, nargs="+", default=[1.0, 1.1, 1.2, 1.3, 1.5])
    ap.add_argument("--test-size", type=float, default=0.2)
    args = ap.parse_args(argv if argv is not None else sys.argv[1:])

    from app._data_loader import load_model_from_path, load_win_head_for
    from app._model_eval import _split_by_date
    from src.constants._local_paths import LocalPaths
    from src.constants._results_cols import ResultsCols
    from src.pipeline._ingestion import load_raw
    from src.pipeline.commands._evaluate import _resolve_backtest_model_path
    from src.policies._score_policy import CURRENT_ODDS, PROB, ExpectedValueScorePolicy

    path = _resolve_backtest_model_path(args.version)
    place_ai = load_model_from_path(path)
    win_ai = load_win_head_for(path)
    eff = getattr(win_ai or place_ai, "effective_model", win_ai or place_ai)
    print(f"[pre] モデル {Path(path).name}（Win ヘッド={'あり' if win_ai else 'なし(Place代用)'}）")

    featured = load_raw(args.featured_path or LocalPaths.FEATURED_DATA_PATH)
    if args.jra_only:
        featured = featured[central_index_mask(featured.index)]
    _, test = _split_by_date(featured, args.test_size)
    table = ExpectedValueScorePolicy.calc(eff, test)
    won = (pd.to_numeric(test[ResultsCols.RANK], errors="coerce").to_numpy() == 1).astype(float)
    rids = table.index.astype(str).to_numpy()
    umas = pd.to_numeric(table[ResultsCols.UMABAN], errors="coerce").to_numpy()
    probs = np.asarray(table[PROB], dtype=float)
    finals = np.asarray(table[CURRENT_ODDS], dtype=float)
    pred_by_key = {(rids[i], int(umas[i])): (probs[i], finals[i], won[i])
                   for i in range(len(rids)) if not np.isnan(umas[i])}
    print(f"[pre] holdout(test) 予測 {len(pred_by_key):,} 頭 / {len(set(rids)):,} レース")

    files = sorted(glob.glob(f"{args.odds_dir}/OZ*.txt") + glob.glob(f"{args.odds_dir}/oz*.txt"))
    if not files:
        print(f"OZ の .txt が見つかりません（{args.odds_dir}/OZ*.txt）。", file=sys.stderr)
        return 1
    oz_tansho: dict[str, dict[int, float]] = {}
    for fp in files:
        long_df = parse_odds(fp, "OZ")
        for rid, g in long_df.groupby("race_id"):
            tan = {int(r.combo): float(r.odds) for r in g[g.bet == "tansho"].itertuples()
                   if r.odds and float(r.odds) > 0}
            if tan:
                oz_tansho[str(rid)] = tan
    print(f"[pre] OZ 前売り {len(oz_tansho):,} レース（{len(files)} ファイル）")

    recs = build_prerace_records(pred_by_key, oz_tansho)
    cov_races = len({r["rid"] for r in recs})
    if not recs:
        print("OZ と holdout の race_id が重複せず評価不能（OZ の期間が holdout と別かも）。",
              file=sys.stderr)
        return 1
    print(f"[pre] 評価対象（OZ×holdout 突合）: {len(recs):,} 頭 / {cov_races:,} レース\n")

    print("[pre] 前売り選定 vs 最終選定（同一レース集合・精算は常に最終単勝）")
    print(f"  {'閾値':<8}{'選定':<12}{'return_rate':>12}{'hit_rate':>10}{'n_bets':>9}{'profit':>12}")
    for t in args.ev_thresholds:
        rf = ev_settle(recs, "o_final", t)
        rp = ev_settle(recs, "o_pre", t)
        print(f"  EV>{t:<5.2f}{'最終(先読)':<12}{_f(rf['return_rate']):>12}{_f(rf['hit_rate']):>10}"
              f"{rf['n_bets']:>9}{_f(rf['profit']):>12}")
        print(f"  {'':<8}{'前売り':<12}{_f(rp['return_rate']):>12}{_f(rp['hit_rate']):>10}"
              f"{rp['n_bets']:>9}{_f(rp['profit']):>12}")
    print("\n  判定: 前売りでも return_rate>1 が残る → bet時情報で市場に勝つ本物候補（要 年またぎOOS）。")
    print("        前売りで <1 に崩れる → ROI は最終オッズ先読みの産物＝実運用では消える。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
