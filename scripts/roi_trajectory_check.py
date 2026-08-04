"""オッズ軌跡の realizable 検証 — 前売り(OZ)→直前(TYB)→最終 の選定を同一レースで対置する。

ユーザ提案「オッズ推移を追えば最終確率を予測でき、bet時に活かせる」を realizable に検証する。
最終オッズは bet前に手に入らないが、**TYB 直前オッズ（発走15分前・raw_jrdb_tyb）は bet前に観測可能**。
よって「最新観測（直前）で選定すると前売りより良いか？」を実データで測れる:

  ・EV_pre   = p̂ · OZ前売り   （早期・realizable）
  ・EV_tyb   = p̂ · TYB直前     （直前・realizable）← 軌跡を追う realizable 版の本命
  ・EV_final = p̂ · 最終単勝    （look-ahead・参照）
精算は常に最終単勝（pari-mutuel）。

理論予測（中心定理 êdge=(1-ρ)(p_ML-q)）: 直前 q^tyb→q^fin に近い分、選定は最終(0.74)へ寄り、
前売り(0.84)より悪化するはず＝「追跡精度↑でエッジ↓」の実証。>0.84 を超えて >1 が出れば要精査。

TYB 単勝の格納スケール（ZZZ9.9 の暗黙小数）は不明なので、最終オッズとの比で自己校正する。

使い方:
  python scripts/roi_trajectory_check.py --version baseline_jrdb_seirei --jra-only \
      --odds-dir data/jrdb_txt/oz_check --db data/keibam.db
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
def infer_odds_scale(raw_by_key: dict, final_by_key: dict) -> float:
    """raw オッズと既知スケールの最終オッズの比の中央値から倍率を推定（1.0 or 0.1）。

    格納が ZZZ9.9 の暗黙小数（例 123＝12.3）なら raw≈10×final → 0.1 を返す。
    既に実オッズ（12.3）なら比≈1 → 1.0。
    """
    ratios = []
    for key, o_raw in raw_by_key.items():
        of = final_by_key.get(key)
        if of and of > 0 and o_raw and o_raw > 0:
            ratios.append(o_raw / of)
    if not ratios:
        return 1.0
    med = float(np.median(ratios))
    return 0.1 if med > 3.0 else 1.0     # ≈10 なら /10、≈1 ならそのまま


def build_traj_records(
    pred_by_key: dict, oz_pre_by_race: dict, tyb_by_key: dict,
) -> list[dict]:
    """予測(p̂,最終,won)・OZ前売り・TYB直前 の3源すべてに存在する馬だけのレコード列。"""
    recs = []
    for rid, tan in oz_pre_by_race.items():
        for uma, o_pre in tan.items():
            key = (str(rid), int(uma))
            if key not in pred_by_key or key not in tyb_by_key:
                continue
            o_tyb = tyb_by_key[key]
            if not o_pre or o_pre <= 0 or not o_tyb or o_tyb <= 0:
                continue
            p, o_final, won = pred_by_key[key]
            if not o_final or o_final <= 0:
                continue
            recs.append({"rid": str(rid), "uma": int(uma), "p": float(p),
                         "o_pre": float(o_pre), "o_tyb": float(o_tyb),
                         "o_final": float(o_final), "won": float(won)})
    return recs


def ev_settle(recs: list[dict], sel_key: str, ev_thr: float) -> dict:
    """sel_key で EV=p̂·odds_sel>閾値 を選び、精算は常に最終単勝。"""
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


def _load_tyb(engine) -> dict:
    from sqlalchemy import text
    df = pd.read_sql(text("SELECT race_id, umaban, tansho_odds FROM raw_jrdb_tyb"), engine)
    out: dict = {}
    for r in df.itertuples():
        if pd.isna(r.umaban) or r.tansho_odds is None:
            continue
        try:
            o = float(r.tansho_odds)
        except (TypeError, ValueError):
            continue
        if o > 0:
            out[(str(r.race_id).split(".")[0], int(r.umaban))] = o
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="オッズ軌跡 realizable 検証（前売り→直前→最終）")
    ap.add_argument("--version", default="baseline_jrdb_seirei")
    ap.add_argument("--featured-path", default=None)
    ap.add_argument("--jra-only", action="store_true")
    ap.add_argument("--odds-dir", default="data/jrdb_txt/oz_check")
    ap.add_argument("--db", default=None)
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
    from src.storage._db import get_engine

    path = _resolve_backtest_model_path(args.version)
    place_ai = load_model_from_path(path)
    win_ai = load_win_head_for(path)
    eff = getattr(win_ai or place_ai, "effective_model", win_ai or place_ai)
    print(f"[traj] モデル {Path(path).name}（Win ヘッド={'あり' if win_ai else 'なし'}）")

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
    pred_by_key = {(rids[i].split(".")[0], int(umas[i])): (probs[i], finals[i], won[i])
                   for i in range(len(rids)) if not np.isnan(umas[i])}
    final_by_key = {k: v[1] for k, v in pred_by_key.items()}
    print(f"[traj] holdout(test) 予測 {len(pred_by_key):,} 頭")

    files = sorted(glob.glob(f"{args.odds_dir}/OZ*.txt") + glob.glob(f"{args.odds_dir}/oz*.txt"))
    if not files:
        print(f"OZ の .txt が見つかりません（{args.odds_dir}）。", file=sys.stderr)
        return 1
    oz_pre: dict = {}
    for fp in files:
        long_df = parse_odds(fp, "OZ")
        for rid, g in long_df.groupby("race_id"):
            tan = {int(r.combo): float(r.odds) for r in g[g.bet == "tansho"].itertuples()
                   if r.odds and float(r.odds) > 0}
            if tan:
                oz_pre[str(rid).split(".")[0]] = tan
    print(f"[traj] OZ 前売り {len(oz_pre):,} レース")

    tyb_raw = _load_tyb(get_engine(args.db))
    scale = infer_odds_scale(tyb_raw, final_by_key)
    tyb_by_key = {k: v * scale for k, v in tyb_raw.items()}
    print(f"[traj] TYB 直前 {len(tyb_raw):,} 頭（自己校正スケール ×{scale}）")

    recs = build_traj_records(pred_by_key, oz_pre, tyb_by_key)
    if not recs:
        print("3源（予測×OZ×TYB）の突合が空。期間の重なりを確認してください。", file=sys.stderr)
        return 1
    print(f"[traj] 評価対象（予測×OZ×TYB 突合）: {len(recs):,} 頭 / "
          f"{len({r['rid'] for r in recs}):,} レース\n")

    print("[traj] 前売り(realizable) vs 直前(realizable) vs 最終(look-ahead)｜精算は常に最終単勝")
    print(f"  {'閾値':<8}{'選定':<16}{'return_rate':>12}{'hit_rate':>10}{'n_bets':>9}{'profit':>12}")
    for t in args.ev_thresholds:
        for label, key in [("前売りOZ", "o_pre"), ("直前TYB", "o_tyb"), ("最終(先読)", "o_final")]:
            r = ev_settle(recs, key, t)
            head = f"EV>{t:<5.2f}" if key == "o_pre" else ""
            print(f"  {head:<8}{label:<16}{_f(r['return_rate']):>12}{_f(r['hit_rate']):>10}"
                  f"{r['n_bets']:>9}{_f(r['profit']):>12}")
    print("\n  判定: 直前TYB が前売りより良化し >1 に届く → 最新観測で市場に勝つ本物候補（要 年またぎOOS）。")
    print("        直前TYB ≈ 最終 < 前売り → 追跡精度↑でエッジ↓の理論通り＝realizable エッジ無し。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
