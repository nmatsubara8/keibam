"""オッズの「動き」×ML の残差エッジ検証 — 市場追従ではなく市場修正Δqの情報を測る。

dec着済み: 市場に追従(最終へ収束)すると勝てない（êdge=(1-ρ)(p_ML−q)）。
未検証(ユーザ提案): 市場の**修正の向き** Δq_i = q_TYB − q_OZ（OZ/TYB とも bet前に観測可＝realizable）と
ML の**不一致**にエッジが宿るか。特に Case2「市場は修正したが ML は逆」。

2系統で検証（規律: in-sample スクリーン→OOS 両方向、事前定義セグメント）:
  (I) 動きΔR²: blend(fund=q_OZ, public=q_TYB) の ΔR²。>0 両方向なら「早期オッズが最新水準を超える
      残差情報を持つ」＝市場が過修正/慣性＝動きに情報。≈0 なら最新水準が動きを包摂＝情報なし。
  (II) セグメントROI(realizable・精算は最終単勝): 上昇/下落(sign Δq) × ML上/下(sign resid, resid=p_ML−q_TYB)
      × 人気帯。どの領域が return_rate>1 か（多重比較に注意し、in-sample の生存は OOS で要再現）。

使い方:
  python scripts/odds_movement_edge_check.py --version baseline_jrdb_seirei --jra-only \
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
from src.policies._blend import blend_diagnostic, fit_blend  # noqa: E402


# ── 純ロジック（テスト対象） ─────────────────────────────────────────
def _norm_within(df: pd.DataFrame, src: str, dst: str) -> pd.Series:
    """race_id(列 rid)ごとに src を Σ=1 正規化して dst 名の Series を返す。"""
    g = df.groupby("rid")[src]
    return (df[src] / g.transform("sum")).rename(dst)


def add_normalized(df: pd.DataFrame) -> pd.DataFrame:
    """o_oz/o_tyb/p_ml から within-race の q_oz/q_tyb/p_mln, Δq, resid, 人気帯を付与。"""
    out = df.copy()
    out["inv_oz"] = 1.0 / out["o_oz"]
    out["inv_tyb"] = 1.0 / out["o_tyb"]
    out["q_oz"] = _norm_within(out, "inv_oz", "q_oz")
    out["q_tyb"] = _norm_within(out, "inv_tyb", "q_tyb")
    out["p_mln"] = _norm_within(out, "p_ml", "p_mln")
    out["dq"] = out["q_tyb"] - out["q_oz"]          # 市場の修正（+:被支持/短縮 −:見限り/延伸）
    out["resid"] = out["p_mln"] - out["q_tyb"]      # ML と最新市場のズレ
    out["pop_band"] = pd.cut(out["o_tyb"], [0, 3, 7, 20, np.inf],
                             labels=["本命<3", "対抗3-7", "中穴7-20", "大穴≥20"])
    return out.drop(columns=["inv_oz", "inv_tyb"])


def segment_label(dq: float, resid: float, dead: float) -> str:
    """(市場修正の向き × ML残差の向き) の4象限。|値|<dead は中立で除外。"""
    if abs(dq) < dead or abs(resid) < dead:
        return "中立"
    mv = "市場↑" if dq > 0 else "市場↓"
    ml = "ML↑" if resid > 0 else "ML↓"
    tag = "一致" if (dq > 0) == (resid > 0) else "逆行"   # Case2=逆行
    return f"{mv}×{ml}({tag})"


def segment_roi(df: pd.DataFrame, dead: float = 0.0) -> pd.DataFrame:
    """セグメント別に flat 回収率（精算=最終単勝）・n・的中率・平均最終オッズ。"""
    d = df.copy()
    d["seg"] = [segment_label(a, b, dead) for a, b in zip(d["dq"], d["resid"], strict=False)]
    rows = []
    for seg, g in d.groupby("seg"):
        n = len(g)
        payout = (g["o_final"] * g["won"]).sum()
        rows.append({"seg": seg, "n": n, "hit": float(g["won"].mean()),
                     "return_rate": float(payout) / n if n else float("nan"),
                     "mean_o_final": float(g["o_final"].mean())})
    return pd.DataFrame(rows).sort_values("return_rate", ascending=False)


def races_for_blend(df: pd.DataFrame, fund_col: str, public_col: str) -> list:
    """(p_fund, p_public, winner) の列を作る（勝ち馬ちょうど1頭のレースのみ）。"""
    races = []
    for _rid, g in df.groupby("rid"):
        pf = {int(u): float(v) for u, v in zip(g["uma"], g[fund_col], strict=False) if v > 0}
        pp = {int(u): float(v) for u, v in zip(g["uma"], g[public_col], strict=False) if v > 0}
        w = g.loc[g["won"] == 1, "uma"]
        if len(w) == 1 and pf and pp:
            races.append((pf, pp, int(w.iloc[0])))
    return races


def dr2_in(races: list, min_races: int) -> float | None:
    if len(races) < min_races:
        return None
    return blend_diagnostic(races, fit_blend(races))["delta_r2"]


def dr2_oos(tr: list, te: list, min_races: int) -> float | None:
    if len(tr) < min_races or len(te) < min_races:
        return None
    return blend_diagnostic(te, fit_blend(tr))["delta_r2"]


def _f(v) -> str:
    return "—" if v is None or (isinstance(v, float) and np.isnan(v)) else f"{v:+.4f}"


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
    ap = argparse.ArgumentParser(description="オッズの動き×ML 残差エッジ検証")
    ap.add_argument("--version", default="baseline_jrdb_seirei")
    ap.add_argument("--featured-path", default=None)
    ap.add_argument("--jra-only", action="store_true")
    ap.add_argument("--odds-dir", default="data/jrdb_txt/oz_check")
    ap.add_argument("--db", default=None)
    ap.add_argument("--test-size", type=float, default=0.2)
    ap.add_argument("--min-races", type=int, default=200)
    ap.add_argument("--dead", type=float, default=0.01, help="中立とみなす |Δq|,|resid| の下限")
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
    print(f"[move] モデル {Path(path).name}（Win ヘッド={'あり' if win_ai else 'なし'}）")

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
    pred = {(rids[i].split(".")[0], int(umas[i])): (probs[i], finals[i], won[i])
            for i in range(len(rids)) if not np.isnan(umas[i])}

    files = sorted(glob.glob(f"{args.odds_dir}/OZ*.txt") + glob.glob(f"{args.odds_dir}/oz*.txt"))
    oz: dict = {}
    for fp in files:
        long_df = parse_odds(fp, "OZ")
        for rid, g in long_df.groupby("race_id"):
            tan = {int(r.combo): float(r.odds) for r in g[g.bet == "tansho"].itertuples()
                   if r.odds and float(r.odds) > 0}
            if tan:
                oz[str(rid).split(".")[0]] = tan
    tyb_raw = _load_tyb(get_engine(args.db))
    # 自己校正（最終オッズ比）
    ratios = [tyb_raw[k] / pred[k][1] for k in tyb_raw if k in pred and pred[k][1] > 0]
    scale = 0.1 if (ratios and float(np.median(ratios)) > 3.0) else 1.0
    tyb = {k: v * scale for k, v in tyb_raw.items()}

    recs = []
    for rid, tan in oz.items():
        for uma, o_oz in tan.items():
            key = (rid, int(uma))
            if key in pred and key in tyb and o_oz > 0 and tyb[key] > 0:
                p, o_final, w = pred[key]
                if o_final > 0:
                    recs.append({"rid": rid, "uma": int(uma), "o_oz": o_oz,
                                 "o_tyb": tyb[key], "o_final": o_final, "p_ml": p, "won": w})
    if not recs:
        print("3源突合が空。", file=sys.stderr)
        return 1
    df = add_normalized(pd.DataFrame(recs))
    df["year"] = df["rid"].str[:4]
    print(f"[move] 突合 {len(df):,} 頭 / {df['rid'].nunique():,} レース（TYBスケール×{scale}）\n")

    # (I) 動きΔR²（市場修正が最新水準を超える情報を持つか）
    print("[move] (I) 動きΔR²: blend(fund=q_OZ, public=q_TYB) — 早期オッズが最新水準に上乗せするか")
    races = races_for_blend(df, "q_oz", "q_tyb")
    d_in = dr2_in(races, args.min_races)
    print(f"  in-sample ΔR² = {_f(d_in)}（>0 なら動きに残差情報の候補）")
    yrs = sorted(df["year"].unique())
    if len(yrs) >= 2 and d_in is not None and d_in > 0:
        a, b = yrs[0], yrs[-1]
        rA = races_for_blend(df[df.year == a], "q_oz", "q_tyb")
        rB = races_for_blend(df[df.year == b], "q_oz", "q_tyb")
        print(f"  OOS {a}→{b} = {_f(dr2_oos(rA, rB, args.min_races))} / "
              f"{b}→{a} = {_f(dr2_oos(rB, rA, args.min_races))}（両方向+ で生存）")
    else:
        print("  in-sample ≤0 → 即棄却（動きは最新水準に包摂＝情報なし）。" if (d_in or 0) <= 0
              else "  年が1つ→OOS 不可。")

    # (II) セグメントROI（realizable・精算=最終）
    print("\n[move] (II) セグメント別 flat 回収率（realizable 選定・精算=最終単勝）")
    seg = segment_roi(df, args.dead)
    print(f"  {'セグメント':<22}{'n':>8}{'hit':>8}{'return_rate':>12}{'平均最終O':>10}")
    for _, r in seg.iterrows():
        print(f"  {r['seg']:<22}{r['n']:>8}{r['hit']:>8.4f}{r['return_rate']:>12.4f}"
              f"{r['mean_o_final']:>10.1f}")
    win = seg[seg["return_rate"] > 1.0]
    print(f"\n  return_rate>1 のセグメント: {len(win)}（多重比較に注意・in-sample の生存は OOS で要再現）")
    if len(win):
        print("  → 人気帯クロスと年またぎ OOS で再現するか次段で確認。")
    else:
        print("  → 全セグメント<1＝動き×ML でも realizable エッジ無し（市場効率の確認）。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
