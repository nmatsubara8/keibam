"""卍式「購入ゾーン」再現 v2 — OOF ゾーン学習・train限定ビン・形状制約・両側打ち切り・walk-forward。

我々の既存検証は「edge 上位x%（片側テール）」。卍式は (指数帯 × オッズ帯) の**中央購入帯**で
極端edge（=モデル過信）を両側打ち切りする非重複軸。v1 の4つの落とし穴を塞いだ厳密版:

  #1 OOF: ゾーン選択に使う edge は必ず OOF（学習内予測の楽観を排除）。
     各テスト年 Y について: ゾーン評価期間=[Y-oof_win, Y-1] を、その手前までで fit したモデルで予測
     （＝OOF）。本番モデルは <Y 全部で再学習し Y を予測。ゾーンは OOF だけで確定→Y に固定適用。
  #2 ビン境界: オッズ帯は**事前固定**、edge帯は**ゾーン評価期間の分位**で固定（テスト年を見ない）。
  #3 多重選択: 50セル自由 ON/OFF ではなく**形状制約（連結矩形）**をグリッド探索（自由度~4）＝過学習抑制。
     方式A(自由セル ROI≥thr) と 方式C(矩形) を両方出す。
  #4 価格時点: 使えるのは TYB(T-15分)。7分前スナップショットが無いので「T-15市場で卍式が機能するか」
     までの検証（卍戦略そのものの否定ではない）。

軸は (絶対edge, odds) と (レース内edge順位, odds) を --edge-axis で選べる（卍の横軸=指数に近いのは順位）。
判定は年別 OOS・単一高配当依存(上位払戻除外ROI)・プラセボ。精算=JRDB SED 確定複勝払戻(100%)。

使い方:
  python scripts/manji_purchase_zone.py --jra-only --db data/keibam.db --edge-axis rank
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.constants._model_category import central_index_mask  # noqa: E402

_ORTH = ["deokure_rate", "pace_idx", "chokyo_idx", "gekiso_idx",
         "start_idx", "ten_idx", "agari_idx", "manken_idx"]
_ODDS_EDGES = [1.0, 1.5, 2.0, 3.0, 5.0, 10.0, np.inf]     # 事前固定の複勝オッズ帯（#2）


# ── 純ロジック（テスト対象） ─────────────────────────────────────────
def odds_band(o: float) -> int:
    for i in range(len(_ODDS_EDGES) - 1):
        if _ODDS_EDGES[i] <= o < _ODDS_EDGES[i + 1]:
            return i
    return -1     # 帯外（1.0未満）は無効


def assign_edge_band(edge: np.ndarray, bin_edges: np.ndarray) -> np.ndarray:
    """固定境界 bin_edges で edge を帯へ（テスト年も学習境界を使う #2）。"""
    return np.clip(np.digitize(edge, bin_edges[1:-1]), 0, len(bin_edges) - 2)


def cell_roi_table(df: pd.DataFrame) -> pd.DataFrame:
    """(eb,ob) セルごとの n / 複勝ROI / 払戻合計 を返す。"""
    g = df.groupby(["eb", "ob"])
    out = g.apply(lambda x: pd.Series({"n": len(x), "roi": float((x["pay"] * x["won"]).mean())}),
                  include_groups=False).reset_index()
    return out


def select_free_cells(roi_tbl: pd.DataFrame, min_roi: float, min_n: int) -> set:
    """方式A: ROI≥min_roi かつ n≥min_n の自由セル集合。"""
    ok = roi_tbl[(roi_tbl["roi"] >= min_roi) & (roi_tbl["n"] >= min_n)]
    return set(zip(ok["eb"], ok["ob"], strict=False))


def select_rectangle(df: pd.DataFrame, n_eb: int, n_ob: int, min_roi: float, min_n: int) -> set:
    """方式C: 連結矩形 [e_lo,e_hi]×[o_lo,o_hi] を全探索し ROI≥min_roi & n≥min_n の最良を返す（自由度4）。"""
    best, best_roi, best_n = None, -1.0, -1
    for elo in range(n_eb):
        for ehi in range(elo, n_eb):
            for olo in range(n_ob):
                for ohi in range(olo, n_ob):
                    m = (df["eb"].between(elo, ehi)) & (df["ob"].between(olo, ohi))
                    sub = df[m]
                    if len(sub) < min_n:
                        continue
                    roi = float((sub["pay"] * sub["won"]).mean())
                    # ROI 最大、同点はサンプル数の多い（＝頑健な）矩形を優先
                    if roi >= min_roi and (roi > best_roi or (roi == best_roi and len(sub) > best_n)):
                        best_roi, best_n = roi, len(sub)
                        best = {(e, o) for e in range(elo, ehi + 1) for o in range(olo, ohi + 1)}
    return best or set()


def apply_zone(df: pd.DataFrame, zone: set) -> tuple[float, int, float]:
    mask = [(e, o) in zone for e, o in zip(df["eb"], df["ob"], strict=False)]
    sub = df[mask]
    if len(sub) == 0:
        return float("nan"), 0, 0.0
    pay = (sub["pay"] * sub["won"]).to_numpy()
    return float(pay.mean()), len(sub), float(pay.sum())


def roi_excl_top(sub_pay: np.ndarray, k: int = 5) -> float:
    """上位 k 件の払戻を除いた ROI（単一高配当依存の検査）。"""
    if len(sub_pay) <= k:
        return float("nan")
    s = np.sort(sub_pay)[:-k]
    return float(s.sum() / len(sub_pay))     # 分母は全件（除外は分子のみ）


def _load_col(engine, table, col):
    from sqlalchemy import text
    df = pd.read_sql(text(f"SELECT race_id, umaban, {col} FROM {table}"), engine)
    df["rid"] = df["race_id"].astype(str).str.split(".").str[0]
    df["uma"] = pd.to_numeric(df["umaban"], errors="coerce")
    df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["uma"]).assign(uma=lambda x: x["uma"].astype(int))[["rid", "uma", col]]


def _fit_edge(train_df, pred_df, have, med):
    from lightgbm import LGBMClassifier
    from scipy.special import logit
    lq_tr = logit(np.clip(train_df["q"].to_numpy(), 1e-6, 1 - 1e-6))
    m = LGBMClassifier(n_estimators=300, num_leaves=31, learning_rate=0.05,
                       min_child_samples=200, verbose=-1).fit(
        np.column_stack([lq_tr, train_df[have].fillna(med).to_numpy()]), train_df["won"].to_numpy())
    lq = logit(np.clip(pred_df["q"].to_numpy(), 1e-6, 1 - 1e-6))
    p = m.predict_proba(np.column_stack([lq, pred_df[have].fillna(med).to_numpy()]))[:, 1]
    return p - pred_df["q"].to_numpy()


def _edge_axis(edge, rids, axis):
    """axis='abs' は絶対edge、'rank' はレース内edge順位（0=最小..1=最大）を返す。"""
    if axis == "abs":
        return edge
    return pd.Series(edge).groupby(pd.Series(rids)).rank(pct=True).to_numpy()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="卍式 購入ゾーン v2（OOF・形状制約・walk-forward）")
    ap.add_argument("--featured-path", default=None)
    ap.add_argument("--jra-only", action="store_true")
    ap.add_argument("--db", default=None)
    ap.add_argument("--edge-axis", choices=["abs", "rank"], default="rank",
                    help="横軸: abs=絶対edge / rank=レース内edge順位(卍の指数帯に近い)")
    ap.add_argument("--oof-win", type=int, default=2, help="OOFゾーン評価期間の年数（直近）")
    ap.add_argument("--n-eb", type=int, default=8, help="edge 帯数")
    ap.add_argument("--min-roi", type=float, default=1.0)
    ap.add_argument("--min-n", type=int, default=300)
    args = ap.parse_args(argv if argv is not None else sys.argv[1:])

    from src.constants._local_paths import LocalPaths
    from src.constants._results_cols import ResultsCols
    from src.pipeline._ingestion import load_raw
    from src.storage._db import get_engine
    from sqlalchemy import text

    featured = load_raw(args.featured_path or LocalPaths.FEATURED_DATA_PATH)
    if args.jra_only:
        featured = featured[central_index_mask(featured.index)]
    rank = pd.to_numeric(featured[ResultsCols.RANK], errors="coerce")
    base = pd.DataFrame({
        "rid": featured.index.astype(str).str.split(".").str[0].to_numpy(),
        "uma": pd.to_numeric(featured[ResultsCols.UMABAN], errors="coerce").to_numpy(),
        "won": (rank <= 3).astype(float).to_numpy(),
    }).dropna(subset=["uma"])
    base["uma"] = base["uma"].astype(int)
    base["year"] = base["rid"].str[:4].astype(int)

    eng = get_engine(args.db)
    kyi_cols = pd.read_sql(text("SELECT * FROM raw_jrdb_kyi LIMIT 0"), eng).columns.tolist()
    have = [c for c in _ORTH if c in kyi_cols]
    kyi = pd.read_sql(text(f"SELECT race_id, umaban, {', '.join(have)} FROM raw_jrdb_kyi"), eng)
    kyi["rid"] = kyi["race_id"].astype(str).str.split(".").str[0]
    kyi["uma"] = pd.to_numeric(kyi["umaban"], errors="coerce")
    kyi = kyi.dropna(subset=["uma"])
    kyi["uma"] = kyi["uma"].astype(int)
    for c in have:
        kyi[c] = pd.to_numeric(kyi[c], errors="coerce")
    df = (base.merge(kyi[["rid", "uma", *have]], on=["rid", "uma"], how="inner")
            .merge(_load_col(eng, "raw_jrdb_tyb", "fukusho_odds").rename(columns={"fukusho_odds": "fo"}),
                   on=["rid", "uma"], how="inner")
            .merge(_load_col(eng, "raw_jrdb_sed", "fukusho_payoff").rename(columns={"fukusho_payoff": "fp"}),
                   on=["rid", "uma"], how="inner"))
    df = df[df["fo"] >= 1.0].copy()
    df["pay"] = (df["fp"] / 100.0).fillna(0.0)
    inv = 1.0 / df["fo"]
    df["q"] = (inv / inv.groupby(df["rid"]).transform("sum") * 3).clip(upper=0.99)
    df["ob"] = df["fo"].map(odds_band)
    df = df[df["ob"] >= 0].copy()
    n_ob = len(_ODDS_EDGES) - 1
    print(f"[manji2] 結合 {len(df):,}頭 / {df['rid'].nunique():,}レース｜軸={args.edge_axis}｜OOF窓={args.oof_win}年")

    years = sorted(df["year"].unique())
    print(f"\n  {'test年':>6}{'方式':>5}{'zone':>6}{'OOS_n':>8}{'OOS_ROI':>9}{'除上位5':>9}{'全張り':>8}")
    agg = {"A": [0.0, 0], "C": [0.0, 0]}
    for i in range(args.oof_win + 1, len(years)):
        Y = years[i]
        oof_years = years[i - args.oof_win:i]           # OOF ゾーン評価期間（直近 oof_win 年）
        zfit = df[df["year"] < oof_years[0]]            # ゾーン評価期間の手前までで fit
        zeval = df[df["year"].isin(oof_years)].copy()
        prod_tr = df[df["year"] < Y]                   # 本番モデルは <Y 全部で再学習
        te = df[df["year"] == Y].copy()
        if len(zfit) < 15000 or len(zeval) < 5000 or len(te) < 3000:
            continue
        med = df[have].median()
        # OOF edge（zeval は zfit に含まれない＝学習外予測）
        zeval["edge"] = _edge_axis(_fit_edge(zfit, zeval, have, med), zeval["rid"].to_numpy(), args.edge_axis)
        # 本番 edge（te は prod_tr に含まれない）
        te["edge"] = _edge_axis(_fit_edge(prod_tr, te, have, med), te["rid"].to_numpy(), args.edge_axis)
        # ビン境界は OOF(zeval) の分位で固定 → te も同じ境界 (#2)
        be = np.quantile(zeval["edge"], np.linspace(0, 1, args.n_eb + 1))
        be[0], be[-1] = -np.inf, np.inf
        zeval["eb"] = assign_edge_band(zeval["edge"].to_numpy(), be)
        te["eb"] = assign_edge_band(te["edge"].to_numpy(), be)
        roi_tbl = cell_roi_table(zeval)
        zone_a = select_free_cells(roi_tbl, args.min_roi, args.min_n)
        zone_c = select_rectangle(zeval, args.n_eb, n_ob, args.min_roi, args.min_n)
        flat = float((te["pay"] * te["won"]).mean())
        for name, zone in (("A", zone_a), ("C", zone_c)):
            roi, n, psum = apply_zone(te, zone)
            mask = [(e, o) in zone for e, o in zip(te["eb"], te["ob"], strict=False)]
            excl = roi_excl_top((te[mask]["pay"] * te[mask]["won"]).to_numpy()) if n else float("nan")
            rs = "—" if np.isnan(roi) else f"{roi:.4f}"
            es = "—" if np.isnan(excl) else f"{excl:.4f}"
            print(f"  {Y:>6}{name:>5}{len(zone):>6}{n:>8}{rs:>9}{es:>9}{flat:>8.4f}")
            if n:
                agg[name][0] += psum
                agg[name][1] += n
    print("\n[manji2] walk-forward 全体 OOS_ROI:")
    for name in ("A", "C"):
        s, n = agg[name]
        r = s / n if n else float("nan")
        print(f"  方式{name}: {r:.4f}（n={n:,}）")
    ra = agg["A"][0] / agg["A"][1] if agg["A"][1] else 0
    rc = agg["C"][0] / agg["C"][1] if agg["C"][1] else 0
    if max(ra, rc) > 1.0:
        print("  → 控除超え候補。年別一貫性・除上位ROI・プラセボ・自票オッズ低下を要精査。")
    else:
        print("  → OOF・形状制約でも <1＝中央購入帯でも控除未満。卍当時アルファの環境差/減衰を確定。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
