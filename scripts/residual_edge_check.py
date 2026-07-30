"""市場残差エッジ検定 — Δ=win−q を直交JRDB指数で説明できるか（ROIより先に E[Δ]>0 を見る）。

本セッションで市場は半強効率（5経路 null・意思決定層も真OOSで ROI0.83）と確定。唯一 ROI>1 を
生みうるのは E[Δ]=E[p_true−q_market]>0、すなわち**市場が価格化しきれていない直交情報**。
ユーザ方法論（Phase A）に従い、ROI ではなく残差の説明力で先に判定する:

  target = win（0/1）、baseline 特徴 = logit(q_market)、追加 = 直交JRDB指数
  （deokure_rate/pace_idx/chokyo_idx/gekiso_idx/start_idx/ten_idx/agari_idx/manken_idx）
  → 学習年で fit、真OOS年で ΔlogLoss/ΔAUC（市場に上乗せするか）
  → 予測 Δ=p_full−q を OOS で decile binning し、各ビンの realized 勝率 vs 平均 q（E[Δ]の符号）

q は**最終単勝**（最も効率的＝最難関のバー）。ここで上乗せが出なければ realizable エッジは無い。
出れば TYB(realizable) で再検証する価値がある初の候補。

使い方:
  python scripts/residual_edge_check.py --jra-only --db data/keibam.db --cutoff-year 2024
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


# ── 純ロジック（テスト対象） ─────────────────────────────────────────
def within_race_q(odds: pd.Series, rids: pd.Series) -> pd.Series:
    """最終単勝オッズ → レース内 Σ=1 の市場勝率。非正は寄与0。"""
    inv = 1.0 / odds.where(odds > 0)
    s = inv.groupby(rids).transform("sum")
    return (inv / s).fillna(0.0)


def logit(p: np.ndarray, eps: float = 1e-6) -> np.ndarray:
    p = np.clip(p, eps, 1 - eps)
    return np.log(p / (1 - p))


def residual_bin_stats(delta: np.ndarray, won: np.ndarray, q: np.ndarray,
                       n_bins: int = 10) -> pd.DataFrame:
    """予測 Δ の decile ごとに realized 勝率・平均 q・E[Δ]=realized−q を出す。"""
    df = pd.DataFrame({"delta": delta, "won": won, "q": q})
    df["bin"] = pd.qcut(df["delta"].rank(method="first"), n_bins, labels=False)
    g = df.groupby("bin")
    out = g.agg(n=("won", "size"), pred_delta=("delta", "mean"),
                realized=("won", "mean"), mean_q=("q", "mean")).reset_index()
    out["E_delta"] = out["realized"] - out["mean_q"]
    return out


def _load_kyi_orth(engine) -> pd.DataFrame:
    from sqlalchemy import text
    cols0 = pd.read_sql(text("SELECT * FROM raw_jrdb_kyi LIMIT 0"), engine).columns.tolist()
    have = [c for c in _ORTH if c in cols0]
    miss = [c for c in _ORTH if c not in cols0]
    if miss:
        print(f"[resid] raw_jrdb_kyi に無い候補列（除外）: {miss}", file=sys.stderr)
    sel = ["race_id", "umaban", *have]
    df = pd.read_sql(text(f"SELECT {', '.join(sel)} FROM raw_jrdb_kyi"), engine)
    df["race_id"] = df["race_id"].astype(str).str.split(".").str[0]
    df["umaban"] = pd.to_numeric(df["umaban"], errors="coerce")
    for c in have:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.dropna(subset=["umaban"]), have


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="市場残差エッジ検定（直交JRDB指数）")
    ap.add_argument("--featured-path", default=None)
    ap.add_argument("--jra-only", action="store_true")
    ap.add_argument("--db", default=None)
    ap.add_argument("--cutoff-year", type=int, default=2024, help="この年以降を真OOS test に")
    ap.add_argument("--n-bins", type=int, default=10)
    args = ap.parse_args(argv if argv is not None else sys.argv[1:])

    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import log_loss, roc_auc_score

    from src.constants._local_paths import LocalPaths
    from src.constants._results_cols import ResultsCols
    from src.pipeline._ingestion import load_raw
    from src.storage._db import get_engine

    featured = load_raw(args.featured_path or LocalPaths.FEATURED_DATA_PATH)
    if args.jra_only:
        featured = featured[central_index_mask(featured.index)]
    base = pd.DataFrame({
        "rid": featured.index.astype(str).str.split(".").str[0],
        "uma": pd.to_numeric(featured[ResultsCols.UMABAN], errors="coerce"),
        "odds": pd.to_numeric(featured[ResultsCols.TANSHO_ODDS], errors="coerce"),
        "won": (pd.to_numeric(featured[ResultsCols.RANK], errors="coerce") == 1).astype(float),
    }).dropna(subset=["uma"])
    base["uma"] = base["uma"].astype(int)
    base["year"] = base["rid"].str[:4]
    base["q"] = within_race_q(base["odds"], base["rid"]).to_numpy()
    base = base[base["q"] > 0]

    kyi, have = _load_kyi_orth(get_engine(args.db))
    df = base.merge(kyi, on=["rid", "uma"], how="inner")
    cov = len(df) / len(base) if len(base) else 0
    print(f"[resid] featured {len(base):,}頭 → KYI直交結合 {len(df):,}頭（被覆率 {cov:.1%}）｜列: {have}")

    df["logit_q"] = logit(df["q"].to_numpy())
    tr = df[df["year"].astype(int) < args.cutoff_year]
    te = df[df["year"].astype(int) >= args.cutoff_year]
    if len(tr) < 5000 or len(te) < 5000:
        print(f"[resid] 学習 {len(tr):,}/検証 {len(te):,} が薄すぎ。cutoff を見直してください。", file=sys.stderr)
        return 1
    print(f"[resid] 学習<{args.cutoff_year}: {len(tr):,}頭 / 真OOS≥{args.cutoff_year}: {len(te):,}頭\n")

    # 直交特徴の欠損は学習中央値で補完し標準化
    med = tr[have].median()
    Xtr_o = ((tr[have].fillna(med) - med) / (tr[have].std().replace(0, 1))).to_numpy()
    Xte_o = ((te[have].fillna(med) - med) / (tr[have].std().replace(0, 1))).to_numpy()
    ytr, yte = tr["won"].to_numpy(), te["won"].to_numpy()
    qte = te["q"].to_numpy()

    # baseline: logit(q) のみ / full: logit(q)+直交
    Xtr_b = tr[["logit_q"]].to_numpy()
    Xte_b = te[["logit_q"]].to_numpy()
    Xtr_f = np.hstack([Xtr_b, Xtr_o])
    Xte_f = np.hstack([Xte_b, Xte_o])
    mb = LogisticRegression(max_iter=1000, C=1.0).fit(Xtr_b, ytr)
    mf = LogisticRegression(max_iter=1000, C=1.0).fit(Xtr_f, ytr)
    pb = mb.predict_proba(Xte_b)[:, 1]
    pf = mf.predict_proba(Xte_f)[:, 1]

    ll_b, ll_f = log_loss(yte, pb), log_loss(yte, pf)
    auc_b, auc_f = roc_auc_score(yte, pb), roc_auc_score(yte, pf)
    print("[resid] Test1 OOS 上乗せ（市場 logit(q) に直交指数を足して改善するか）")
    print(f"  logloss  baseline={ll_b:.5f}  +直交={ll_f:.5f}  Δ={ll_f-ll_b:+.5f}（負で改善）")
    print(f"  AUC      baseline={auc_b:.5f}  +直交={auc_f:.5f}  Δ={auc_f-auc_b:+.5f}（正で改善）")
    coef = dict(zip(have, mf.coef_[0][1:], strict=False))
    print("  係数(標準化):", {k: round(v, 4) for k, v in sorted(coef.items(), key=lambda x: -abs(x[1]))})

    # Test2: 予測 Δ=pf−q の decile ごとに realized 勝率 vs 平均 q（E[Δ]の符号）
    delta = pf - qte
    stats = residual_bin_stats(delta, yte, qte, args.n_bins)
    print("\n[resid] Test2 OOS 残差 binning（E[Δ]=realized−q が上位ビンで正か）")
    print(f"  {'bin':>4}{'n':>8}{'pred_Δ':>10}{'realized':>10}{'mean_q':>10}{'E[Δ]':>10}")
    for _, r in stats.iterrows():
        print(f"  {int(r['bin']):>4}{int(r['n']):>8}{r['pred_delta']:>+10.4f}"
              f"{r['realized']:>10.4f}{r['mean_q']:>10.4f}{r['E_delta']:>+10.4f}")
    top = stats.iloc[-1]
    print(f"\n[resid] 判定: ΔlogLoss={ll_f-ll_b:+.5f} / 最上位ビン E[Δ]={top['E_delta']:+.4f}")
    if ll_f - ll_b < -0.0005 and top["E_delta"] > 0:
        print("  → 直交指数が市場に上乗せの候補。次: TYB(realizable) で再検証＋年またぎ二重確認。")
    else:
        print("  → 上乗せ無し＝これら JRDB 指数も市場に価格化済み（半公開エコー）。真の直交源は"
              "映像由来の前走不利/出遅れ事象のみ。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
