"""複勝(place)版 市場残差エッジ検定 — Phase C。単勝で確立した規律を低効率券種へ移植。

単勝は完全決着（残差+6%・控除25%の1/4・集中もせず ROI≈0.85）。ただし直交JRDB指数に
**実在の上乗せ(+0.0062)** が確認された。ユーザ仮説: P(rank≤3) は展開/不利/馬群依存が大きく
（pace_idx/deokure_rate の効きが単勝より大）、かつ複勝プールは薄く非効率が残りやすい
＝直交アルファが控除を越えうる唯一の場所。

  target = 複勝的中(着順≤3)、baseline = logit(q_place)、追加 = 直交JRDB指数
  q_place: 直前複勝オッズ(TYB fukusho_odds)由来（レース内 Σ=3 正規化＝期待入着3頭）
  ROI    : 確定複勝払戻(SED fukusho_payoff/100)で全張り＝控除の壁を実額で判定
  真OOS年で ΔlogLoss / 予測Δ binning の複勝ROI / 極端テール。ROI>1 のビンが唯一の合格線。

使い方:
  python scripts/place_residual_edge_check.py --jra-only --db data/keibam.db --cutoff-year 2024
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
def place_market_q(fuku_odds: pd.Series, rids: pd.Series, n_place: int = 3) -> pd.Series:
    """複勝オッズ → レース内 Σ=n_place 正規化の市場入着確率（P(top3)近似）。[0,0.99]clip。"""
    inv = 1.0 / fuku_odds.where(fuku_odds > 0)
    s = inv.groupby(rids).transform("sum")
    return (inv / s * n_place).clip(upper=0.99).fillna(0.0)


def logit(p: np.ndarray, eps: float = 1e-6) -> np.ndarray:
    p = np.clip(p, eps, 1 - eps)
    return np.log(p / (1 - p))


def place_bin_stats(delta: np.ndarray, won: np.ndarray, q: np.ndarray,
                    payoff_mult: np.ndarray, n_bins: int = 10) -> pd.DataFrame:
    """予測Δの decile ごとに realized 入着率・平均q・E[Δ]・複勝ROI(=payoff_mult·won の平均)。"""
    df = pd.DataFrame({"delta": delta, "won": won, "q": q, "pay": payoff_mult * won})
    df["bin"] = pd.qcut(df["delta"].rank(method="first"), n_bins, labels=False)
    g = df.groupby("bin")
    out = g.agg(n=("won", "size"), pred_delta=("delta", "mean"),
                realized=("won", "mean"), mean_q=("q", "mean"), ROI=("pay", "mean")).reset_index()
    out["E_delta"] = out["realized"] - out["mean_q"]
    return out


def _load_kyi_orth(engine):
    from sqlalchemy import text
    cols0 = pd.read_sql(text("SELECT * FROM raw_jrdb_kyi LIMIT 0"), engine).columns.tolist()
    have = [c for c in _ORTH if c in cols0]
    df = pd.read_sql(text(f"SELECT race_id, umaban, {', '.join(have)} FROM raw_jrdb_kyi"), engine)
    df["rid"] = df["race_id"].astype(str).str.split(".").str[0]
    df["uma"] = pd.to_numeric(df["umaban"], errors="coerce")
    for c in have:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["uma"]).copy()
    df["uma"] = df["uma"].astype(int)
    return df[["rid", "uma", *have]], have


def _load_col(engine, table, col):
    """(race_id, umaban, col) を rid/uma/col の DataFrame で返す（複勝オッズ/払戻の共通ローダ）。"""
    from sqlalchemy import text
    df = pd.read_sql(text(f"SELECT race_id, umaban, {col} FROM {table}"), engine)
    df["rid"] = df["race_id"].astype(str).str.split(".").str[0]
    df["uma"] = pd.to_numeric(df["umaban"], errors="coerce")
    df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["uma"]).copy()
    df["uma"] = df["uma"].astype(int)
    return df[["rid", "uma", col]]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="複勝 市場残差エッジ検定（Phase C）")
    ap.add_argument("--featured-path", default=None)
    ap.add_argument("--jra-only", action="store_true")
    ap.add_argument("--db", default=None)
    ap.add_argument("--cutoff-year", type=int, default=2024)
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
    rank = pd.to_numeric(featured[ResultsCols.RANK], errors="coerce")
    base = pd.DataFrame({
        "rid": featured.index.astype(str).str.split(".").str[0],
        "uma": pd.to_numeric(featured[ResultsCols.UMABAN], errors="coerce"),
        "won": (rank <= 3).astype(float),
    }).dropna(subset=["uma"])
    base["uma"] = base["uma"].astype(int)
    base["year"] = base["rid"].str[:4]

    eng = get_engine(args.db)
    fuku_odds = _load_col(eng, "raw_jrdb_tyb", "fukusho_odds").rename(columns={"fukusho_odds": "fo"})
    fuku_pay = _load_col(eng, "raw_jrdb_sed", "fukusho_payoff").rename(columns={"fukusho_payoff": "fp"})
    kyi, have = _load_kyi_orth(eng)

    df = (base.merge(fuku_odds, on=["rid", "uma"], how="inner")
              .merge(fuku_pay, on=["rid", "uma"], how="inner")
              .merge(kyi, on=["rid", "uma"], how="inner"))
    df = df[df["fo"] > 0].copy()
    # 複勝オッズのスケール自己校正（ZZZ9.9 暗黙小数。的中馬の払戻÷100 と整合させる）
    plc = df[df["won"] == 1]
    scale = 1.0
    if len(plc) > 100:
        r = (plc["fp"] / 100.0) / plc["fo"]
        med = float(r.median())
        scale = 0.1 if med < 0.34 else 1.0   # fo が ×10 表記なら payoff比≈0.1×
    df["fo"] = df["fo"] * scale
    df["q"] = place_market_q(df["fo"], df["rid"]).to_numpy()
    df["pay_mult"] = (df["fp"] / 100.0).fillna(0.0)
    df = df[df["q"] > 0]
    print(f"[place] 結合 {len(df):,}頭 / {df['rid'].nunique():,}レース｜複勝odds自己校正×{scale}｜列 {have}")

    df["logit_q"] = logit(df["q"].to_numpy())
    tr = df[df["year"].astype(int) < args.cutoff_year]
    te = df[df["year"].astype(int) >= args.cutoff_year]
    if len(tr) < 5000 or len(te) < 5000:
        print(f"[place] 学習 {len(tr):,}/検証 {len(te):,} が薄い。", file=sys.stderr)
        return 1
    print(f"[place] 学習<{args.cutoff_year}: {len(tr):,} / 真OOS≥{args.cutoff_year}: {len(te):,}"
          f"（複勝的中率 OOS {te['won'].mean():.3f}）\n")

    med = tr[have].median()
    std = tr[have].std().replace(0, 1)
    Xo_tr = ((tr[have].fillna(med) - med) / std).to_numpy()
    Xo_te = ((te[have].fillna(med) - med) / std).to_numpy()
    ytr, yte, qte = tr["won"].to_numpy(), te["won"].to_numpy(), te["q"].to_numpy()
    pay_te = te["pay_mult"].to_numpy()

    Xb_tr, Xb_te = tr[["logit_q"]].to_numpy(), te[["logit_q"]].to_numpy()
    mb = LogisticRegression(max_iter=1000).fit(Xb_tr, ytr)
    mf = LogisticRegression(max_iter=1000).fit(np.hstack([Xb_tr, Xo_tr]), ytr)
    pb = mb.predict_proba(Xb_te)[:, 1]
    pf = mf.predict_proba(np.hstack([Xb_te, Xo_te]))[:, 1]
    print("[place] Test1 OOS 上乗せ（複勝市場に直交指数を足して改善するか）")
    print(f"  logloss base={log_loss(yte, pb):.5f} +直交={log_loss(yte, pf):.5f} "
          f"Δ={log_loss(yte, pf)-log_loss(yte, pb):+.5f}")
    print(f"  AUC     base={roc_auc_score(yte, pb):.5f} +直交={roc_auc_score(yte, pf):.5f} "
          f"Δ={roc_auc_score(yte, pf)-roc_auc_score(yte, pb):+.5f}")
    coef = dict(zip(have, mf.coef_[0][1:], strict=False))
    print("  係数:", {k: round(v, 4) for k, v in sorted(coef.items(), key=lambda x: -abs(x[1]))})

    stats = place_bin_stats(pf - qte, yte, qte, pay_te, args.n_bins)
    print("\n[place] Test2 OOS 残差 binning（複勝ROI=確定複勝払戻の全張り回収率）")
    print(f"  {'bin':>4}{'n':>8}{'realized':>10}{'mean_q':>10}{'E[Δ]':>10}{'複勝ROI':>10}")
    for _, r in stats.iterrows():
        print(f"  {int(r['bin']):>4}{int(r['n']):>8}{r['realized']:>10.4f}{r['mean_q']:>10.4f}"
              f"{r['E_delta']:>+10.4f}{r['ROI']:>10.4f}")

    d = pf - qte
    order = np.argsort(-d)
    print("\n[place] 極端テール（予測Δ上位 x% を確定複勝払戻で全張り）")
    print(f"  {'上位':>7}{'n':>8}{'realized':>10}{'複勝ROI':>10}")
    for pct in (10.0, 5.0, 2.0, 1.0):
        k = max(1, int(len(d) * pct / 100))
        idx = order[:k]
        print(f"  {pct:>6.1f}%{k:>8}{float(yte[idx].mean()):>10.4f}{float(pay_te[idx].mean()):>10.4f}")
    best = stats["ROI"].max()
    print(f"\n[place] 判定: 最良ビン複勝ROI={best:.4f}")
    if best > 1.0:
        print("  → 複勝で控除超えの候補。次: TYB realizable 選定＋プラセボ＋年またぎで厳格二重検証。")
    else:
        print("  → 複勝でも全ビン<1＝控除の壁を越えず。低効率券種でも実在アルファは takeout 未満"
              "＝公開+半公開データの探索は完全終了。残るは映像由来の proprietary 情報のみ。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
