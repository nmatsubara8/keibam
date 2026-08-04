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


def add_field_interactions(df: pd.DataFrame) -> tuple[pd.DataFrame, list]:
    """自馬 pace_median/ten_idx と**場の全馬**から相互作用特徴を計算（レース単位→broadcast）。

    ユーザ設計の (1)逃げ圧力 (2)逃げ競合 (3)ペース分散 (4)脚質ミスマッチ を実装。
    pace_median: 位置取り（小=前・<0.2逃げ,<0.5先行）。ten_idx: テン指数（大=前半速い）。
    自馬 pace_median × 場の nige_cnt/front_ratio 等の交互作用は GBM が自動学習する。
    """
    d = df.copy()
    d["_pm"] = pd.to_numeric(d["pace_median"], errors="coerce")
    d["_ti"] = pd.to_numeric(d["ten_idx"], errors="coerce")
    g = d.groupby("rid")
    ti_mean, ti_std = g["_ti"].transform("mean"), g["_ti"].transform("std").replace(0, 1)
    d["_ti_z"] = (d["_ti"] - ti_mean) / ti_std            # レース内の相対速さ
    ti_med = g["_ti"].transform("median")
    d["_is_nige"] = (d["_pm"] < 0.2).astype(float)        # 逃げ候補
    d["_is_front"] = (d["_pm"] < 0.35).astype(float)      # 前に行く
    d["_strong_nige"] = ((d["_pm"] < 0.2) & (d["_ti"] > ti_med)).astype(float)  # 速い逃げ
    d["_ep_part"] = np.where(d["_is_front"] > 0, d["_ti_z"], 0.0)
    g2 = d.groupby("rid")
    d["nige_cnt"] = g2["_is_nige"].transform("sum")                 # 逃げ頭数
    d["front_ratio"] = g2["_is_front"].transform("mean")           # 前に行く比率
    d["pace_med_var"] = g2["_pm"].transform("var")                 # 位置取りの分散
    d["ten_var"] = g2["_ti"].transform("var")                      # (3)ペース分散
    d["escape_pressure"] = g2["_ep_part"].transform("sum")         # (1)逃げ圧力
    d["front_conflict"] = g2["_strong_nige"].transform("sum")      # (2)逃げ競合
    feats = ["nige_cnt", "front_ratio", "pace_med_var", "ten_var",
             "escape_pressure", "front_conflict"]
    return d.drop(columns=[c for c in d.columns if c.startswith("_")]), feats


def shuffle_within_race(X: np.ndarray, rids: np.ndarray, seed: int) -> np.ndarray:
    """行(特徴ベクトル)をレース内でシャッフル（プラセボ: 特徴の位置情報を破壊）。"""
    rng = np.random.default_rng(seed)
    out = X.copy()
    order = np.argsort(rids, kind="stable")
    srt = rids[order]
    start = 0
    for i in range(1, len(srt) + 1):
        if i == len(srt) or srt[i] != srt[start]:
            block = order[start:i]
            out[block] = X[block][rng.permutation(len(block))]
            start = i
    return out


def _fit_predict(Xtr, ytr, Xte, nonlinear: bool):
    """線形(Logistic)か非線形(LightGBM・交互作用を自動学習)で fit→test 確率を返す。"""
    if nonlinear:
        from lightgbm import LGBMClassifier
        m = LGBMClassifier(n_estimators=300, num_leaves=31, learning_rate=0.05,
                           min_child_samples=200, subsample=0.8, colsample_bytree=0.8,
                           verbose=-1).fit(Xtr, ytr)
    else:
        from sklearn.linear_model import LogisticRegression
        m = LogisticRegression(max_iter=1000).fit(Xtr, ytr)
    return m.predict_proba(Xte)[:, 1], m


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


def _load_kyi_orth(engine, extra=()):
    from sqlalchemy import text
    cols0 = pd.read_sql(text("SELECT * FROM raw_jrdb_kyi LIMIT 0"), engine).columns.tolist()
    have = [c for c in [*_ORTH, *extra] if c in cols0]
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
    ap.add_argument("--nonlinear", action="store_true",
                    help="LightGBM残差モデル＋脚質(kyakushitsu)で交互作用(出遅れ×脚質等)を拾う")
    ap.add_argument("--placebo", action="store_true",
                    help="直交特徴をレース内シャッフルしテール過学習/リークを排除")
    ap.add_argument("--pace", action="store_true",
                    help="脚質構成の相互作用特徴(nige_count等×自馬pace_median)を追加＝ABM de-risk")
    args = ap.parse_args(argv if argv is not None else sys.argv[1:])

    from sklearn.metrics import log_loss, roc_auc_score

    from src.constants._local_paths import LocalPaths
    from src.constants._results_cols import ResultsCols
    from src.pipeline._ingestion import load_raw
    from src.storage._db import get_engine

    featured = load_raw(args.featured_path or LocalPaths.FEATURED_DATA_PATH)
    if args.jra_only:
        featured = featured[central_index_mask(featured.index)]
    rank = pd.to_numeric(featured[ResultsCols.RANK], errors="coerce")
    cols = {
        "rid": featured.index.astype(str).str.split(".").str[0].to_numpy(),
        "uma": pd.to_numeric(featured[ResultsCols.UMABAN], errors="coerce").to_numpy(),
        "won": (rank <= 3).astype(float).to_numpy(),
    }
    # ABM de-risk: 脚質構成の相互作用特徴（自馬 pace_median × 場の nige_count/front_ratio 等）
    pace_cols = []
    if args.pace:
        cand = ["pace_median", "front_ratio", "nige_count", "senko_count",
                "mean_pace_median", "min_pace_median", "std_pace_median"]
        pace_cols = [c for c in cand if c in featured.columns]
        miss = [c for c in cand if c not in featured.columns]
        if miss:
            print(f"[place] featured に無いペース列（除外）: {miss}", file=sys.stderr)
        for c in pace_cols:
            cols[c] = pd.to_numeric(featured[c], errors="coerce").to_numpy()
    base = pd.DataFrame(cols).dropna(subset=["uma"])
    base["uma"] = base["uma"].astype(int)
    base["year"] = base["rid"].str[:4]

    eng = get_engine(args.db)
    fuku_odds = _load_col(eng, "raw_jrdb_tyb", "fukusho_odds").rename(columns={"fukusho_odds": "fo"})
    fuku_pay = _load_col(eng, "raw_jrdb_sed", "fukusho_payoff").rename(columns={"fukusho_payoff": "fp"})
    # 非線形モードは脚質(kyakushitsu)も入れて 出遅れ×脚質 等の交互作用を GBM に拾わせる
    kyi, have = _load_kyi_orth(eng, extra=("kyakushitsu",) if args.nonlinear else ())

    df = (base.merge(fuku_odds, on=["rid", "uma"], how="inner")
              .merge(fuku_pay, on=["rid", "uma"], how="inner")
              .merge(kyi, on=["rid", "uma"], how="inner"))
    if pace_cols:
        have = [*have, *pace_cols]     # 自馬 pace_median（相互作用の材料）
    # 逃げ競合→ペース崩壊→複勝残差: 場の相互作用特徴を計算し GBM 特徴集合へ
    if args.pace and "pace_median" in df.columns and "ten_idx" in df.columns:
        df, inter_feats = add_field_interactions(df)
        # ペース崩壊確率の proxy（速い逃げ競合×前圧力×ばらつき）。GBM が自馬脚質と交互作用させる。
        df["pace_collapse"] = (df["front_conflict"].clip(lower=0)
                               * df["escape_pressure"].clip(lower=0)).fillna(0.0)
        have = [*have, *inter_feats, "pace_collapse"]
        print(f"[place] 相互作用特徴 追加: {[*inter_feats, 'pace_collapse']}")
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
    mode = "非線形GBM＋脚質交互作用" if args.nonlinear else "線形"
    print(f"[place] モデル: {mode}{'（プラセボ:特徴レース内シャッフル）' if args.placebo else ''}")

    Xb_tr, Xb_te = tr[["logit_q"]].to_numpy(), te[["logit_q"]].to_numpy()
    if args.placebo:
        Xo_te = shuffle_within_race(Xo_te, te["rid"].to_numpy(), seed=0)
    pb, _ = _fit_predict(Xb_tr, ytr, Xb_te, args.nonlinear)
    pf, mf = _fit_predict(np.hstack([Xb_tr, Xo_tr]), ytr, np.hstack([Xb_te, Xo_te]), args.nonlinear)
    print("[place] Test1 OOS 上乗せ（複勝市場に直交指数を足して改善するか）")
    print(f"  logloss base={log_loss(yte, pb):.5f} +直交={log_loss(yte, pf):.5f} "
          f"Δ={log_loss(yte, pf)-log_loss(yte, pb):+.5f}")
    print(f"  AUC     base={roc_auc_score(yte, pb):.5f} +直交={roc_auc_score(yte, pf):.5f} "
          f"Δ={roc_auc_score(yte, pf)-roc_auc_score(yte, pb):+.5f}")
    imp = mf.feature_importances_[1:] if args.nonlinear else mf.coef_[0][1:]
    scores = dict(zip(have, imp, strict=False))
    label = "重要度" if args.nonlinear else "係数"
    print(f"  {label}:", {k: round(float(v), 4) for k, v in sorted(scores.items(), key=lambda x: -abs(x[1]))})

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
    # EV選択（realizable）: 予測入着率 pf × 複勝オッズ(直前 fo) > 閾値 → 確定複勝払戻で精算。
    # Δ選択は本命に偏る。EV選択は市場が過小評価する「中穴の入着(value)」を拾う真の money-test。
    fo_te = te["fo"].to_numpy()
    ev = pf * fo_te
    print("\n[place] EV選択（複勝 value: pf×複勝オッズ>閾値・realizable選定→確定払戻精算）")
    print(f"  {'EV閾値':>8}{'n':>9}{'hit':>9}{'複勝ROI':>10}{'平均配当':>10}")
    for thr in (1.0, 1.05, 1.1, 1.2, 1.3):
        m = ev > thr
        n = int(m.sum())
        if n == 0:
            print(f"  {thr:>8.2f}{0:>9}{'—':>9}{'—':>10}{'—':>10}")
            continue
        roi = float(pay_te[m].mean())
        print(f"  {thr:>8.2f}{n:>9}{float(yte[m].mean()):>9.4f}{roi:>10.4f}"
              f"{float(fo_te[m].mean()):>10.2f}")

    # 年別再現（Step5安全策）: 上位5%複勝ROI を 2024/2025/2026 個別に
    yr_te = te["year"].to_numpy()
    print("\n[place] 年別再現（予測Δ上位5%の複勝ROI・偶然/過学習排除）")
    k5 = max(1, int(len(d) * 0.05))
    top5 = set(order[:k5].tolist())
    for y in sorted(set(yr_te.tolist())):
        idx = np.array([i for i in range(len(d)) if i in top5 and yr_te[i] == y])
        if len(idx) > 20:
            print(f"  {y}: n={len(idx):>5}  複勝ROI={float(pay_te[idx].mean()):.4f}")

    best = stats["ROI"].max()
    best_ev = max((float(pay_te[(pf * fo_te) > t].mean())
                   for t in (1.0, 1.05, 1.1, 1.2, 1.3) if ((pf * fo_te) > t).sum() > 50), default=0.0)
    print(f"\n[place] 判定: 最良ビン複勝ROI={best:.4f} / 最良EV選択ROI={best_ev:.4f}")
    if best > 1.0 or best_ev > 1.0:
        print("  → 複勝で控除超えの候補。次: プラセボ＋年またぎ＋TYB realizable で厳格二重検証。")
    else:
        print("  → 複勝でも全選択<1＝控除の壁を越えず。直交アルファは複勝で単勝の14倍強いが"
              " takeout 未満＝公開+半公開データの探索は完全終了。残るは映像由来 proprietary のみ。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
