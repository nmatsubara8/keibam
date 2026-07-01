"""アンサンブル・スタッキング評価ハーネス — 市場＋3ヘッドを対数プールで合成し OOS 比較。

ベース予測器（すべて学習年で作り、OOS 年で予測＝base にとって真の OOS）:
  - 市場 p_mkt        : 確定単勝オッズ由来（build_edge_frame）
  - LightGBM r_hat    : 既存 win-head（学習済みモデル）
  - 条件付ロジット      : レース内 softmax 最尤（conditional_logit の機構）
  - LGBMRanker        : lambdarank + 温度較正（lgbm_ranker の機構）

メタ学習器（合成）: 対数線形プール  p ∝ Π_k p_k^{w_k}  ＝ レース内 softmax(Σ_k w_k log p_k)。
Benter の市場合成を多ベースに一般化したもの。重み w_k は「勝ち馬の within-race 尤度最大化」で
最尤推定（conditional_logit の最尤機構を log 確率を特徴として流用）。

**リーク対策**: ベースは OOS 年を一切見ない（学習年のみ）。メタは **OOS 年内の race 単位 K-fold で
クロスfit**（メタが評価と同じレースを見ない）。→ ベース・メタ双方リークなしで「合成が市場/各ヘッドを
超えるか」を1表で判定できる。全モデルを同一の（欠損除外後）集合で評価する。

実行:
  python stack_eval.py --years 2026
  python stack_eval.py --years 2026 --relevance top6 --folds 5
"""

from __future__ import annotations

import argparse
import sys
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
warnings.filterwarnings("ignore", message="X does not have valid feature names", category=UserWarning)


def _fit_scaler(tr, cols):
    """学習データで中央値・平均・標準偏差を求め、分散0列を落とした keep を返す。"""
    import numpy as np

    med = tr[cols].astype("float64").median(numeric_only=True)
    mean = tr[cols].astype("float64").fillna(med).mean()
    std = tr[cols].astype("float64").fillna(med).std().replace(0, np.nan)
    keep = std.dropna().index.tolist()
    return keep, med[keep], mean[keep], std[keep]


def _design(df, keep, med, mean, std):
    z = (df[keep].astype("float64").fillna(med) - mean) / std
    return z.fillna(0.0).to_numpy(dtype="float64")


def _clogit_map(CL, tr, oos, cols, l2):
    """条件付きロジットを学習し OOS 予測を (race_id,馬番)->勝率 の辞書で返す。"""
    import numpy as np
    import pandas as pd
    from src.simulation._edge_diagnostic import _actual_win

    keep, med, mean, std = _fit_scaler(tr, cols)
    y = _actual_win(tr).to_numpy()
    wpr = pd.Series(y, index=tr.index).groupby(level=0).sum()
    good = set(wpr[wpr >= 1].index)
    m = tr.index.isin(good)
    w, _ = CL._fit_conditional_logit(_design(tr[m], keep, med, mean, std),
                                     tr.index.to_numpy()[m], y[m], l2)
    u = _design(oos, keep, med, mean, std) @ w
    p = CL._predict_within_race(u, oos.index.to_numpy())
    umb = pd.to_numeric(oos["馬番"], errors="coerce").to_numpy()
    return {(str(r), int(x)): float(v)
            for r, x, v in zip(oos.index.astype(str), umb, p.to_numpy()) if x == x and v == v}


def _ranker_map(CL, relevance_fn, tr, oos, cols, relevance, n_est):
    """LGBMRanker を学習・温度較正し OOS 予測を (race_id,馬番)->勝率 の辞書で返す。"""
    import lightgbm as lgb
    import numpy as np
    import pandas as pd
    from src.simulation._edge_diagnostic import _actual_win

    tr_ok = tr[pd.to_numeric(tr["着順"], errors="coerce").notna()].sort_index(kind="stable")
    y_rel = relevance_fn(pd.to_numeric(tr_ok["着順"], errors="coerce"), relevance)
    groups = tr_ok.groupby(level=0, sort=False).size().to_numpy()
    Xtr = tr_ok[cols].astype("float64")
    rk = lgb.LGBMRanker(objective="lambdarank", n_estimators=n_est, num_leaves=31,
                        learning_rate=0.05, min_child_samples=50, subsample=0.8,
                        subsample_freq=1, colsample_bytree=0.8, random_state=0,
                        n_jobs=-1, verbose=-1)
    rk.fit(Xtr, y_rel, group=groups)
    s = rk.predict(Xtr); sm, ss = float(np.mean(s)), float(np.std(s)) or 1.0
    z = ((s - sm) / ss).reshape(-1, 1)
    yw = _actual_win(tr_ok).to_numpy()
    wpr = pd.Series(yw, index=tr_ok.index).groupby(level=0).sum()
    good = set(wpr[wpr >= 1].index)
    m = tr_ok.index.isin(good)
    wt, _ = CL._fit_conditional_logit(z[m], tr_ok.index.to_numpy()[m], yw[m], l2=1e-6)
    so = rk.predict(oos[cols].astype("float64"))
    u = ((so - sm) / ss) * float(wt[0])
    p = CL._predict_within_race(u, oos.index.to_numpy())
    umb = pd.to_numeric(oos["馬番"], errors="coerce").to_numpy()
    return {(str(r), int(x)): float(v)
            for r, x, v in zip(oos.index.astype(str), umb, p.to_numpy()) if x == x and v == v}


def main():
    ap = argparse.ArgumentParser(description="アンサンブル・スタッキング OOS 評価（市場＋3ヘッド 対数プール）")
    ap.add_argument("--years", type=int, nargs="+", required=True, metavar="YYYY")
    ap.add_argument("--train-years", type=int, nargs="+", default=None, metavar="YYYY")
    ap.add_argument("--relevance", choices=["top3", "top6"], default="top6")
    ap.add_argument("--l2", type=float, default=10.0, help="条件付きロジットの L2（既定 10）")
    ap.add_argument("--folds", type=int, default=5, help="メタのクロスfit分割数（OOS 年内・race単位）")
    ap.add_argument("--n-estimators", type=int, default=300)
    ap.add_argument("--version", default=None)
    args = ap.parse_args()

    import numpy as np
    import pandas as pd

    import conditional_logit as CL
    from lgbm_ranker import _relevance
    from app._data_loader import (
        find_model_paths, load_latest_model, load_model_by_version, load_win_head_for,
    )
    from app._model_eval import load_featured_data
    from src.policies._score_policy import ExpectedValueScorePolicy, META_COLS
    from src.simulation._edge_diagnostic import build_edge_frame, _actual_win, _win_logloss

    featured = load_featured_data()
    if featured is None or featured.empty or "着順" not in featured.columns:
        print("featured_data / 着順 がありません")
        return

    year = featured.index.astype(str).str[:4]
    oos_set = {str(y) for y in args.years}
    if args.train_years:
        train_set = {str(y) for y in args.train_years}
    else:
        cutoff = min(int(y) for y in oos_set)
        train_set = {y for y in set(year.unique()) if y.isdigit() and int(y) < cutoff}
    train_set -= oos_set
    tr = featured[year.isin(train_set)]
    oos = featured[year.isin(oos_set)]
    if tr.empty or oos.empty:
        print(f"学習 {sorted(train_set)} / OOS {sorted(oos_set)} のどちらかが空")
        return

    drop_cols = set(META_COLS) | {"着順", "rank", "rank_win"}
    cols = CL._select_feature_matrix(featured, drop_cols, no_odds=False)

    print(f"ベース学習中（学習={sorted(train_set)} 馬={len(tr):,} 特徴={len(cols)}）…")
    clogit = _clogit_map(CL, tr, oos, cols, args.l2)
    print("  条件付きロジット OK")
    ranker = _ranker_map(CL, _relevance, tr, oos, cols, args.relevance, args.n_estimators)
    print("  LGBMRanker OK")

    # 市場 & LightGBM
    model = load_model_by_version(args.version) if args.version else load_latest_model()
    if model is None:
        print("比較用 LightGBM が見つかりません")
        return
    paths = find_model_paths("models")
    win_ai = load_win_head_for(paths[0]) if paths else None
    score_model = (win_ai or model).effective_model
    head = "win-head" if win_ai is not None else "place-head"

    won = _actual_win(oos)
    edge = build_edge_frame(ExpectedValueScorePolicy.calc(score_model, oos), won.to_numpy())
    edge["p_clogit"] = [clogit.get((str(r), int(u)), np.nan)
                        for r, u in zip(edge.index.astype(str), edge["umaban"])]
    edge["p_ranker"] = [ranker.get((str(r), int(u)), np.nan)
                        for r, u in zip(edge.index.astype(str), edge["umaban"])]

    bases = ["p_mkt", "r_hat", "p_clogit", "p_ranker"]
    e = edge.dropna(subset=bases + ["won"]).copy()
    if e.empty:
        print("全ベースが揃う馬がありません")
        return

    # メタ特徴 = log(base 勝率)。レース内 softmax(Σ w_k log p_k) = 対数プール。
    EPS = 1e-9
    Xmeta = np.log(np.clip(e[bases].to_numpy(dtype="float64"), EPS, 1.0))
    y = e["won"].to_numpy(dtype="float64")
    rid = e.index.to_numpy()

    # OOS 年内 race 単位 K-fold クロスfit（メタが評価レースを見ない）
    races = pd.Index(pd.unique(e.index.astype(str)))
    fold_of = {r: i % args.folds for i, r in enumerate(races)}
    fold_arr = np.array([fold_of[str(r)] for r in rid])
    p_stack = np.full(len(e), np.nan)
    wsum = np.zeros(len(bases)); nfit = 0
    for k in range(args.folds):
        tr_m = fold_arr != k
        te_m = fold_arr == k
        # 学習fold: 勝ち馬のあるレースのみ
        wpr = pd.Series(y[tr_m], index=rid[tr_m]).groupby(level=0).sum()
        good = set(wpr[wpr >= 1].index)
        gm = np.array([str(r) in good for r in rid[tr_m]])
        w, _ = CL._fit_conditional_logit(Xmeta[tr_m][gm], rid[tr_m][gm], y[tr_m][gm], l2=1.0)
        wsum += w; nfit += 1
        u_te = Xmeta[te_m] @ w
        p_stack[te_m] = CL._predict_within_race(
            u_te, rid[te_m]).to_numpy()
    e["p_stack"] = p_stack
    wavg = wsum / max(nfit, 1)

    # スコアボード（全モデル同一集合）
    print("=" * 74)
    print(f"スタッキング OOS 評価（学習={sorted(train_set)} → 評価={sorted(oos_set)} / "
          f"レース={e.index.nunique():,} 馬={len(e):,} / LGBM={head} / rel={args.relevance}）")
    print(f"  {'予測器':<20}{'勝logloss':>11}{'Brier':>10}{'ECE':>9}{'AUC':>8}")
    print("-" * 74)
    rows = [("市場 p_mkt", "p_mkt"), ("LightGBM r_hat", "r_hat"),
            ("条件付ロジット", "p_clogit"), ("LGBMRanker", "p_ranker"),
            ("★合成 stack", "p_stack")]
    best_ll = None
    for name, col in rows:
        p = e[col]
        ll = _win_logloss(p, e["won"])
        brier = float(np.nanmean((p.to_numpy() - e["won"].to_numpy()) ** 2))
        ece = CL._ece(p, e["won"])
        try:
            from sklearn.metrics import roc_auc_score
            msk = p.notna() & e["won"].notna()
            auc = roc_auc_score(e["won"][msk], p[msk])
        except Exception:  # noqa: BLE001
            auc = float("nan")
        print(f"  {name:<20}{ll:>11.4f}{brier:>10.5f}{ece:>9.4f}{auc:>8.4f}")
    print("-" * 74)
    print("  合成の対数プール重み（クロスfit平均・大きいほど寄与大）:")
    for b, wv in zip(bases, wavg):
        print(f"    {b:<10} w={wv:>7.3f}")
    print("-" * 74)
    print("  ※ ★合成が市場・各ヘッドの勝logloss を下回れば、アンサンブルが較正を改善（市場併走型の前進）。")
    print("     重みが市場に集中し合成≈市場なら、現データでは上乗せ無し（form 充足後に再評価）。")
    print("=" * 74)


if __name__ == "__main__":
    main()
