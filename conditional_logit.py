"""条件付きロジット（レース内 softmax 最尤）ヘッドの最小プロトタイプ — OOS 品質比較。

目的: GBDT(LightGBM) とは帰納バイアスが直交する「確率的・レース内 Σ=1」モデルを1つ用意し、
市場・LightGBM と並べて **勝ち馬 logloss / Brier / ECE / AUC** を OOS で比較する。
Benter モデルの本流（条件付きロジット＋市場合成）に相当する土台。

モデル: レース r の馬 i の効用 u_i = w·x_i、勝率 P(win_i)=softmax_r(u)。
グローバル切片はレース内 softmax で相殺され識別不能なので持たない（特徴は z 標準化）。
学習は L2 正則化つき負の対数尤度を L-BFGS で最小化（解析勾配 X^T(p−y)+2λw）。

**リーク回避**: 学習は必ず OOS 年より前の年のみ（--years で指定した年は評価専用）。特徴量からは
着順/rank/rank_win/単勝/horse_id/date/馬番/枠番 を除外（既存 META_COLS と同じ）。

実行:
  python conditional_logit.py --years 2025 2026            # 2025-26 を OOS 評価、それ以前で学習
  python conditional_logit.py --years 2026 --no-odds-features --l2 5.0
"""

from __future__ import annotations

import argparse
import sys
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
warnings.filterwarnings("ignore", message="X does not have valid feature names", category=UserWarning)


def calc_score_table(score_model, X):
    """ExpectedValueScorePolicy.calc を呼び、特徴数不一致は retrain を促す明確なメッセージに変換する。

    rebuild-featured で featured の列が増える/減ると、学習済みモデルの期待特徴数とズレて
    predict_proba が ValueError になる。その場合は「retrain が必要」と案内して終了する。
    """
    from src.policies._score_policy import ExpectedValueScorePolicy

    try:
        return ExpectedValueScorePolicy.calc(score_model, X)
    except ValueError as e:  # noqa: BLE001
        if "features" in str(e):
            raise SystemExit(
                f"特徴数不一致: {e}\n"
                "→ rebuild-featured で featured の列が変わっています。先に retrain してください:\n"
                "   python -m src.pipeline.run_pipeline retrain --holdout-years <OOS年>\n"
                "  （例: --holdout-years 2026 → その後この評価を再実行）"
            ) from e
        raise


def _ece(prob, won, n_bins=10):
    """Expected Calibration Error（等頻度ビンで |平均予測 − 実勝率| を加重平均）。"""
    import numpy as np
    import pandas as pd

    s = pd.DataFrame({"p": np.asarray(prob, float), "y": np.asarray(won, float)}).dropna()
    if s.empty:
        return float("nan")
    s["bin"] = pd.qcut(s["p"].rank(method="first"), min(n_bins, len(s)), labels=False)
    err = 0.0
    for _, g in s.groupby("bin"):
        err += len(g) / len(s) * abs(g["p"].mean() - g["y"].mean())
    return float(err)


def _select_feature_matrix(featured, drop_cols, no_odds):
    """featured から条件付きロジット用の数値特徴列名を決める（学習・評価で共通に使う）。"""
    from src.constants._feature_cols import ODDS_DERIVED_FEATURE_COLS

    num = featured.select_dtypes(include="number")
    cols = [c for c in num.columns if c not in drop_cols]
    if no_odds:
        cols = [c for c in cols if c not in set(ODDS_DERIVED_FEATURE_COLS)]
    return cols


def _fit_conditional_logit(X, race_ids, y_win, l2):
    """レース内 softmax の負の対数尤度を L-BFGS で最小化し重み w を返す（解析勾配）。

    X: (n,d) 標準化済み / race_ids: (n,) レースid / y_win: (n,) 1着=1。
    勝ち馬のいないレースは呼び出し側で除外済みとする。
    """
    import numpy as np
    from scipy.optimize import minimize

    order = np.argsort(race_ids.astype(str), kind="stable")
    Xs = np.ascontiguousarray(X[order], dtype="float64")
    ys = y_win[order].astype("float64")
    race_sorted = race_ids.astype(str)[order]
    _, starts = np.unique(race_sorted, return_index=True)
    starts = np.sort(starts)
    seg_sizes = np.diff(np.append(starts, len(race_sorted)))

    n_winner = float(ys.sum())

    def obj(w):
        u = Xs @ w
        seg_max = np.maximum.reduceat(u, starts)
        u_shift = u - np.repeat(seg_max, seg_sizes)
        exp_u = np.exp(u_shift)
        seg_sumexp = np.add.reduceat(exp_u, starts)
        lse = np.log(seg_sumexp) + seg_max
        p = exp_u / np.repeat(seg_sumexp, seg_sizes)
        nll = float(np.sum(lse) - np.dot(u, ys)) + l2 * float(w @ w)
        grad = Xs.T @ (p - ys) + 2.0 * l2 * w
        # 尺度をレース数で割らず raw 合計（l2 はその尺度に合わせて選ぶ）
        return nll, grad

    w0 = np.zeros(Xs.shape[1])
    res = minimize(obj, w0, jac=True, method="L-BFGS-B",
                   options={"maxiter": 500, "maxfun": 5000})
    return res.x, {"nll": res.fun, "n_winner": n_winner, "converged": bool(res.success)}


def _predict_within_race(X, race_ids):
    """学習済みは呼び出し側で X@w 済みの効用を渡す想定でなく、ここでは softmax を race 単位で行う。"""
    import numpy as np
    import pandas as pd

    u = pd.Series(X, index=race_ids)
    # レースごとに softmax（数値安定化）
    def _sm(g):
        m = g.max()
        e = np.exp(g - m)
        s = e.sum()
        return e / s if s > 0 else e * np.nan
    return u.groupby(level=0, group_keys=False).transform(_sm)


def main():
    ap = argparse.ArgumentParser(description="条件付きロジット OOS 品質比較（市場 vs LightGBM vs 条件付きロジット）")
    ap.add_argument("--years", type=int, nargs="+", required=True, metavar="YYYY",
                    help="OOS 評価年（この年は学習に使わない。例: 2025 2026）")
    ap.add_argument("--train-years", type=int, nargs="+", default=None, metavar="YYYY",
                    help="学習年を明示（既定: OOS 最小年より前の全年）")
    ap.add_argument("--l2", type=float, default=10.0, help="L2 正則化係数（既定 10.0）")
    ap.add_argument("--no-odds-features", action="store_true",
                    help="オッズ由来特徴を除外（市場エコーを避けて独立性を見る）")
    ap.add_argument("--version", default=None, help="比較用 LightGBM のバージョン（既定は最新）")
    args = ap.parse_args()

    import numpy as np
    import pandas as pd

    from app._data_loader import (
        find_model_paths, load_latest_model, load_model_by_version, load_win_head_for,
    )
    from app._model_eval import load_featured_data
    from src.policies._score_policy import ExpectedValueScorePolicy, META_COLS
    from src.simulation._edge_diagnostic import build_edge_frame, _actual_win, _win_logloss

    featured = load_featured_data()
    if featured is None or featured.empty:
        print("featured_data がありません（先に rebuild-featured）")
        return
    if "着順" not in featured.columns and "rank_win" not in featured.columns:
        print("勝敗列（着順 / rank_win）がありません")
        return

    year = featured.index.astype(str).str[:4]
    oos_set = {str(y) for y in args.years}
    if args.train_years:
        train_set = {str(y) for y in args.train_years}
    else:
        cutoff = min(int(y) for y in oos_set)
        train_set = {y for y in set(year.unique()) if y.isdigit() and int(y) < cutoff}
    train_set -= oos_set  # 念のため OOS 年は学習から除く

    tr = featured[year.isin(train_set)]
    oos = featured[year.isin(oos_set)]
    if tr.empty:
        print(f"学習年 {sorted(train_set)} のレースがありません（--train-years 指定を検討）")
        return
    if oos.empty:
        print(f"OOS 年 {sorted(oos_set)} のレースがありません")
        return

    # --- 特徴量行列（学習で fit → 学習/OOS 共通に適用）---
    drop_cols = set(META_COLS) | {"着順", "rank", "rank_win"}
    cols = _select_feature_matrix(featured, drop_cols, args.no_odds_features)
    if not cols:
        print("使える数値特徴量がありません")
        return

    med = tr[cols].median(numeric_only=True)
    mean = tr[cols].fillna(med).mean()
    std = tr[cols].fillna(med).std().replace(0, np.nan)
    keep = std.dropna().index.tolist()  # 分散0列は落とす
    med, mean, std = med[keep], mean[keep], std[keep]

    def _design(df):
        # nullable 型（Int64/Float64）が混ざると object 配列になり np.exp が落ちるため float64 に明示変換
        z = (df[keep].astype("float64").fillna(med) - mean) / std
        return z.fillna(0.0).to_numpy(dtype="float64")

    # 学習: 勝ち馬のあるレースのみ
    y_tr = _actual_win(tr).to_numpy()
    win_per_race = pd.Series(y_tr, index=tr.index).groupby(level=0).sum()
    good_races = set(win_per_race[win_per_race >= 1].index)
    tr_mask = tr.index.isin(good_races)
    X_tr = _design(tr[tr_mask])
    rid_tr = tr.index.to_numpy()[tr_mask]
    y_tr = y_tr[tr_mask]

    print(f"学習: {sorted(train_set)} レース={len(good_races):,} 馬={len(X_tr):,} 特徴={len(keep)} "
          f"（l2={args.l2}, no_odds={args.no_odds_features}）… 最尤推定中")
    w, info = _fit_conditional_logit(X_tr, rid_tr, y_tr, args.l2)
    print(f"  収束={info['converged']} NLL={info['nll']:.1f}")

    # --- OOS 予測（条件付きロジット）---
    u_oos = _design(oos) @ w
    p_clogit_rows = _predict_within_race(u_oos, oos.index.to_numpy())  # index=race_id
    # (race_id, 馬番) -> p のマップ
    umb = pd.to_numeric(oos["馬番"], errors="coerce").to_numpy()
    clogit_map = {(str(r), int(u)): float(p)
                  for r, u, p in zip(oos.index.astype(str), umb, p_clogit_rows.to_numpy())
                  if u == u and p == p}

    # --- 市場 & LightGBM（既存パイプライン）---
    if args.version:
        model = load_model_by_version(args.version)
    else:
        model = load_latest_model()
    if model is None:
        print("比較用 LightGBM が見つかりません")
        return
    # Win ヘッドがあれば優先（真の勝率）
    win_ai = None
    paths = find_model_paths("models")
    if paths:
        win_ai = load_win_head_for(paths[0])
    score_model = (win_ai or model).effective_model
    head = "win-head" if win_ai is not None else "place-head"

    won = _actual_win(oos)
    table = calc_score_table(score_model, oos)
    edge = build_edge_frame(table, won.to_numpy())
    edge["p_clogit"] = [clogit_map.get((str(r), int(u)), np.nan)
                        for r, u in zip(edge.index.astype(str), edge["umaban"])]

    # --- スコアボード ---
    print("=" * 72)
    print(f"OOS 品質比較（学習={sorted(train_set)} → 評価={sorted(oos_set)} / "
          f"レース={edge.index.nunique():,} 馬={len(edge):,} / LGBM={head}）")
    print(f"  {'予測器':<20}{'勝logloss':>11}{'Brier':>10}{'ECE':>9}{'AUC':>8}{'echo(vs市場)':>13}")
    print("-" * 72)
    for name, col in [("市場 p_mkt", "p_mkt"), ("LightGBM r_hat", "r_hat"),
                      ("条件付ロジット", "p_clogit")]:
        p = edge[col]
        ll = _win_logloss(p, edge["won"])
        brier = float(np.nanmean((p.to_numpy() - edge["won"].to_numpy()) ** 2))
        ece = _ece(p, edge["won"])
        try:
            from sklearn.metrics import roc_auc_score
            m = p.notna() & edge["won"].notna()
            auc = roc_auc_score(edge["won"][m], p[m])
        except Exception:  # noqa: BLE001
            auc = float("nan")
        d = edge.dropna(subset=[col, "p_mkt"])
        echo = (float(np.corrcoef(d[col], d["p_mkt"])[0, 1])
                if len(d) > 2 and d[col].std() > 0 and d["p_mkt"].std() > 0 else float("nan"))
        echo_s = "  —" if col == "p_mkt" else f"{echo:>13.3f}"
        print(f"  {name:<20}{ll:>11.4f}{brier:>10.5f}{ece:>9.4f}{auc:>8.4f}{echo_s}")
    print("-" * 72)
    print("  ※ 勝logloss が小さいほど良い（市場が基準）。ECE 小=較正良好。")
    print("     echo(vs市場) が低いほど市場と独立（アンサンブル寄与が大きい）。")
    print("=" * 72)


if __name__ == "__main__":
    main()
