"""LGBMRanker（LambdaMART / lambdarank）ヘッドの最小プロトタイプ — OOS 品質比較。

PyCon2018 トークの定石: 分類ではなく **レース内の順位付け（NDCG 最適化）** を学習する。
同じ LightGBM でも目的関数が違うのでスコアの性格が変わり、アンサンブル多様性の source になる。

relevance（関連度）は「馬券に絡む上位だけを評価」する定義を既定にする（トークの「馬券外は0」）:
  - top3 : 1着=3, 2着=2, 3着=1, それ以外=0（既定）
  - top6 : max(0, 6-着順)（1着=5 … 6着=0）

ランカーの生スコアは確率ではないため、**学習データでレース内 softmax の温度を1個較正**して
（条件付きロジットの最尤機構を1特徴で流用）勝率に変換し、市場・LightGBM と同じ土俵で
勝logloss/Brier/ECE/AUC/echo を比較する。

**リーク回避**: 学習は OOS 年より前のみ。特徴量は scoring と同じ META/リーク列を除外。

実行:
  python lgbm_ranker.py --years 2026
  python lgbm_ranker.py --years 2026 --relevance top6 --no-odds-features
"""

from __future__ import annotations

import argparse
import sys
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
warnings.filterwarnings("ignore", message="X does not have valid feature names", category=UserWarning)


def _relevance(rank, scheme):
    """着順(数値) → relevance ラベル（非負整数）。NaN 着順は 0。"""
    import numpy as np

    r = rank.to_numpy()
    if scheme == "top6":
        rel = np.clip(6 - r, 0, 5)
    else:  # top3
        rel = np.select([r == 1, r == 2, r == 3], [3, 2, 1], default=0)
    rel = np.where(np.isnan(r), 0, rel)
    return rel.astype(int)


def main():
    ap = argparse.ArgumentParser(description="LGBMRanker(LambdaMART) OOS 品質比較")
    ap.add_argument("--years", type=int, nargs="+", required=True, metavar="YYYY",
                    help="OOS 評価年（この年は学習に使わない）")
    ap.add_argument("--train-years", type=int, nargs="+", default=None, metavar="YYYY")
    ap.add_argument("--relevance", choices=["top3", "top6"], default="top3",
                    help="関連度ラベル（既定 top3=馬券圏のみ評価）")
    ap.add_argument("--no-odds-features", action="store_true")
    ap.add_argument("--n-estimators", type=int, default=300)
    ap.add_argument("--num-leaves", type=int, default=31)
    ap.add_argument("--learning-rate", type=float, default=0.05)
    ap.add_argument("--version", default=None, help="比較用 LightGBM のバージョン（既定は最新）")
    args = ap.parse_args()

    import lightgbm as lgb
    import numpy as np
    import pandas as pd

    import conditional_logit as CL  # _ece / _select_feature_matrix / _fit_conditional_logit / _predict_within_race を流用
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
    if "着順" not in featured.columns:
        print("着順 列がありません（ランキング学習に必須）")
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
        print(f"学習 {sorted(train_set)} / OOS {sorted(oos_set)} のどちらかが空です")
        return

    drop_cols = set(META_COLS) | {"着順", "rank", "rank_win"}
    cols = CL._select_feature_matrix(featured, drop_cols, args.no_odds_features)
    if not cols:
        print("使える数値特徴量がありません")
        return

    # --- ランカー学習（レース単位 group。着順 NaN 行は除外）---
    tr_rank = pd.to_numeric(tr["着順"], errors="coerce")
    tr_ok = tr[tr_rank.notna()].sort_index(kind="stable")  # group を連続させるため race_id でソート
    y_rel = _relevance(pd.to_numeric(tr_ok["着順"], errors="coerce"), args.relevance)
    group_sizes = tr_ok.groupby(level=0, sort=False).size().to_numpy()
    X_tr = tr_ok[cols].astype("float64")  # nullable→float64（NaN は LightGBM が扱う）

    print(f"学習: {sorted(train_set)} レース={len(group_sizes):,} 馬={len(X_tr):,} 特徴={len(cols)} "
          f"relevance={args.relevance} no_odds={args.no_odds_features} … LGBMRanker 学習中")
    ranker = lgb.LGBMRanker(
        objective="lambdarank", n_estimators=args.n_estimators, num_leaves=args.num_leaves,
        learning_rate=args.learning_rate, min_child_samples=50,
        subsample=0.8, subsample_freq=1, colsample_bytree=0.8,
        random_state=0, n_jobs=-1, verbose=-1,
    )
    ranker.fit(X_tr, y_rel, group=group_sizes)

    # --- ランカー生スコア → レース内 softmax 温度を train で較正（1特徴の最尤）---
    s_tr = ranker.predict(X_tr)
    s_mean, s_std = float(np.mean(s_tr)), float(np.std(s_tr)) or 1.0
    z_tr = ((s_tr - s_mean) / s_std).reshape(-1, 1)
    y_win_tr = _actual_win(tr_ok).to_numpy()
    win_per_race = pd.Series(y_win_tr, index=tr_ok.index).groupby(level=0).sum()
    good = set(win_per_race[win_per_race >= 1].index)
    m = tr_ok.index.isin(good)
    w_temp, _ = CL._fit_conditional_logit(z_tr[m], tr_ok.index.to_numpy()[m], y_win_tr[m], l2=1e-6)
    print(f"  温度較正: softmax 係数 w={float(w_temp[0]):.3f}")

    # --- OOS 予測 → 温度較正して勝率へ ---
    s_oos = ranker.predict(oos[cols].astype("float64"))
    u_oos = ((s_oos - s_mean) / s_std) * float(w_temp[0])
    p_rows = CL._predict_within_race(u_oos, oos.index.to_numpy())
    umb = pd.to_numeric(oos["馬番"], errors="coerce").to_numpy()
    rank_map = {(str(r), int(u)): float(p)
                for r, u, p in zip(oos.index.astype(str), umb, p_rows.to_numpy())
                if u == u and p == p}

    # --- 市場 & 既存 LightGBM ---
    model = load_model_by_version(args.version) if args.version else load_latest_model()
    if model is None:
        print("比較用 LightGBM が見つかりません")
        return
    win_ai = None
    paths = find_model_paths("models")
    if paths:
        win_ai = load_win_head_for(paths[0])
    score_model = (win_ai or model).effective_model
    head = "win-head" if win_ai is not None else "place-head"

    won = _actual_win(oos)
    table = ExpectedValueScorePolicy.calc(score_model, oos)
    edge = build_edge_frame(table, won.to_numpy())
    edge["p_ranker"] = [rank_map.get((str(r), int(u)), np.nan)
                        for r, u in zip(edge.index.astype(str), edge["umaban"])]

    print("=" * 72)
    print(f"OOS 品質比較（学習={sorted(train_set)} → 評価={sorted(oos_set)} / "
          f"レース={edge.index.nunique():,} 馬={len(edge):,} / LGBM={head} / rel={args.relevance}）")
    print(f"  {'予測器':<20}{'勝logloss':>11}{'Brier':>10}{'ECE':>9}{'AUC':>8}{'echo(vs市場)':>13}")
    print("-" * 72)
    for name, col in [("市場 p_mkt", "p_mkt"), ("LightGBM r_hat", "r_hat"),
                      ("LGBMRanker", "p_ranker")]:
        p = edge[col]
        ll = _win_logloss(p, edge["won"])
        brier = float(np.nanmean((p.to_numpy() - edge["won"].to_numpy()) ** 2))
        ece = CL._ece(p, edge["won"])
        try:
            from sklearn.metrics import roc_auc_score
            msk = p.notna() & edge["won"].notna()
            auc = roc_auc_score(edge["won"][msk], p[msk])
        except Exception:  # noqa: BLE001
            auc = float("nan")
        d = edge.dropna(subset=[col, "p_mkt"])
        echo = (float(np.corrcoef(d[col], d["p_mkt"])[0, 1])
                if len(d) > 2 and d[col].std() > 0 and d["p_mkt"].std() > 0 else float("nan"))
        echo_s = "  —" if col == "p_mkt" else f"{echo:>13.3f}"
        print(f"  {name:<20}{ll:>11.4f}{brier:>10.5f}{ece:>9.4f}{auc:>8.4f}{echo_s}")
    print("-" * 72)
    print("  ※ 勝logloss/ECE 小=良。echo 低=市場と独立（アンサンブル寄与大）。")
    print("     ランカーは NDCG 最適化なので順位付け(AUC)に強く出やすい。")
    print("=" * 72)


if __name__ == "__main__":
    main()
