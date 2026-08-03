"""OBJ_COMPARE: 目的関数比較（BINARY/LAMBDARANK/XENDCG/RACE_SOFTMAX_CE）を development で回す。

docs/objective_comparison_design.md（凍結）に従う。development(2015-2024) walk-forward OOF・同一特徴・
label=winner-only・全 arm を同一 fold で学習し、レース内 softmax 勝率の **LogLoss（nats/race）**・NDCG@1,@3・
年別再現性を出す。判定は PRIMARY=RACE_SOFTMAX_CE−BINARY(m=1)、SECONDARY={LAMBDARANK,XENDCG}−BINARY
(Holm m=2) の ΔLogLoss を venue×日 block bootstrap paired CI で。

selection 域のみ（2025+ は fail-closed）。採否は dev で確定しない＝有望目的が出たら凍結し 2027 で一度だけ。
要ローカル（featured＋lightgbm）。純部は src/training/_listwise_objective.py（tests 済）。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.training._temporal_split import filter_selection_domain, rolling_folds  # noqa: E402

ARMS = ["BINARY", "LAMBDARANK", "XENDCG", "RACE_SOFTMAX_CE"]
_DROP = {"rank", "rank_win", "着順", "単勝", "date", "horse_id", "通過"}
_LGB_PARAMS = dict(num_leaves=31, learning_rate=0.05, min_child_samples=50, verbose=-1,
                   n_estimators=300, feature_fraction=0.8, bagging_fraction=0.8, bagging_freq=1)


def _group_sizes(race_ids):
    """race ラベル配列（race 連続前提）→ 各 race の頭数リスト（LGBMRanker group 用）。"""
    import numpy as np
    _, first, cnt = np.unique(np.asarray(race_ids), return_index=True, return_counts=True)
    return cnt[np.argsort(first)]


def _build_matrix(featured):
    """featured(JRA development) → (df: 数値特徴＋レース内z＋log q、_race/_year/_winner/_rank/_odds), feat_cols。"""
    import numpy as np
    import pandas as pd
    from src.constants._model_category import central_index_mask
    from src.constants._results_cols import ResultsCols

    year = pd.to_numeric(pd.Series(featured.index.astype(str)).str[:4], errors="coerce")
    keep = central_index_mask(featured.index) & (year >= 2015).to_numpy() & (year <= 2024).to_numpy()
    f = featured[keep].copy()
    race = pd.Series(f.index.astype(str), index=f.index)
    odds = pd.to_numeric(f.get(ResultsCols.TANSHO_ODDS), errors="coerce")
    rank = pd.to_numeric(f.get(ResultsCols.RANK), errors="coerce")
    num = f.select_dtypes(include=[np.number])
    base_cols = [c for c in num.columns if str(c) not in _DROP and not str(c).startswith("rank")]
    X = num[base_cols].copy()
    g = race.to_numpy()
    for c in base_cols:                                # レース内 z-score（相対特徴）
        s = X[c]
        mu = s.groupby(g).transform("mean")
        sd = s.groupby(g).transform("std").replace(0, np.nan)
        X[c + "_z"] = ((s - mu) / sd).fillna(0.0)
    q = (1.0 / odds).groupby(g).transform(lambda v: v / v.sum())   # 市場 implied（レース内正規化）
    X["log_q"] = np.log(q.clip(lower=1e-6))            # market-informed 特徴（係数は学習＝anchor でない）
    feat_cols = list(X.columns)
    X = X.fillna(0.0)
    X["_race"] = g
    X["_year"] = pd.to_numeric(race.str[:4], errors="coerce").to_numpy()
    X["_winner"] = (rank == 1).astype(float).to_numpy()
    X["_rank"] = rank.to_numpy()
    X["_odds"] = odds.to_numpy()
    return X.reset_index(drop=True), feat_cols


def _fit_scores(arm, Xtr, ytr, gtr, Xte):
    """arm を学習し (test raw score, train raw score) を返す（確率化は呼出側）。lightgbm 依存。"""
    import lightgbm as lgb
    from src.training._listwise_objective import make_race_softmax_fobj
    if arm == "BINARY":
        m = lgb.LGBMClassifier(objective="binary", **_LGB_PARAMS).fit(Xtr, ytr)
        return m.predict_proba(Xte)[:, 1], m.predict_proba(Xtr)[:, 1]
    if arm in ("LAMBDARANK", "XENDCG"):
        obj = "lambdarank" if arm == "LAMBDARANK" else "rank_xendcg"
        m = lgb.LGBMRanker(objective=obj, **_LGB_PARAMS)
        m.fit(Xtr, ytr.astype(int), group=list(_group_sizes(gtr)))
        return m.predict(Xte), m.predict(Xtr)
    if arm == "RACE_SOFTMAX_CE":
        booster = lgb.train({**_LGB_PARAMS, "objective": make_race_softmax_fobj(gtr)},
                            lgb.Dataset(Xtr, label=ytr), num_boost_round=_LGB_PARAMS["n_estimators"])
        return booster.predict(Xte), booster.predict(Xtr)
    raise ValueError(arm)


def _race_probs(arm, sc_te, sc_tr, gte, ytr, gtr):
    """arm の raw score をレース内勝率へ（BINARY=再正規化 / softmax=直接 / ranker=OOF temperature）。"""
    import numpy as np
    from src.training._listwise_objective import fit_race_temperature, race_softmax_probs
    if arm == "BINARY":
        return race_softmax_probs(np.log(np.clip(sc_te, 1e-9, 1.0)), gte)   # p_i/Σp = softmax(log p)
    if arm == "RACE_SOFTMAX_CE":
        return race_softmax_probs(sc_te, gte)
    T = fit_race_temperature(sc_tr, ytr, gtr)          # ranker: train OOF から単一 T>0
    return race_softmax_probs(sc_te / T, gte)


def main() -> int:
    import numpy as np
    from app._model_eval import load_featured_data
    from src.simulation._model_compare import block_bootstrap_ci
    from src.training._listwise_objective import ndcg_at_k

    ap = argparse.ArgumentParser(description="目的関数比較（development・selection 域・非証拠）")
    ap.add_argument("--featured", default=None)
    ap.add_argument("--first-eval-year", type=int, default=2018)
    ap.add_argument("--n-boot", type=int, default=10000)
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    print("=" * 92)
    print("OBJ_COMPARE 目的関数比較（development 2015-2024 walk-forward OOF・selection 域・⚠採否は 2027）")
    feat = load_featured_data(args.featured)
    if feat is None or feat.empty:
        print("[エラー] featured を読めません（ローカルで実行）", file=sys.stderr)
        return 2
    df, feat_cols = _build_matrix(feat)
    df = df.sort_values("_race").reset_index(drop=True)          # ranker group 用に race 連続化
    _, used_years = filter_selection_domain([{"year": int(y)} for y in df["_year"].dropna().unique()])
    folds = rolling_folds(used_years, args.first_eval_year)
    if not folds:
        print("[STOP] fold が作れない。", file=sys.stderr)
        return 3
    print(f"[設定] features={len(feat_cols)}  arms={ARMS}  "
          f"folds={[(f'{min(t)}-{max(t)}', e) for t, e in folds]}")

    nll = {a: [] for a in ARMS}                          # per-race winner −log p（arm 間で同 race 順）
    nd1, nd3 = {a: [] for a in ARMS}, {a: [] for a in ARMS}
    blocks = []
    for tr_years, ey in folds:
        tr = df[df["_year"].isin(tr_years)]
        te = df[df["_year"] == ey]
        if tr.empty or te.empty:
            continue
        Xtr, Xte = tr[feat_cols].to_numpy(), te[feat_cols].to_numpy()
        ytr, gtr, gte = tr["_winner"].to_numpy(), tr["_race"].to_numpy(), te["_race"].to_numpy()
        win_mask = te["_winner"].to_numpy() == 1
        blocks.append([str(r)[:10] for r in te["_race"].to_numpy()[win_mask]])
        rel = np.clip(4.0 - te["_rank"].fillna(99).to_numpy(), 0, 3)   # NDCG relevance（参考）
        for a in ARMS:
            sc_te, sc_tr = _fit_scores(a, Xtr, ytr, gtr, Xte)
            p = _race_probs(a, sc_te, sc_tr, gte, ytr, gtr)
            nll[a].append(list(-np.log(np.asarray(p)[win_mask] + 1e-12)))   # winner NLL（race 順）
            nd1[a].append(ndcg_at_k(rel, p, gte, 1))
            nd3[a].append(ndcg_at_k(rel, p, gte, 3))
        print(f"  fold {min(tr_years)}-{max(tr_years)}→{ey}: races={int(win_mask.sum()):,} 学習完了")

    flat = {a: np.concatenate([np.asarray(x) for x in nll[a]]) if nll[a] else np.asarray([]) for a in ARMS}
    blk = np.concatenate([np.asarray(b) for b in blocks]) if blocks else np.asarray([])
    print(f"\n[絶対（{len(blk):,} races）]  {'arm':>16}{'LogLoss':>10}{'NDCG@1':>9}{'NDCG@3':>9}")
    for a in ARMS:
        print(f"  {'':<10}{a:>16}{float(flat[a].mean()):>10.5f}"
              f"{float(np.nanmean(nd1[a])):>9.4f}{float(np.nanmean(nd3[a])):>9.4f}")

    def paired(a):
        return block_bootstrap_ci(list(flat[a] - flat["BINARY"]), list(blk),
                                  n_boot=max(2000, args.n_boot), seed=args.seed)

    print("\n[判定 ΔLogLoss vs BINARY（venue×日 block bootstrap paired・負=改善・較正後）]")
    pb = paired("RACE_SOFTMAX_CE")
    prim_ok = pb["hi"] < 0 and pb["mean"] <= -0.001
    print(f"  PRIMARY(m=1) RACE_SOFTMAX_CE: ΔLogLoss={pb['mean']:+.5f} "
          f"95%CI[{pb['lo']:+.5f},{pb['hi']:+.5f}] → {'有意改善(候補)' if prim_ok else '有意な改善なし'}")
    from scripts.run_jrdb42_confirm import holm_reject
    sec = {a: paired(a) for a in ("LAMBDARANK", "XENDCG")}
    holm = holm_reject({a: sec[a].get("p_improve", 1.0) for a in sec})
    print("  SECONDARY(Holm m=2):")
    for a in ("LAMBDARANK", "XENDCG"):
        h = holm[a]
        print(f"    {a:<14} ΔLogLoss={sec[a]['mean']:+.5f} 95%CI[{sec[a]['lo']:+.5f},{sec[a]['hi']:+.5f}] "
              f"Holm p={h['p']:.4f} thr={h['threshold']:.4f} reject={h['reject']}")

    print("\n[年別再現性] fold 別 ΔLogLoss(RACE_SOFTMAX_CE−BINARY) の符号:")
    signs = [float(np.mean(a) - np.mean(b)) for a, b in zip(nll["RACE_SOFTMAX_CE"], nll["BINARY"])]
    neg = sum(1 for s in signs if s < 0)
    print(f"  改善(負) {neg}/{len(signs)} fold（過半 {'○' if neg * 2 > len(signs) else '×'}）  "
          f"fold別={[round(s, 4) for s in signs]}")

    print("\n" + "=" * 92)
    print("⚠ selection（development）＝採否確定でない。有望目的が出たら features/objective/hyperparam/seed/")
    print("  温度手順を凍結し 2027 reserved tranche で一度だけ確認（Holm 更新）。ROI は非証拠で別途。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
