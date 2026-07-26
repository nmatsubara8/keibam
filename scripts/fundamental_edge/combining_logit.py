"""ファンダメンタル模型の「単勝と直交する情報」を OOS ΔR² で実測（Benter 型 combining logit）。

各出走馬のリークセーフなファンダ特徴（過去走の相対着順/勝率/経験/間隔/賞金＋斤量/馬体重/年齢/枠。
**市場由来のオッズ・人気は除外**）で条件付きロジット f を学習し、市場含意 π(単勝) と combining:
  u_i = α·log f_i + β·log π_i
を train で当て、test の McFadden R² を market-only と比較。ΔR²=直交情報。プール別(JRA/NAR)。
"""
from __future__ import annotations

import argparse
import re

import numpy as np
import pandas as pd
from scipy.optimize import minimize

FUND = ["form_avg5", "form_best5", "win_rate", "place_rate", "log_nprev",
        "log_days", "log_prize", "kinryo", "horse_weight", "age", "draw_rel", "has_hist"]


def sexage_to_age(s):
    m = re.search(r"(\d+)", str(s))
    return float(m.group(1)) if m else np.nan


def features(runners, hist):
    hist = hist.dropna(subset=["date", "rank", "field_size"]).copy()
    hist["date"] = hist["date"].astype(int)
    hist["rel"] = (hist["rank"] - 1) / (hist["field_size"] - 1).clip(lower=1)
    hist = hist.sort_values("date")
    by_h = {hid: g for hid, g in hist.groupby("horse_id")}

    rows = []
    for r in runners.itertuples():
        rd = int(r.date)
        g = by_h.get(str(r.horse_id))
        prev = g[g["date"] < rd] if g is not None else None
        if prev is None or len(prev) == 0:
            f = dict(form_avg5=0.5, form_best5=0.5, win_rate=0.0, place_rate=0.0,
                     log_nprev=0.0, log_days=np.log(365.0), log_prize=0.0, has_hist=0.0)
        else:
            last5 = prev.tail(5)
            f = dict(
                form_avg5=float(last5["rel"].mean()),
                form_best5=float(last5["rel"].min()),
                win_rate=float((prev["rank"] == 1).mean()),
                place_rate=float((prev["rank"] <= 3).mean()),
                log_nprev=float(np.log1p(len(prev))),
                log_days=float(np.log(max(1, rd_minus(rd, int(prev["date"].max()))))),
                log_prize=float(np.log1p(last5["prize"].fillna(0).mean())),
                has_hist=1.0,
            )
        f.update(
            kinryo=float(r.kinryo) if not pd.isna(r.kinryo) else 55.0,
            horse_weight=float(r.horse_weight) if not pd.isna(r.horse_weight) else 470.0,
            age=sexage_to_age(r.sexage) if not pd.isna(sexage_to_age(r.sexage)) else 4.0,
            draw_rel=float(r.umaban) / float(r.field_size),
            race_id=r.race_id, date=rd, win=int(r.rank == 1),
            p_mkt_raw=1.0 / float(r.tansho),
        )
        rows.append(f)
    df = pd.DataFrame(rows)
    df["p_mkt"] = df["p_mkt_raw"] / df.groupby("race_id")["p_mkt_raw"].transform("sum")
    return df


def rd_minus(a, b):
    from datetime import date
    da = date(a // 10000, (a // 100) % 100, a % 100)
    db = date(b // 10000, (b // 100) % 100, b % 100)
    return (da - db).days


def races_of(df, feat_cols):
    out = []
    for _rid, g in df.groupby("race_id", sort=False):
        y = np.where(g["win"].values == 1)[0]
        if len(y) != 1:
            continue
        out.append((g[feat_cols].values.astype(float), int(y[0]), int(g["date"].iloc[0])))
    return out


def cl_fit(races):
    k = races[0][0].shape[1]

    def nll_grad(theta):
        nll = 0.0
        g = np.zeros(k)
        for X, y, _ in races:
            u = X @ theta
            u -= u.max()
            e = np.exp(u)
            p = e / e.sum()
            nll -= np.log(max(p[y], 1e-300))
            g += X.T @ p - X[y]
        return nll, g

    res = minimize(nll_grad, np.zeros(k), jac=True, method="L-BFGS-B")
    return res.x


def cl_nll(races, theta):
    nll = 0.0
    for X, y, _ in races:
        u = X @ theta
        u -= u.max()
        e = np.exp(u)
        nll -= np.log(max((e / e.sum())[y], 1e-300))
    return nll


def null_nll(races):
    return sum(np.log(X.shape[0]) for X, _, _ in races)


def cl_predict(df, feat_cols, theta):
    pred = np.zeros(len(df))
    for _, idx in df.groupby("race_id", sort=False).groups.items():
        rows = df.loc[idx]
        u = rows[feat_cols].values.astype(float) @ theta
        u -= u.max()
        e = np.exp(u)
        pred[[df.index.get_loc(i) for i in idx]] = e / e.sum()
    return pred


def run_pool(name, runners, hist):
    df = features(runners, hist).reset_index(drop=True)
    # standardize fundamental features by TRAIN stats (split by date median)
    cutoff = df["date"].quantile(0.7)
    tr_mask = df["date"] <= cutoff
    mu, sd = df.loc[tr_mask, FUND].mean(), df.loc[tr_mask, FUND].std().replace(0, 1)
    dfz = df.copy()
    dfz[FUND] = (df[FUND] - mu) / sd

    # 1) fundamental CL on train
    tr = dfz[tr_mask]
    races_tr_f = races_of(tr, FUND)
    theta_f = cl_fit(races_tr_f)
    dfz["f_pred"] = np.clip(cl_predict(dfz, FUND, theta_f), 1e-6, 1)
    dfz["log_f"] = np.log(dfz["f_pred"])
    dfz["log_pmkt"] = np.log(np.clip(df["p_mkt"].values, 1e-6, 1))

    te = dfz[~tr_mask]
    races_tr_m = races_of(dfz[tr_mask], ["log_pmkt"])
    races_tr_c = races_of(dfz[tr_mask], ["log_f", "log_pmkt"])
    races_te_m = races_of(te, ["log_pmkt"])
    races_te_c = races_of(te, ["log_f", "log_pmkt"])

    if not races_te_c or not races_tr_c:
        print(f"\n[{name}] test/train races 不足 (train={len(races_tr_c)}, test={len(races_te_c)}) — skip")
        return None
    races_tr_fo = races_of(dfz[tr_mask], ["log_f"])
    races_te_fo = races_of(te, ["log_f"])
    th_m = cl_fit(races_tr_m)
    th_c = cl_fit(races_tr_c)
    th_fo = cl_fit(races_tr_fo)
    nn = null_nll(races_te_m)
    r2_m = 1 - cl_nll(races_te_m, th_m) / nn
    r2_c = 1 - cl_nll(races_te_c, th_c) / nn
    r2_fo = 1 - cl_nll(races_te_fo, th_fo) / nn
    echo = np.corrcoef(te["log_f"], te["log_pmkt"])[0, 1]

    print(f"\n{'='*60}\n[{name}]  train races≈{len(races_tr_c)}  test races≈{len(races_te_c)}\n{'='*60}")
    print(f"  OOS McFadden R²(fundamental only) = {r2_fo:.4f}  ← ファンダ単独の予測力")
    print(f"  OOS McFadden R²(market only)   = {r2_m:.4f}")
    print(f"  OOS McFadden R²(market + fund) = {r2_c:.4f}")
    print(f"  ΔR² (単勝と直交するファンダ情報) = {r2_c - r2_m:+.4f}")
    print(f"  combining weights: α(fund)={th_c[0]:+.3f}  β(market)={th_c[1]:+.3f}")
    print(f"  echo corr(log f, log π) on test = {echo:.3f}")
    dll = (cl_nll(races_te_m, th_m) - cl_nll(races_te_c, th_c)) / len(races_te_c)
    print(f"  Δ平均対数尤度/レース = {dll:+.4f}  (>0でファンダが上乗せ)")
    return r2_c - r2_m


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--jra-runners")
    ap.add_argument("--nar-runners")
    ap.add_argument("--hist", required=True)
    args = ap.parse_args()
    hist = pd.read_csv(args.hist)
    hist["horse_id"] = hist["horse_id"].astype(str)
    for name, rf in [("JRA(中央=効率市場)", args.jra_runners), ("NAR(地方=部分非効率)", args.nar_runners)]:
        if not rf:
            continue
        runners = pd.read_csv(rf)
        runners["horse_id"] = runners["horse_id"].astype(str)
        run_pool(name, runners, hist)
    print("\n読み取り: ΔR²≈0 → ファンダは単勝と直交する情報を持たない(市場が織り込み済)＝公開データ NO-GO。")
    print("ΔR²>0かつ控除超なら初の本物の候補。Benter は香港低効率プールで ΔR²≈0.0178。")


if __name__ == "__main__":
    main()
