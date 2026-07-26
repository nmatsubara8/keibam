"""ベイズ×物理MC三連単 — 過去成績のKalman能力事後(μ,σ)を物理MCに伝播する（本物のベイズ更新）。

これまでの物理MCは σ を固定(0.2)にしていた＝ベイズ更新ではなかった（ユーザー指摘）。本スクリプトは
各馬の能力を**過去成績から逐次ベイズ更新**（_ability_filter の Kalman）し、その事後分散 σ² を
per-horse の不確実性として物理MCに伝播する。

仮説の核: 市場は μ（P1着）を効率的に価格づけする（単勝は帰無）。だが市場が着順に織り込んで
いない可能性があるのは **σ（能力の不確実性）**。少走・休み明け・上がり馬は μ 同じでも σ 大きく、
着順分布(2/3着)が広がる。Harville も固定σ物理もこれを表せない。**ベイズ σ を物理MCに伝播**すれば
市場が着順に織り込まない不確実性構造を突ける — ①ベイズ更新×②物理MCの本当の統合。

設計: ability μ=市場log q（P1着アンカー）。noise=Kalman σ（過去成績由来・as-of forward-safe）。
ability_sigma=1.0 で各simが N(μ, σ) から能力を引く＝事後を伝播。

Stage A: Harville / 固定σ物理 / ベイズσ物理 の3者を実top3 NLL で比較（OOS＋placebo）。

実行: python bayes_physics_trifecta.py --jrdb-dir /tmp/jrdb_all --limit 2000 --n-sim 1200
"""
from __future__ import annotations

import argparse
import glob

import numpy as np
import pandas as pd

from src.jrdb._parser import parse
from src.policies._ability_filter import KalmanAbilityFilter, performance_from_rank
from src.policies._harville import prob_trifecta, prob_trifecta_place_strength
from src.policies._market_residual import market_probs
from src.simulation._agent_race import RaceField, SimConfig, monte_carlo

_CENTRAL = {f"{i:02d}" for i in range(1, 11)}
_STYLE = {1: "front", 2: "front", 3: "stalker", 5: "stalker", 4: "closer", 6: "closer"}
_HMS = {"H": 1.12, "M": 1.0, "S": 0.9}


def build_ability_sigma(sed: pd.DataFrame) -> dict:
    """各 (race_id, ketto) の能力事後標準偏差 σ を Kalman で as-of 更新して返す。

    馬(ketto)ごとに ymd 昇順で、過去走の着順→性能 y=performance_from_rank を逐次 update。
    対象レースの σ は「その走の**前まで**の事後」＝リーク無し（predict で経過日数ぶん σ 拡大）。
    """
    flt = KalmanAbilityFilter(prior_mu=0.0, prior_var=1.0, q_mu_per_day=1e-3,
                              rho_per_day=0.98, stationary_c_var=0.2, obs_var=0.5)
    n_by_race = sed.groupby(sed["race_id"].astype(str))["umaban"].transform("size")
    d = sed.assign(_n=n_by_race).copy()
    d["_ymd"] = pd.to_datetime(d["ymd"], format="%Y%m%d", errors="coerce")
    d = d.dropna(subset=["ketto", "chakujun", "_ymd"])
    out: dict = {}
    for ketto, g in d.groupby("ketto"):
        g = g.sort_values("_ymd")
        st = flt.initial()
        prev_day = None
        for rid, ymd, rk, n in zip(g["race_id"].astype(str), g["_ymd"], g["chakujun"],
                                   g["_n"], strict=False):
            days = 30.0 if prev_day is None else max(0.0, (ymd - prev_day).days)
            st_pred = flt.predict(st, days)
            out[(rid, str(ketto))] = float(np.sqrt(max(flt.variance(st_pred), 1e-6)))  # as-of σ
            y = performance_from_rank(int(rk), int(n))     # この走の性能で更新（次走以降に反映）
            st = flt.update(st_pred, y)
            prev_day = ymd
    return out


def load(jrdb_dir: str, limit: int = 0):
    """SED×KYI → レース列 ＋ 能力σ辞書。"""
    kcols = ["race_id", "umaban", "kyakushitsu", "pace_yosou", "agari_idx"]
    sed = pd.concat([parse(f, "SED")[["race_id", "umaban", "ketto", "ymd",
                                      "kakutei_tansho", "chakujun"]]
                     for f in sorted(glob.glob(f"{jrdb_dir}/SED*.txt"))], ignore_index=True)
    kyi = pd.concat([parse(f, "KYI")[kcols]
                     for f in sorted(glob.glob(f"{jrdb_dir}/KYI*.txt"))], ignore_index=True)
    ab_sigma = build_ability_sigma(sed)
    m = sed.merge(kyi, on=["race_id", "umaban"], how="inner").dropna(
        subset=["kakutei_tansho", "chakujun"])
    m = m[m["kakutei_tansho"] > 1.0]
    m = m[m["race_id"].astype(str).str[4:6].isin(_CENTRAL)]
    races = []
    for rid, g in m.groupby(m["race_id"].astype(str)):
        g = g.dropna(subset=["chakujun"])
        if len(g) < 6:
            continue
        q = market_probs({int(u): float(o) for u, o in
                          zip(g["umaban"], g["kakutei_tansho"], strict=False)})
        if len(q) < 6:
            continue
        top3 = [int(x) for x in g.sort_values("chakujun")["umaban"].head(3)]
        if len(set(top3)) < 3 or any(t not in q for t in top3):
            continue
        umas = [int(u) for u in g["umaban"] if int(u) in q]
        rec = {int(u): row for u, row in zip(g["umaban"], g.to_dict("records"), strict=False)}
        races.append({"rid": str(rid), "q": q, "top3": tuple(top3), "umas": umas, "rec": rec})
        if limit and len(races) >= limit:
            break
    return races, ab_sigma


def place_strength(r, ab_sigma, n_sim, beta, *, bayes_noise, a_scale=0.8, seed=0):
    """物理MC → 市場アンカー place 強度。bayes_noise=True で noise を Kalman σ にする。"""
    umas = r["umas"]
    q = r["q"]
    logq = np.array([np.log(max(q[u], 1e-9)) for u in umas])
    z = (logq - logq.mean()) / (logq.std() + 1e-6)
    ability = 1.0 + a_scale * z
    styles, gate, pace_votes, noise = [], [], [], []
    for u in umas:
        row = r["rec"][u]
        try:
            styles.append(_STYLE.get(int(row.get("kyakushitsu")), "stalker"))
        except (TypeError, ValueError):
            styles.append("stalker")
        gate.append(u)
        pv = str(row.get("pace_yosou", "")).strip()
        if pv in _HMS:
            pace_votes.append(_HMS[pv])
        if bayes_noise:
            sg = ab_sigma.get((r["rid"], str(row.get("ketto"))))
            noise.append(sg if sg is not None else np.nan)
    style_codes = np.array([{"front": 0, "stalker": 1, "closer": 2}[s] for s in styles])
    g = np.array(gate, dtype=float)
    gate_n = (g - g.min()) / (g.max() - g.min() + 1e-6)
    if bayes_noise:
        nz = pd.Series(noise).astype(float)
        nz = nz.fillna(nz.mean() if nz.notna().any() else 0.05)
        # Kalman σ を物理MCの加速度ノイズへ写像（相対スケール・平均0.05へ正規化）
        noise_arr = 0.05 * (nz / max(nz.mean(), 1e-6)).clip(0.3, 3.0).to_numpy()
    else:
        noise_arr = np.full(len(umas), 0.05)
    field = RaceField(ability=np.clip(ability, 0.3, None), style=style_codes,
                      stamina=np.ones(len(umas)), noise=noise_arr, gate=gate_n)
    cfg = SimConfig(pace_intensity=float(np.mean(pace_votes)) if pace_votes else 1.0, turn_k=0.01)
    out = monte_carlo(field, n_sim=n_sim, cfg=cfg, seed=seed, ability_sigma=1.0)
    idx = {u: i for i, u in enumerate(umas)}
    pl = out["place"]
    lp = np.array([np.log((pl[idx[u]] + 1e-3) / (1 - pl[idx[u]] + 1e-3)) for u in umas])
    zlp = (lp - lp.mean()) / (lp.std() + 1e-6)
    return {u: q[u] * np.exp(beta * zlp[i]) for i, u in enumerate(umas)}


def fit_beta(tr, ab_sigma, n_sim, bayes_noise):
    best_b, best_n = 0.0, 1e18
    for b in (-0.3, -0.15, 0.0, 0.15, 0.3):
        tot = 0.0
        for r in tr[:250]:
            ps = place_strength(r, ab_sigma, n_sim, b, bayes_noise=bayes_noise)
            tot -= np.log(max(prob_trifecta_place_strength(r["q"], ps, *r["top3"]), 1e-12))
        if tot < best_n:
            best_n, best_b = tot, b
    return best_b


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--jrdb-dir", default="/tmp/jrdb_all")
    ap.add_argument("--limit", type=int, default=2000)
    ap.add_argument("--n-sim", type=int, default=1200)
    ap.add_argument("--train-frac", type=float, default=0.5)
    args = ap.parse_args()

    races, ab_sigma = load(args.jrdb_dir, limit=args.limit)
    cov = np.mean([any((r["rid"], str(r["rec"][u].get("ketto"))) in ab_sigma for u in r["umas"])
                   for r in races])
    n_tr = int(len(races) * args.train_frac)
    tr, te = races[:n_tr], races[n_tr:]
    print(f"レース {len(races):,}（train {len(tr)} / test {len(te)}）能力σ被覆 {cov*100:.0f}% n_sim={args.n_sim}")

    nll_h = np.array([-np.log(max(prob_trifecta(r["q"], *r["top3"]), 1e-12)) for r in te])
    rng = np.random.default_rng(0)
    for bayes in (False, True):
        b = fit_beta(tr, ab_sigma, args.n_sim, bayes)
        nll = []
        for r in te:
            ps = place_strength(r, ab_sigma, args.n_sim, b, bayes_noise=bayes)
            nll.append(-np.log(max(prob_trifecta_place_strength(r["q"], ps, *r["top3"]), 1e-12)))
        nll = np.array(nll)
        d = nll - nll_h
        boot = np.array([d[rng.integers(0, len(d), len(d))].mean() for _ in range(1000)])
        ci = (float(np.percentile(boot, 2.5)), float(np.percentile(boot, 97.5)))
        tag = "ベイズσ物理" if bayes else "固定σ物理 "
        print(f"  {tag}: beta={b:+.2f}  NLL={nll.mean():.4f}  ΔNLL(vs Harville)={d.mean():+.5f}"
              f"  CI95=({ci[0]:+.5f},{ci[1]:+.5f})")
    print(f"  （Harville NLL={nll_h.mean():.4f}）")
    print("\n→ ベイズσ物理 が 固定σ物理 より ΔNLL を伸ばせば「能力不確実性が着順に効く」の証拠。")


if __name__ == "__main__":
    main()
