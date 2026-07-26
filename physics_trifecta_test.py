"""物理MC三連単モデル（Stage A）— コース/ペース物理の着順分布は Harville を超えるか。

三連単の順序(2/3着)は物理現象（ペース・スタミナ・位置取り・距離ロス）。統計的 Harville は
その貧弱な近似。ここでは agent_race 物理MC を **JRDB で駆動**し、着順の**joint分布**（Harville が
表せない着位間相関＝物理由来）から三連単確率を出し、実着順(SED)の listwise NLL で Harville と比較する。

市場アンカー: sim の能力 ability を市場単勝(log q)に合わせて較正し、**P(1着)は効率的市場に一致**させる。
物理が価値を出すのは条件付き 2/3着のみ、という設計（単勝は効率的・連系順序に非効率、の仮説）。

JRDB → RaceField:
  ability   = 1 + a_scale·z(log q_market)   （train で a_scale/ability_sigma を市場勝率一致に較正）
  style     = JRDB 脚質(1逃2先→front / 3差5好位→stalker / 4追6自在→closer)
  pace_intensity = ペース予想 H/M/S（H>1 前傾→終盤失速→差し台頭）
  stamina   = agari_idx(上がり指数=終盤脚) 由来 / noise = 出遅率 由来 / gate = 枠(馬番)

実行: python physics_trifecta_test.py --jrdb-dir /tmp/jrdb_all --limit 1500 --n-sim 1500
"""
from __future__ import annotations

import argparse
import glob

import numpy as np
import pandas as pd

from src.jrdb._parser import parse
from src.policies._harville import prob_trifecta, prob_trifecta_place_strength
from src.policies._market_residual import market_probs
from src.simulation._agent_race import RaceField, SimConfig, monte_carlo

_CENTRAL = {f"{i:02d}" for i in range(1, 11)}
# JRDB 脚質コード → sim スタイル名
_STYLE = {1: "front", 2: "front", 3: "stalker", 5: "stalker", 4: "closer", 6: "closer"}
_HMS_PACE = {"H": 1.12, "M": 1.0, "S": 0.9}


def load(jrdb_dir: str, limit: int = 0) -> list[dict]:
    """SED×KYI → レース列（market q / 実top3 / 物理入力: style/pace/agari/deokure/umaban）。"""
    kcols = ["race_id", "umaban", "kyakushitsu", "pace_yosou", "agari_idx", "deokure_rate"]
    sed = pd.concat([parse(f, "SED")[["race_id", "umaban", "kakutei_tansho", "chakujun"]]
                     for f in sorted(glob.glob(f"{jrdb_dir}/SED*.txt"))], ignore_index=True)
    kyi = pd.concat([parse(f, "KYI")[kcols]
                     for f in sorted(glob.glob(f"{jrdb_dir}/KYI*.txt"))], ignore_index=True)
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
    return races


def build_field(r: dict, a_scale: float) -> tuple[RaceField, list[int], float]:
    """レース → (RaceField, 馬番順, pace_intensity)。ability は log q を a_scale で。"""
    umas = r["umas"]
    q = r["q"]
    logq = np.array([np.log(max(q[u], 1e-9)) for u in umas])
    z = (logq - logq.mean()) / (logq.std() + 1e-6)
    ability = 1.0 + a_scale * z                       # 市場勝率アンカー（P1着を市場へ）
    styles, agari, deokure, gate, pace_votes = [], [], [], [], []
    for u in umas:
        row = r["rec"][u]
        ks = row.get("kyakushitsu")
        try:
            styles.append(_STYLE.get(int(ks), "stalker"))
        except (TypeError, ValueError):
            styles.append("stalker")
        agari.append(pd.to_numeric(row.get("agari_idx"), errors="coerce"))
        deokure.append(pd.to_numeric(row.get("deokure_rate"), errors="coerce"))
        gate.append(u)
        pv = str(row.get("pace_yosou", "")).strip()
        if pv in _HMS_PACE:
            pace_votes.append(_HMS_PACE[pv])
    style_codes = np.array([{"front": 0, "stalker": 1, "closer": 2}[s] for s in styles])
    ag = pd.Series(agari).astype(float)
    stamina = 1.0 + 0.15 * ((ag - ag.mean()) / (ag.std() + 1e-6)).fillna(0.0).to_numpy()
    dk = pd.Series(deokure).astype(float)
    noise = 0.05 * (1.0 + 0.5 * ((dk - dk.mean()) / (dk.std() + 1e-6)).fillna(0.0).clip(-1, 2).to_numpy())
    g = np.array(gate, dtype=float)
    gate_n = (g - g.min()) / (g.max() - g.min() + 1e-6)   # 枠 0..1
    pace_intensity = float(np.mean(pace_votes)) if pace_votes else 1.0
    field = RaceField(ability=np.clip(ability, 0.3, None), style=style_codes,
                      stamina=np.clip(stamina, 0.4, None), noise=np.clip(noise, 0.02, None),
                      gate=gate_n)
    return field, umas, pace_intensity


def sim_place_strength(r, a_scale, n_sim, beta, seed=0):
    """物理MC → **市場アンカー place 強度**を返す。P(1着)=市場のまま、2/3着の順序だけ物理で。

    sim の複勝率(P_top3) を「place 強度」に使う。ただし絶対較正のズレを避けるため、
    market_win を基準に **sim が示す over/under-place（複勝propensity − 勝率相当）** を
    beta で載せる: place_strength_i ∝ q_i · exp(beta · z(logit(P_sim_place_i))).
    beta=0 で Harville（place=win）に一致＝帰無。
    """
    field, umas, pace = build_field(r, a_scale)
    cfg = SimConfig(pace_intensity=pace, turn_k=0.01)
    out = monte_carlo(field, n_sim=n_sim, cfg=cfg, seed=seed, ability_sigma=0.2)
    idx = {u: i for i, u in enumerate(umas)}
    q = r["q"]
    pl = out["place"]                                    # sim 複勝率 P(top3)
    lp = np.array([np.log((pl[idx[u]] + 1e-3) / (1 - pl[idx[u]] + 1e-3)) for u in umas])
    zlp = (lp - lp.mean()) / (lp.std() + 1e-6)
    place_strength = {}
    for i, u in enumerate(umas):
        place_strength[u] = q[u] * np.exp(beta * zlp[i])
    win = out["win"]
    sim_win = {u: float(win[idx[u]]) for u in umas}
    return place_strength, sim_win


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--jrdb-dir", default="/tmp/jrdb_all")
    ap.add_argument("--limit", type=int, default=1500)
    ap.add_argument("--n-sim", type=int, default=1500)
    ap.add_argument("--train-frac", type=float, default=0.5)
    args = ap.parse_args()

    races = load(args.jrdb_dir, limit=args.limit)
    n_tr = int(len(races) * args.train_frac)
    tr, te = races[:n_tr], races[n_tr:]
    print(f"レース: {len(races):,}（train {len(tr)} / test {len(te)}）n_sim={args.n_sim}")

    # a_scale を train で較正: sim 勝率が市場勝率に最も一致する値（sim の物理を市場帯へ）
    best_a, best_err = 1.0, 1e9
    for a in (0.8, 1.2, 1.6, 2.2, 3.0):
        err = 0.0
        for r in tr[:100]:
            _, sw = sim_place_strength(r, a, max(500, args.n_sim // 2), 0.0)
            err += sum(abs(sw[u] - r["q"][u]) for u in r["umas"])
        if err < best_err:
            best_err, best_a = err, a
    print(f"市場アンカー較正: a_scale={best_a}（sim勝率↔市場勝率 乖離最小）")

    # beta を train で較正（三連単NLL最小・beta=0 は Harville 帰無）
    best_b, best_n = 0.0, 1e18
    for b in (-0.3, -0.15, 0.0, 0.15, 0.3):
        tot = 0.0
        for r in tr[:250]:
            ps, _ = sim_place_strength(r, best_a, args.n_sim, b)
            tot -= np.log(max(prob_trifecta_place_strength(r["q"], ps, *r["top3"]), 1e-12))
        if tot < best_n:
            best_n, best_b = tot, b
    print(f"物理 place 強度の較正: beta={best_b}（train三連単NLL最小・0=Harville）")

    # Stage A: 物理 place 強度（市場アンカー）vs Harville で実 top3 の NLL＋placebo
    rng = np.random.default_rng(0)
    nll_phys, nll_harv, nll_plac = [], [], []
    for r in te:
        ps, _ = sim_place_strength(r, best_a, args.n_sim, best_b)
        pp = prob_trifecta_place_strength(r["q"], ps, *r["top3"])
        ph = prob_trifecta(r["q"], *r["top3"])
        # placebo: 物理 place 強度をレース内シャッフル（市場q基準は保つ）→ 信号破壊
        keys = list(ps)
        vals = [ps[u] / r["q"][u] for u in keys]  # 物理由来の乗数だけ抜く
        rng.shuffle(vals)
        ps_p = {u: r["q"][u] * v for u, v in zip(keys, vals, strict=False)}
        pp_p = prob_trifecta_place_strength(r["q"], ps_p, *r["top3"])
        nll_phys.append(-np.log(max(pp, 1e-12)))
        nll_harv.append(-np.log(max(ph, 1e-12)))
        nll_plac.append(-np.log(max(pp_p, 1e-12)))
    nll_phys, nll_harv, nll_plac = map(np.array, (nll_phys, nll_harv, nll_plac))
    d = nll_phys - nll_harv
    boot = np.array([d[rng.integers(0, len(d), len(d))].mean() for _ in range(1000)])
    ci = (float(np.percentile(boot, 2.5)), float(np.percentile(boot, 97.5)))
    print("\n== Stage A: 物理MC(市場アンカーplace強度) vs Harville（実top3 NLL）==")
    print(f"  Harville NLL={nll_harv.mean():.4f} → 物理MC={nll_phys.mean():.4f}"
          f"  ΔNLL={d.mean():+.5f} CI95=({ci[0]:+.5f},{ci[1]:+.5f})")
    print(f"  → {'物理が Harville を有意に上回る' if ci[1] < 0 else '有意でない（CI 0跨ぎ）'}")
    print(f"  placebo(物理signalシャッフル): ΔNLL={(nll_plac - nll_harv).mean():+.5f}"
          f"  → {'帰無OK（改善消失＝物理が本物）' if (nll_plac - nll_harv).mean() > -0.005 else 'placeboでも改善（artifact疑い）'}")


if __name__ == "__main__":
    main()
