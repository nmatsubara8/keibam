"""確率モデル比較の事前定義評価（Step3 以降の成功判定ハーネス）。

Mixture-PL のような自由度追加は **ROI 単独で判断しない**。ベースライン（例: β=0 の Step1）
に対して以下を必ず全部見る:
    ΔNLL（listwise・proper scoring） / ΔBrier / ΔECE（較正）＋ Bootstrap CI ＋ LRT
    ＋ 予測区間較正（interval_calibration・Step4 の能力分散 N(μ,σ²) の被覆率検査）
特に較正: Mixture で「NLL だけ改善して較正が悪化」が起こり得るため ΔECE を独立に監視する。

成功条件（事前定義・後知恵の閾値調整禁止）:
    (1) ΔNLL < 0（挑戦側が改善）
    (2) 有意性: Bootstrap 95% CI の上端 < 0、または LRT p < 0.05
    (3) 較正が悪化しない: ΔECE ≤ +0.005
    (4) ROI は _pnl_objective.evaluate_pnl で別途確認（判定材料だが単独判断はしない）

入力はレース列と「レース→勝率dict」の関数2つ（ベースライン/挑戦側）。純粋計算のみ。
"""
from __future__ import annotations

import math
from typing import Callable, Mapping, Sequence

import numpy as np


def race_nll(probs: Mapping[int, float], winner: int) -> float:
    """1レースの listwise NLL = −log P[winner]。P=0 は 30.0（クリップ）。"""
    p = float(probs.get(winner, 0.0))
    return -math.log(p) if p > 0 else 30.0


def race_brier(probs: Mapping[int, float], winner: int) -> float:
    """1レースの Brier = Σ_h (P_h − y_h)² / 頭数（y=1着指示）。"""
    if not probs:
        return 1.0
    return sum((float(p) - (1.0 if h == winner else 0.0)) ** 2 for h, p in probs.items()) / len(probs)


def ece(probs_list: Sequence[float], outcomes: Sequence[int], n_bins: int = 10) -> float:
    """Expected Calibration Error（馬単位の予測勝率 vs 実勝敗、等幅ビン）。

    ECE = Σ_b (n_b/N)·|mean(p)_b − mean(y)_b|。較正が完全なら 0。
    """
    p = np.asarray(probs_list, dtype=float)
    y = np.asarray(outcomes, dtype=float)
    if len(p) == 0:
        return 0.0
    edges = np.linspace(0.0, 1.0, n_bins + 1)
    total = 0.0
    for i in range(n_bins):
        m = (p >= edges[i]) & (p < edges[i + 1] if i < n_bins - 1 else p <= edges[i + 1])
        if m.sum() == 0:
            continue
        total += (m.sum() / len(p)) * abs(p[m].mean() - y[m].mean())
    return float(total)


def interval_calibration(
    means: Sequence[float],
    variances: Sequence[float],
    observed: Sequence[float],
    levels: Sequence[float] = (0.5, 0.8, 0.95),
) -> dict:
    """予測区間較正（Prediction Interval Calibration）— ガウス予測 N(μ,σ²) vs 実観測。

    能力フィルタの predictive() が出す (μ, σ²) が観測 y をどれだけ正しく覆うかを検査する。
    各名目水準 L について経験被覆率 = mean( |y−μ|/σ < z_L ) を計り、名目との差を出す。
    併せて PIT（Φ((y−μ)/σ)＝較正が完全なら一様分布）の平均/分散のずれも返す。

    分散を持つ意味はここで検証される: σ を過小申告（過信）すると被覆率が名目を割り、
    その状態で Kelly に流すと過大賭けになる。coverage_gap_max ≤ 0.05 程度を合格目安とする。

    Returns: {"coverage": {L: 経験被覆率}, "coverage_gap_max": max|経験−名目|,
              "pit_mean": (理想0.5), "pit_var": (理想1/12≈0.0833), "n": 件数}
    """
    from statistics import NormalDist

    nd = NormalDist()
    mu = np.asarray(means, dtype=float)
    sd = np.sqrt(np.asarray(variances, dtype=float))
    y = np.asarray(observed, dtype=float)
    ok = sd > 0
    mu, sd, y = mu[ok], sd[ok], y[ok]
    if len(y) == 0:
        return {"coverage": {}, "coverage_gap_max": float("nan"),
                "pit_mean": float("nan"), "pit_var": float("nan"), "n": 0}
    z = np.abs(y - mu) / sd
    coverage = {}
    gap = 0.0
    for lv in levels:
        z_l = nd.inv_cdf(0.5 + lv / 2.0)
        emp = float((z < z_l).mean())
        coverage[lv] = emp
        gap = max(gap, abs(emp - lv))
    pit = np.array([nd.cdf(v) for v in (y - mu) / sd])
    return {"coverage": coverage, "coverage_gap_max": float(gap),
            "pit_mean": float(pit.mean()), "pit_var": float(pit.var()), "n": int(len(y))}


def compare_models(
    races: Sequence[Mapping],
    baseline_fn: Callable[[Mapping], dict[int, float]],
    challenger_fn: Callable[[Mapping], dict[int, float]],
    *,
    k_extra_params: int = 0,
    n_boot: int = 1000,
    seed: int = 0,
) -> dict:
    """ベースライン vs 挑戦側をレース列で比較し、事前定義の成功判定込みで返す。

    races 各要素は {"odds": ..., "winner": 馬番, ...}（モデル関数がそのまま受ける辞書）。
    k_extra_params は挑戦側の追加自由度（LRT の df。例: Mixture β表=12）。

    Returns（主要キー）:
        nll_base/nll_chal, d_nll（挑戦−基準・負=改善）, d_nll_ci95（Bootstrap）,
        lrt_stat/lrt_p, brier_*, d_brier, ece_*, d_ece, n_races, success（bool）
    """
    from src.policies._market_residual import kl_from_market
    from src.policies._market_residual import market_probs

    nll_b, nll_c, br_b, br_c, kl_b, kl_c = [], [], [], [], [], []
    pb_flat: list[float] = []
    pc_flat: list[float] = []
    y_flat: list[int] = []
    for r in races:
        w = r.get("winner")
        if w is None:
            continue
        pb = baseline_fn(r)
        pc = challenger_fn(r)
        if not pb or not pc:
            continue
        nll_b.append(race_nll(pb, w))
        nll_c.append(race_nll(pc, w))
        br_b.append(race_brier(pb, w))
        br_c.append(race_brier(pc, w))
        if r.get("odds"):  # VOI: 市場からの情報獲得量 D_KL(P‖q)
            q = market_probs(r["odds"])
            kl_b.append(kl_from_market(pb, q))
            kl_c.append(kl_from_market(pc, q))
        for h in pb:
            pb_flat.append(float(pb[h]))
            pc_flat.append(float(pc.get(h, 0.0)))
            y_flat.append(1 if h == w else 0)
    n = len(nll_b)
    if n == 0:
        return {"n_races": 0, "success": False}

    a_b, a_c = float(np.mean(nll_b)), float(np.mean(nll_c))
    d = np.asarray(nll_c) - np.asarray(nll_b)  # 負=改善
    rng = np.random.default_rng(seed)
    boots = np.array([d[rng.integers(0, n, n)].mean() for _ in range(n_boot)])
    ci = (float(np.percentile(boots, 2.5)), float(np.percentile(boots, 97.5)))

    # LRT（入れ子モデル前提: β=0 がベースライン）。2N·(NLL0−NLL1) 〜 χ²(df)
    lrt = 2.0 * n * (a_b - a_c)
    try:
        from scipy.stats import chi2

        lrt_p = float(chi2.sf(max(lrt, 0.0), max(k_extra_params, 1)))
    except Exception:  # noqa: BLE001 — scipy 不在は p 無し
        lrt_p = float("nan")

    e_b = ece(pb_flat, y_flat)
    e_c = ece(pc_flat, y_flat)
    d_nll = a_c - a_b
    d_ece = e_c - e_b
    significant = ci[1] < 0.0 or (not math.isnan(lrt_p) and lrt_p < 0.05 and d_nll < 0)
    return {
        "n_races": n,
        "nll_base": a_b, "nll_chal": a_c, "d_nll": d_nll, "d_nll_ci95": ci,
        "lrt_stat": float(lrt), "lrt_p": lrt_p,
        "brier_base": float(np.mean(br_b)), "brier_chal": float(np.mean(br_c)),
        "d_brier": float(np.mean(br_c) - np.mean(br_b)),
        "ece_base": e_b, "ece_chal": e_c, "d_ece": d_ece,
        # VOI: 市場からの情報獲得量（nats）。ΔKL>0=市場に対する新情報が増えた
        # （正しさは success 側の ΔNLL が担保。KL 単独では過学習でも増える点に注意）
        "kl_market_base": float(np.mean(kl_b)) if kl_b else float("nan"),
        "kl_market_chal": float(np.mean(kl_c)) if kl_c else float("nan"),
        "d_kl_market": float(np.mean(kl_c) - np.mean(kl_b)) if kl_b else float("nan"),
        # 事前定義の成功条件: 改善・有意・較正非悪化（ROI は別途 evaluate_pnl で確認）
        "success": bool(d_nll < 0 and significant and d_ece <= 0.005),
    }
