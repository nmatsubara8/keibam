"""市場アンカー残差ヘッド r_θ(x) の学習（Step1・線形・market-anchored listwise NLL＋L2）。

_market_residual の定義:  s_i = log q_i + r_i,  P = softmax(s),  r≡0 → P≡q（帰無＝市場）。
本モジュールは r_i = θ·x_i（線形）を、勝者の market-anchored listwise NLL 最小化で fit する:
    L(θ) = mean_race[ −log softmax(log q + Xθ)[winner] ] + λ‖θ‖²
λ‖θ‖² が「市場から離れるには証拠を要求する」正則化（過学習防止の要）。**特徴集合と λ は事前登録**
（結果を見て選ばない）。特徴はレース内 z-score 済みを渡す前提（スケール依存を除く）。

θ≡0 で厳密に市場へ退化するため、baseline(market) vs challenger(market+residual) の入れ子比較に使える。
scipy 不在/失敗は θ≡0（fail-soft＝市場）に落ちる。純粋計算（numpy）。
"""
from __future__ import annotations

import math
from typing import Mapping, Sequence

from src.policies._market_residual import anchored_strengths
from src.policies._market_residual import market_probs


def residual_predict(x_row: Mapping[str, float], theta: Mapping[str, float]) -> float:
    """r_i = Σ_f θ_f · x_{i,f}（欠損特徴は 0 寄与）。"""
    return float(sum(float(theta.get(f, 0.0)) * float(x_row.get(f, 0.0) or 0.0) for f in theta))


def residual_win_probs(
    odds_map: Mapping, feats_by_horse: Mapping[int, Mapping[str, float]],
    theta: Mapping[str, float],
) -> dict[int, float]:
    """P_i = softmax(log q_i + θ·x_i)。theta≡0 なら市場 q に厳密一致（帰無）。"""
    q = market_probs(odds_map)
    r = {h: residual_predict(feats_by_horse.get(h, {}), theta) for h in q}
    s = anchored_strengths(q, r)
    if not s:
        return {}
    m = max(s.values())
    exp = {k: math.exp(v - m) for k, v in s.items()}
    tot = sum(exp.values())
    return {k: v / tot for k, v in exp.items()} if tot > 0 else {k: 0.0 for k in s}


def fit_residual_head(
    races: Sequence[Mapping],
    feature_names: Sequence[str],
    *,
    l2: float = 1.0,
    maxiter: int = 2000,
) -> dict[str, float]:
    """線形残差ヘッド θ を market-anchored listwise NLL＋L2 で fit（**事前登録の特徴集合で**）。

    races 各要素: {"odds": {馬番:単勝}, "feats": {馬番: {特徴名:値}}, "winner": 馬番}。
    返す {特徴名: θ}。scipy 不在/失敗/空データは θ≡0（＝市場）。数値は fit_beta_fast と同流儀で
    (N, H, F) にパディングしてベクトル化。特徴は呼び出し側でレース内 z-score 済みを渡すこと。
    """
    import numpy as np

    feats = list(feature_names)
    nf = len(feats)
    valid = [r for r in races if r.get("odds") and r.get("winner") is not None and r.get("feats")]
    if not valid or nf == 0:
        return {f: 0.0 for f in feats}
    h_max = max(len(r["odds"]) for r in valid)
    n = len(valid)
    pad = -1e9
    log_q = np.full((n, h_max), pad)
    X = np.zeros((n, h_max, nf))
    win_ix = np.zeros(n, dtype=np.int64)
    ok = np.ones(n, dtype=bool)
    for i, r in enumerate(valid):
        q = market_probs(r["odds"])
        horses = [h for h in q if q[h] > 0]
        if r["winner"] not in horses:
            ok[i] = False
            continue
        fb = r["feats"]
        for j, h in enumerate(horses):
            log_q[i, j] = math.log(q[h])
            row = fb.get(h, {})
            for k, f in enumerate(feats):
                v = row.get(f, 0.0)
                X[i, j, k] = float(v) if v == v and v is not None else 0.0
        win_ix[i] = horses.index(r["winner"])
    log_q, X, win_ix = log_q[ok], X[ok], win_ix[ok]
    n = len(log_q)
    if n == 0:
        return {f: 0.0 for f in feats}
    valid_mask = (log_q > pad / 2)

    def objective(theta) -> float:
        th = np.asarray(theta, float)
        s = log_q + X @ th                       # (N, H)
        s = np.where(valid_mask, s, pad)
        s = s - s.max(axis=1, keepdims=True)
        e = np.exp(s) * valid_mask
        p = e / e.sum(axis=1, keepdims=True)
        pw = p[np.arange(n), win_ix]
        return float(-np.log(np.maximum(pw, 1e-300)).mean() + l2 * float((th ** 2).sum()))

    try:
        from scipy.optimize import minimize
        res = minimize(objective, np.zeros(nf), method="L-BFGS-B",
                       options={"maxiter": maxiter})
        return {f: float(v) for f, v in zip(feats, res.x, strict=False)}
    except Exception:  # noqa: BLE001 — scipy 不在/失敗は帰無（市場）へ
        return {f: 0.0 for f in feats}
