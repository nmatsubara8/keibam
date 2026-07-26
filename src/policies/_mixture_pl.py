"""Mixture Plackett-Luce（Step3）— ペース潜在状態で IIA を緩和する混合モデル。

素の PL/Harville は IIA（他馬の顔ぶれに依存しない選択比）を仮定するが、実際の着順は
ペース展開に依存する（ハイペース→差し有利/スローペース→逃げ有利）。潜在状態 z で混合して緩和する:

    P(勝ち馬=w) = Σ_z P(z) · softmax( log q + r + β(style, z) )_w,   z ∈ {slow, normal, fast}

強度の完全分解（Step4 の状態空間まで見据えた形）:
    s_i(z) = log q_i + a_i(t) + r_θ(x_i) + β(style_i, z)
      log q_i        市場（prior）
      a_i(t)         時系列潜在能力（Step4: Kalman/Elo。本モジュールでは residual に合流）
      r_θ(x_i)       市場で説明できない残差（Step1・λ_r 正則化）
      β(style_i, z)  ペース状態×脚質の相互作用（本モジュール・λ_β 正則化）

自由度の統制（過学習防止の要）:
- β は **馬ごとではなく 脚質×状態の表**（4×3=12 パラメータのみ）。馬ごとの β_i(z) は
  18頭×3=54 自由度になり公開データでは危険。
- **β≡0 で Step1（市場アンカー残差）へ厳密に退化**する入れ子構造。λ_β‖β‖² を必ず入れ、
  「β=0 から離れるには証拠を要求する」。
- P(z) は最初は固定（ほぼ均等の Dirichlet prior 相当）で十分。後段で pace_pressure 等から
  小さいモデルで出す（レース別 pace_probs を渡せる口だけ用意）。
- 状態数は 3 で固定（Slow/Normal/Fast）。公開データでこれ以上は学習できない。

識別性の注意（合成データ実験で確認済み）: **一様固定 P(z) ＋ 勝者のみ NLL では β はほぼ識別
不能**。脚質×ペースの効果は状態間で反対称（逃げ: slow有利/fast不利）なので、一様周辺化で
打ち消し合い勝率がベースラインとほぼ一致してしまう。β を学習するには (a) pace_pressure 等
から情報のあるレース別 P(z) を与える（真の状態に 70% 程度の精度で fit β の符号・大小を回収
できることを確認済み）、または (b) 完全着順の listwise 尤度に拡張する、のいずれかが必要。

ゲージ自由度: 状態 z 内で全脚質に定数 c_z を足しても softmax は不変（識別不能）。
λ_β‖β‖² がゲージを解消する（ゼロ平均解を選ぶ）。fit_beta は加えて状態内中心化も行う。

成功判定（事前定義・_model_compare 参照）: β=0 をベースラインに ΔNLL<0 かつ
Bootstrap CI / LRT で有意、かつ較正（ECE）が悪化しないこと。ROI 単独では判断しない。

純粋関数のみ（pandas/IO 非依存）。fit のみ scipy に fail-soft 依存。
"""
from __future__ import annotations

import math
from typing import Mapping, Sequence

from src.constants._pace_states import PACE_STATES  # noqa: F401  （正準は constants・再 export）
from src.constants._pace_states import STYLES  # noqa: F401
from src.policies._harville import PlaceExponents
from src.policies._harville import place_probs_corrected
from src.policies._market_residual import anchored_strengths
from src.policies._market_residual import market_probs
from src.preprocessing._place_prob import _prob_in_top

# β表の型: {(style, z): float}。beta_zero() が帰無（Step1 退化）。
BetaTable = Mapping[tuple[str, str], float]


def style_from_pace_ratio(ratio: float | None) -> str:
    """第1コーナー位置比（0=先頭〜1=最後方・featured の pace_median）→ 4脚質。

    閾値: <0.2 逃げ / <0.5 先行 / <0.8 差し / それ以外 追込。欠損は senko（中庸）に倒す。
    """
    if ratio is None or (isinstance(ratio, float) and math.isnan(ratio)):
        return "senko"
    r = float(ratio)
    if r < 0.2:
        return "nige"
    if r < 0.5:
        return "senko"
    if r < 0.8:
        return "sashi"
    return "oikomi"


def uniform_pace_probs() -> dict[str, float]:
    """P(z) の既定＝ほぼ均等の固定 prior（Dirichlet(α→∞) 相当）。"""
    return {z: 1.0 / len(PACE_STATES) for z in PACE_STATES}


def beta_zero() -> dict[tuple[str, str], float]:
    """β≡0 の帰無表。mixture が Step1 の true_probs へ厳密に退化する。"""
    return {(s, z): 0.0 for s in STYLES for z in PACE_STATES}


def _state_probs(
    q: Mapping[int, float],
    residual: Mapping[int, float],
    styles: Mapping[int, str],
    beta: BetaTable,
    z: str,
) -> dict[int, float]:
    """状態 z の条件付き勝率 softmax(log q + r + β(style, z))。"""
    shifted = {
        h: float(residual.get(h, 0.0)) + float(beta.get((styles.get(h, "senko"), z), 0.0))
        for h in q
    }
    s = anchored_strengths(q, shifted)
    if not s:
        return {}
    m = max(s.values())
    exp = {k: math.exp(v - m) for k, v in s.items()}
    tot = sum(exp.values())
    return {k: v / tot for k, v in exp.items()} if tot > 0 else {k: 0.0 for k in s}


def mixture_win_probs(
    odds_map: Mapping,
    residual: Mapping[int, float] | None = None,
    styles: Mapping[int, str] | None = None,
    beta: BetaTable | None = None,
    pace_probs: Mapping[str, float] | None = None,
) -> dict[int, float]:
    """混合勝率 P_i = Σ_z P(z)·softmax(log q + r + β(style_i, z))_i。

    beta=None/β≡0 → 全状態が同一分布になり **Step1 の true_probs と厳密一致**（帰無）。
    さらに residual=None なら P≡q（市場そのもの）。pace_probs=None は固定均等 prior。
    """
    q = market_probs(odds_map)
    r = residual or {}
    st = styles or {}
    b = beta or {}
    pz = pace_probs or uniform_pace_probs()
    tot = float(sum(pz.values()))
    if tot <= 0:
        pz, tot = uniform_pace_probs(), 1.0
    out: dict[int, float] = {h: 0.0 for h in q}
    for z, w in pz.items():
        pw = _state_probs(q, r, st, b, z)
        for h, p in pw.items():
            out[h] += (float(w) / tot) * p
    return out


def mixture_place_probs(
    odds_map: Mapping,
    residual: Mapping[int, float] | None = None,
    styles: Mapping[int, str] | None = None,
    beta: BetaTable | None = None,
    pace_probs: Mapping[str, float] | None = None,
    exp: PlaceExponents | None = None,
    n_places: int = 3,
) -> dict[int, float]:
    """混合複勝確率 P(h∈top-n) = Σ_z P(z)·P_PL(h∈top-n | z)。

    状態別の複勝は exp 指定で Benter γ補正（Step2）、無指定で素の Harville。
    混合を状態ごとの複勝 marginal の凸結合で取るため枠制約 Σ=n_places は保存される。
    """
    q = market_probs(odds_map)
    r = residual or {}
    st = styles or {}
    b = beta or {}
    pz = pace_probs or uniform_pace_probs()
    tot = float(sum(pz.values()))
    if tot <= 0:
        pz, tot = uniform_pace_probs(), 1.0
    out: dict[int, float] = {h: 0.0 for h in q}
    for z, w in pz.items():
        pw = _state_probs(q, r, st, b, z)
        if not pw:
            continue
        if exp is not None:
            pl = place_probs_corrected(pw, exp, n_places)
        else:
            pl = {
                h: _prob_in_top(pw, h, [u for u in pw if u != h], n_places) for h in pw
            }
        for h, p in pl.items():
            out[h] += (float(w) / tot) * p
    return out


def mixture_nll(
    odds_map: Mapping,
    residual: Mapping[int, float],
    styles: Mapping[int, str],
    beta: BetaTable,
    winner: int,
    *,
    pace_probs: Mapping[str, float] | None = None,
    l2_r: float = 0.0,
    l2_beta: float = 0.0,
) -> float:
    """混合 listwise NLL（1着）＋正則化 L = −log P_mix[winner] + l2_r·Σr² + l2_beta·Σβ²。

    r≡0 かつ β≡0 のとき L=−log q_winner（市場の尤度・正則化も0）。β の l2 は
    「β=0（Step1）から離れるには証拠を要求する」制約で、ゲージ自由度も解消する。
    """
    p = mixture_win_probs(odds_map, residual, styles, beta, pace_probs)
    pw = p.get(winner, 0.0)
    nll = -math.log(pw) if pw > 0 else 30.0
    if l2_r > 0:
        nll += l2_r * sum(float(v) ** 2 for v in residual.values())
    if l2_beta > 0:
        nll += l2_beta * sum(float(v) ** 2 for v in beta.values())
    return nll


def fit_beta(
    races: Sequence[Mapping],
    *,
    l2_beta: float = 0.1,
    pace_probs: Mapping[str, float] | None = None,
    styles_order: tuple[str, ...] = STYLES,
    states_order: tuple[str, ...] = PACE_STATES,
) -> dict[tuple[str, str], float]:
    """β表（脚質×状態・12パラメータ）を混合 NLL 最小化で fit する。

    races の各要素は {"odds": {...}, "styles": {馬番: 脚質}, "winner": 馬番,
    "residual": {...}(任意), "pace_probs": {...}(任意)}。**OOS 検証が前提**（fit 期間と
    評価期間を分ける・リーク回避）。目的関数は 平均混合NLL + l2_beta·‖β‖²。
    解は状態内でゼロ平均に中心化して返す（ゲージ固定）。scipy 不在/失敗時は β≡0（fail-soft）。
    """
    valid = [r for r in races if r.get("odds") and r.get("winner") is not None]
    if not valid:
        return beta_zero()
    n_s, n_z = len(styles_order), len(states_order)

    def unpack(theta: Sequence[float]) -> dict[tuple[str, str], float]:
        return {
            (styles_order[i], states_order[j]): float(theta[i * n_z + j])
            for i in range(n_s)
            for j in range(n_z)
        }

    def objective(theta: Sequence[float]) -> float:
        b = unpack(theta)
        total = 0.0
        for r in valid:
            total += mixture_nll(
                r["odds"],
                r.get("residual", {}),
                r.get("styles", {}),
                b,
                r["winner"],
                pace_probs=r.get("pace_probs", pace_probs),
            )
        return total / len(valid) + l2_beta * sum(v * v for v in theta)

    try:
        from scipy.optimize import minimize

        res = minimize(objective, [0.0] * (n_s * n_z), method="Nelder-Mead",
                       options={"maxiter": 4000, "xatol": 1e-4, "fatol": 1e-6})
        b = unpack(res.x)
        # ゲージ固定: 状態内でゼロ平均へ中心化（softmax 不変・可読性と比較可能性のため）
        for z in states_order:
            mean_z = sum(b[(s, z)] for s in styles_order) / n_s
            for s in styles_order:
                b[(s, z)] -= mean_z
        return b
    except Exception:  # noqa: BLE001 — scipy 不在/最適化失敗は帰無へフォールバック
        return beta_zero()


def fit_beta_fast(
    races: Sequence[Mapping],
    *,
    l2_beta: float = 0.1,
    styles_order: tuple[str, ...] = STYLES,
    states_order: tuple[str, ...] = PACE_STATES,
    maxiter: int = 3000,
) -> dict[tuple[str, str], float]:
    """:func:`fit_beta` の numpy ベクトル化版（実データ規模・数万レース用）。入出力は同一。

    全レースを (N, 頭数上限, 状態数) のテンソルにパディングして目的関数を1回の行列演算で
    評価する（純 Python ループ版は 2万レース×千回評価で数十分 → 数十秒に短縮）。
    レース別 pace_probs は races[i]["pace_probs"] から取る（無ければ一様）。
    数値等価性は fit_beta の目的関数とテストで照合済み。scipy 不在/失敗は β≡0（fail-soft）。
    """
    import numpy as np

    valid = [r for r in races if r.get("odds") and r.get("winner") is not None]
    if not valid:
        return beta_zero()
    n_s, n_z = len(styles_order), len(states_order)
    sidx = {s: i for i, s in enumerate(styles_order)}
    senko = sidx.get("senko", 0)
    h_max = max(len(r["odds"]) for r in valid)
    n = len(valid)
    pad = -1e9  # パディング: softmax で exp→0 になる大負値（-inf は演算警告を出すため避ける）
    log_q = np.full((n, h_max), pad)
    style_ix = np.zeros((n, h_max), dtype=np.int64)
    win_ix = np.zeros(n, dtype=np.int64)
    pz_mat = np.full((n, n_z), 1.0 / n_z)
    ok = np.ones(n, dtype=bool)
    for i, r in enumerate(valid):
        q = market_probs(r["odds"])
        horses = [h for h in q if q[h] > 0]
        if r["winner"] not in horses:
            ok[i] = False
            continue
        resid = r.get("residual") or {}
        styles = r.get("styles") or {}
        for j, h in enumerate(horses):
            log_q[i, j] = math.log(q[h]) + float(resid.get(h, 0.0))
            style_ix[i, j] = sidx.get(styles.get(h, "senko"), senko)
        win_ix[i] = horses.index(r["winner"])
        pz = r.get("pace_probs")
        if pz:
            tot = float(sum(pz.values()))
            if tot > 0:
                pz_mat[i] = [float(pz.get(z, 0.0)) / tot for z in states_order]
    log_q, style_ix, win_ix, pz_mat = log_q[ok], style_ix[ok], win_ix[ok], pz_mat[ok]
    n = len(log_q)
    if n == 0:
        return beta_zero()

    def objective(theta) -> float:
        b = np.asarray(theta, dtype=float).reshape(n_s, n_z)
        st = log_q[:, :, None] + b[style_ix]          # (N, H, Z)
        st = st - st.max(axis=1, keepdims=True)
        e = np.exp(st)
        p = e / e.sum(axis=1, keepdims=True)
        pw = p[np.arange(n), win_ix, :]               # (N, Z) 勝ち馬の状態別勝率
        pmix = (pw * pz_mat).sum(axis=1)
        return float(-np.log(np.maximum(pmix, 1e-300)).mean() + l2_beta * float((b ** 2).sum()))

    try:
        from scipy.optimize import minimize

        res = minimize(objective, np.zeros(n_s * n_z), method="Nelder-Mead",
                       options={"maxiter": maxiter, "xatol": 1e-4, "fatol": 1e-7})
        b = np.asarray(res.x, dtype=float).reshape(n_s, n_z)
        b = b - b.mean(axis=0, keepdims=True)         # 状態内中心化（ゲージ固定）
        return {
            (styles_order[i], states_order[j]): float(b[i, j])
            for i in range(n_s)
            for j in range(n_z)
        }
    except Exception:  # noqa: BLE001 — scipy 不在/最適化失敗は帰無へフォールバック
        return beta_zero()
