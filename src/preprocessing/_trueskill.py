"""TrueSkill（多頭順位対応 μ/σ）レーティングの純粋計算ロジック — Phase 2。

レイヤ規約: preprocessing 層。constants と numpy/pandas（third-party）のみに依存し、
ファイル I/O やスクレイピングは行わない（_data_merger / pipeline 側が配線・永続化する）。

モデル:
- 各馬の地力を Gaussian N(μ, σ²) で表す（Herbrich-Minka-Graepel 2006）。
- 1 レースの全頭順位を「隣接順位ペアの勝敗」に分解し、切断ガウス補正（v/w 関数）で
  各馬の (μ, σ) を 1 パス更新する（因子グラフ近似）。中位馬は上位・下位の 2 比較に
  参加し、μ 変化は加算、σ² 減少は乗算で合成する。

リーク無し as-of:
- compute_trueskill_history は全レースを日付昇順に走査し、各出走の「出走前 (μ, σ)」を
  特徴量として記録してから当該レース結果で更新する（更新後値は次レース以降のみ反映）。

ロードマップ: Phase 3-5（条件別 / 状態空間 / 階層ベイズ）でも TS_FEATURE_COLS を起点に
列を増やす設計とする。
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING
from typing import Sequence

from src.constants._feature_cols import TS_BETA
from src.constants._feature_cols import TS_CONSERVATIVE_K
from src.constants._feature_cols import TS_DRAW_MARGIN
from src.constants._feature_cols import TS_FEATURE_COLS
from src.constants._feature_cols import TS_MU
from src.constants._feature_cols import TS_SIGMA
from src.constants._feature_cols import TS_TAU
from src.constants._results_cols import ResultsCols

if TYPE_CHECKING:
    import pandas as pd

_SQRT_2PI = math.sqrt(2.0 * math.pi)
_TINY = 1e-9


def _pdf(x: float) -> float:
    """標準正規分布の確率密度関数 φ(x)。"""
    return math.exp(-0.5 * x * x) / _SQRT_2PI


def _cdf(x: float) -> float:
    """標準正規分布の累積分布関数 Φ(x)（math.erf ベース、scipy 非依存）。"""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


# ──────────────────────────────────────────
# 切断ガウス補正関数（v / w）
# ──────────────────────────────────────────


def v_win(t: float, eps: float) -> float:
    """勝敗時の平均補正 v = φ(t-ε)/Φ(t-ε)。Φ が極小（大番狂わせ）の場合は極限 -(t-ε)。"""
    x = t - eps
    denom = _cdf(x)
    if denom < _TINY:
        # 数値的に Φ→0 の極限。v→-x（線形）に退化させて発散を防ぐ。
        return -x
    return _pdf(x) / denom


def w_win(t: float, eps: float) -> float:
    """勝敗時の分散補正 w = v·(v + (t-ε))。区間 [0, 1] に収まる。"""
    x = t - eps
    v = v_win(t, eps)
    val = v * (v + x)
    if val < 0.0:
        return 0.0
    if val > 1.0:
        return 1.0
    return val


def v_draw(t: float, eps: float) -> float:
    """引分時の平均補正。dead heat（同着）の更新に使う。"""
    abs_t = abs(t)
    denom = _cdf(eps - abs_t) - _cdf(-eps - abs_t)
    if denom < _TINY:
        # 極限退化（発散防止）
        return (-eps - abs_t) if t < 0 else (eps - abs_t)
    num = _pdf(-eps - abs_t) - _pdf(eps - abs_t)
    res = num / denom
    return -res if t < 0 else res


def w_draw(t: float, eps: float) -> float:
    """引分時の分散補正。区間 [0, 1] に収まる。"""
    abs_t = abs(t)
    denom = _cdf(eps - abs_t) - _cdf(-eps - abs_t)
    if denom < _TINY:
        return 1.0
    v = v_draw(t, eps)
    res = v * v + (
        (eps - abs_t) * _pdf(eps - abs_t) - (-eps - abs_t) * _pdf(-eps - abs_t)
    ) / denom
    if res < 0.0:
        return 0.0
    if res > 1.0:
        return 1.0
    return res


# ──────────────────────────────────────────
# 多頭順位の 1 パス更新
# ──────────────────────────────────────────


def update_ranking(
    mus: Sequence[float],
    sigmas: Sequence[float],
    finish_order: Sequence[float],
    *,
    beta: float = TS_BETA,
    tau: float = TS_TAU,
    draw_margin: float = TS_DRAW_MARGIN,
) -> tuple[list[float], list[float]]:
    """1 レースの全頭順位から各馬の (μ, σ) を更新して返す（純粋関数・順序非依存）。

    全頭順位を隣接順位ペアの勝敗（同着は引分）に分解し、出走前の (μ, σ) を基準に
    各ペアの補正量を算出する。μ 変化は加算、σ² 減少は乗算で合成する（因子グラフ近似）。

    Parameters
    ----------
    mus, sigmas : 各馬の出走前 μ / σ（entrant 順）。
    finish_order : 各馬の着順（小さいほど上位、同値は同着）。
    beta : パフォーマンスノイズ（スキルクラス幅）。
    tau : 動的変動（出走ごとに σ² へ加算し σ の収束しすぎを防ぐ）。
    draw_margin : 引分マージン ε（既定 0）。

    Returns
    -------
    (new_mus, new_sigmas) : 更新後 μ / σ（entrant 順）。
    """
    n = len(mus)
    if n < 2:
        return [float(m) for m in mus], [float(s) for s in sigmas]

    # 動的変動: 出走時に σ² を τ² 拡張（このレースの prior）。
    tau2 = float(tau) * float(tau)
    prior_var = [float(s) * float(s) + tau2 for s in sigmas]
    beta2 = float(beta) * float(beta)

    mu_delta = [0.0] * n
    var_factor = [1.0] * n  # σ² に乗じる縮小係数の積

    # 着順昇順（上位→下位）に並べ替えたインデックス
    order = sorted(range(n), key=lambda i: finish_order[i])

    for rank in range(n - 1):
        a = order[rank]       # 上位（または同着の片方）
        b = order[rank + 1]   # 下位
        is_draw = finish_order[a] == finish_order[b]

        c2 = 2.0 * beta2 + prior_var[a] + prior_var[b]
        c = math.sqrt(c2)
        t = (float(mus[a]) - float(mus[b])) / c

        if is_draw:
            v = v_draw(t, draw_margin)
            w = w_draw(t, draw_margin)
            # 同着: a も b も「勝者方向」の符号は ±だが v_draw が符号を内包。
            mu_delta[a] += (prior_var[a] / c) * v
            mu_delta[b] -= (prior_var[b] / c) * v
        else:
            v = v_win(t, draw_margin)
            w = w_win(t, draw_margin)
            mu_delta[a] += (prior_var[a] / c) * v   # 勝者は上昇
            mu_delta[b] -= (prior_var[b] / c) * v   # 敗者は下降

        var_factor[a] *= 1.0 - (prior_var[a] / c2) * w
        var_factor[b] *= 1.0 - (prior_var[b] / c2) * w

    new_mus = [float(mus[i]) + mu_delta[i] for i in range(n)]
    new_sigmas = []
    for i in range(n):
        new_var = prior_var[i] * var_factor[i]
        new_var = max(new_var, _TINY)
        new_sigmas.append(math.sqrt(new_var))
    return new_mus, new_sigmas


def conservative(mu: float, sigma: float, k: float = TS_CONSERVATIVE_K) -> float:
    """保守的スキル推定 μ - k·σ（リーダーボード順位づけに使う TrueSkill 慣用値）。"""
    return float(mu) - float(k) * float(sigma)


def field_features(values: Sequence[float]) -> tuple[float, list[float]]:
    """出走馬の指標列 → (field_mean, [vs_field...]) を返す（学習・ライブ共通）。"""
    arr = [float(v) for v in values]
    if not arr:
        return float("nan"), []
    fm = sum(arr) / len(arr)
    return fm, [v - fm for v in arr]


def trueskill_win_probabilities(
    mus: Sequence[float], sigmas: Sequence[float], *, beta: float = TS_BETA
) -> list[float]:
    """μ/σ から各馬の勝率（近似）を算出する。Rating Lab の即時照会に使う。

    各馬のパフォーマンスを X_i ~ N(μ_i, σ_i² + β²) とみなし、μ をフィールド平均の
    パフォーマンス標準偏差でスケールした softmax で近似する（多頭の厳密勝率は閉形式
    を持たないため表示用の近似）。
    """
    n = len(mus)
    if n == 0:
        return []
    perf_var = [float(s) * float(s) + float(beta) * float(beta) for s in sigmas]
    scale = math.sqrt(2.0 * (sum(perf_var) / n)) if n else 1.0
    if scale <= 0:
        scale = 1.0
    mu_arr = [float(m) for m in mus]
    mx = max(mu_arr)
    weights = [math.exp((m - mx) / scale) for m in mu_arr]
    total = sum(weights)
    if total <= 0:
        return [1.0 / n] * n
    return [wgt / total for wgt in weights]


# ──────────────────────────────────────────
# as-of 履歴ウォーク（pandas）
# ──────────────────────────────────────────


def compute_trueskill_history(
    df: "pd.DataFrame",
    *,
    mu0: float = TS_MU,
    sigma0: float = TS_SIGMA,
    beta: float = TS_BETA,
    tau: float = TS_TAU,
    draw_margin: float = TS_DRAW_MARGIN,
) -> "tuple[pd.DataFrame, dict]":
    """全レースを日付昇順に 1 パス走査し、リーク無し as-of TrueSkill 特徴量を返す。

    入力 df の各行（= 1 出走）に対し、出走前時点の TS 特徴量（TS_FEATURE_COLS）を
    入力と同じ行順・インデックスで返す。当該レース結果による更新は特徴量確定後に
    行うためリークしない。

    Parameters
    ----------
    df : race_id をインデックス（または 'race_id' 列）に持ち、'horse_id' /
        ResultsCols.UMABAN(馬番) / ResultsCols.RANK(着順) / 'date' 列を含む DataFrame。

    Returns
    -------
    (features, snapshot) :
        features : TS_FEATURE_COLS を列に持つ DataFrame（df と同じインデックス・行順）。
        snapshot : {horse_id(str): {"mu": float, "sigma": float, "n_races": int,
                    "last_date": str}}。最新スナップショット（ライブ予測で参照）。
    """
    import numpy as np
    import pandas as pd

    work = df.reset_index()
    if "race_id" in work.columns:
        rid = work["race_id"].astype(str)
    else:
        rid = work[df.index.name or "index"].astype(str)
    work = work.assign(
        __pos=np.arange(len(work)),
        __rid=rid.to_numpy(),
        __hid=work["horse_id"].astype(str).to_numpy(),
        __date=pd.to_datetime(work["date"], errors="coerce").to_numpy(),
        __finish=pd.to_numeric(work[ResultsCols.RANK], errors="coerce").to_numpy(),
    )
    work = work.sort_values(["__date", "__rid", "__pos"], kind="stable")

    mus: dict[str, float] = {}
    sigmas: dict[str, float] = {}
    counts: dict[str, int] = {}
    last_date: dict[str, str] = {}

    out = np.full((len(df), len(TS_FEATURE_COLS)), np.nan, dtype=float)
    # 列順: ts_mu, ts_sigma, ts_conservative, ts_n_races, ts_field_mean, ts_vs_field
    for _rid, sub in work.groupby("__rid", sort=False):
        positions = sub["__pos"].to_numpy()
        hids = sub["__hid"].tolist()
        finishes = [float(x) for x in sub["__finish"].tolist()]
        cur_mu = [mus.get(h, mu0) for h in hids]
        cur_sigma = [sigmas.get(h, sigma0) for h in hids]
        ncnt = [counts.get(h, 0) for h in hids]
        cons = [conservative(m, s) for m, s in zip(cur_mu, cur_sigma, strict=True)]
        field_mean, vs_field = field_features(cons)

        for k, pos in enumerate(positions):
            out[pos, 0] = cur_mu[k]
            out[pos, 1] = cur_sigma[k]
            out[pos, 2] = cons[k]
            out[pos, 3] = float(ncnt[k])
            out[pos, 4] = field_mean
            out[pos, 5] = vs_field[k]

        valid = [k for k, f in enumerate(finishes) if not math.isnan(f)]
        if len(valid) >= 2:
            v_mu = [cur_mu[k] for k in valid]
            v_sigma = [cur_sigma[k] for k in valid]
            v_finish = [finishes[k] for k in valid]
            new_mu, new_sigma = update_ranking(
                v_mu, v_sigma, v_finish, beta=beta, tau=tau, draw_margin=draw_margin
            )
            for vi, k in enumerate(valid):
                mus[hids[k]] = new_mu[vi]
                sigmas[hids[k]] = new_sigma[vi]
        for k, h in enumerate(hids):
            counts[h] = counts.get(h, 0) + 1
            d = sub["__date"].iloc[k]
            if pd.notna(d):
                last_date[h] = pd.Timestamp(d).strftime("%Y-%m-%d")

    features = pd.DataFrame(out, index=df.index, columns=list(TS_FEATURE_COLS))
    snapshot = {
        h: {
            "mu": round(float(mus[h]), 4),
            "sigma": round(float(sigmas.get(h, sigma0)), 4),
            "n_races": int(counts.get(h, 0)),
            "last_date": last_date.get(h),
        }
        for h in mus
    }
    return features, snapshot
