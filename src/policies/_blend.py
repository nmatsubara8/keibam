"""ベンター(1994) の2段目: ファンダ勝率と公衆 implied 勝率の対数線形プール合成。

合成確率（レース内 softmax）:
    π_i = exp(α·log P_fund_i + β·log P_public_i) / Σ_j exp(...)
α,β は勝ち馬ラベルの最尤推定（条件付きロジット）。``f=log P_fund`` は **out-of-sample** の
ファンダ予測を使うこと（in-sample だと過学習を過大評価する）。

モデルの価値は単独 R² でなく **ΔR² = R²(合成) − R²(公衆)**（公衆に上乗せした独立情報量）で測る。
「市場の写し（tipster）」は ΔR²≈0、独立情報を持つモデルは ΔR²>0（ベンターの実証）。

擬似 R²（Bolton & Chapman 1986 / ベンター式3）:
    R² = 1 − LL(model) / LL(uniform)    （LL は勝ち馬の対数尤度の総和。uniform=レース内一様 1/N）

純粋関数（policies 層・constants/scipy 以外に非依存）。
"""

from __future__ import annotations

import dataclasses
import math
from typing import Mapping, Sequence

Probabilities = Mapping[int, float]
_EPS = 1e-12


@dataclasses.dataclass(frozen=True)
class BlendWeights:
    """合成の重み。alpha=ファンダ、beta=公衆。既定 (1,0)=ファンダそのまま。"""

    alpha: float = 1.0
    beta: float = 0.0


def combine_logpool(
    p_fund: Probabilities, p_public: Probabilities, alpha: float, beta: float
) -> dict[int, float]:
    """対数線形プール合成（レース内で Σ=1 に正規化）。

    両方に存在する馬のみ対象。α=1,β=0 で p_fund、α=0,β=1 で p_public に一致（各々再正規化）。
    """
    horses = [h for h in p_fund if h in p_public]
    if not horses:
        return {}
    scores = {
        h: alpha * math.log(max(p_fund[h], _EPS)) + beta * math.log(max(p_public[h], _EPS))
        for h in horses
    }
    m = max(scores.values())
    exps = {h: math.exp(s - m) for h, s in scores.items()}
    z = sum(exps.values())
    return {h: e / z for h, e in exps.items()}


BlendRace = tuple[Probabilities, Probabilities, int]  # (p_fund, p_public, winner)


def fit_blend(races: Sequence[BlendRace], *, init: tuple[float, float] = (1.0, 1.0)) -> BlendWeights:
    """勝ち馬ラベルの最尤推定で (α, β) を推定する（条件付きロジット）。

    races の P_fund は **out-of-sample** 予測を渡すこと。scipy 未導入/失敗は init を返す。
    """
    valid = [(pf, pp, w) for pf, pp, w in races if pf and pp]
    if not valid:
        return BlendWeights(*init)

    def nll(theta: Sequence[float]) -> float:
        a, b = float(theta[0]), float(theta[1])
        total = 0.0
        for pf, pp, w in valid:
            comb = combine_logpool(pf, pp, a, b)
            total -= math.log(max(comb.get(w, _EPS), _EPS))
        return total

    try:
        from scipy.optimize import minimize

        res = minimize(nll, list(init), method="Nelder-Mead")
        return BlendWeights(alpha=float(res.x[0]), beta=float(res.x[1]))
    except Exception:  # noqa: BLE001 — scipy 未導入/失敗は init へフォールバック
        return BlendWeights(*init)


def total_loglik(prob_by_race: Sequence[Probabilities], winners: Sequence[int]) -> float:
    """各レースの勝ち馬予測確率の対数尤度の総和（≤0、0 に近いほど良い）。"""
    total = 0.0
    for probs, w in zip(prob_by_race, winners, strict=False):
        total += math.log(max(probs.get(w, _EPS), _EPS))
    return total


def uniform_loglik(field_sizes: Sequence[int]) -> float:
    """レース内一様分布（1/N）のベースライン対数尤度。"""
    return sum(math.log(1.0 / n) for n in field_sizes if n > 0)


def pseudo_r2(model_ll: float, uniform_ll: float) -> float:
    """擬似 R² = 1 − LL(model)/LL(uniform)（ベンター式3）。1=完全, 0=一様と同等。"""
    if uniform_ll == 0:
        return 0.0
    return 1.0 - model_ll / uniform_ll


def blend_diagnostic(races: Sequence[BlendRace], weights: BlendWeights) -> dict:
    """公衆・ファンダ・合成の擬似 R² と ΔR²(合成−公衆) を返す（ベンターの価値指標）。

    ΔR² > 0 ⇒ ファンダが公衆に独立情報を上乗せ。ΔR² ≈ 0 ⇒ 市場の写し（価値なし）。
    """
    valid = [(pf, pp, w) for pf, pp, w in races if pf and pp]
    if not valid:
        return {"r2_public": 0.0, "r2_fund": 0.0, "r2_combined": 0.0, "delta_r2": 0.0, "n": 0}

    winners = [w for _, _, w in valid]
    sizes = [len([h for h in pf if h in pp]) for pf, pp, _ in valid]
    uni = uniform_loglik(sizes)

    pub = [dict(pp) for _, pp, _ in valid]
    fund = [dict(pf) for pf, _, _ in valid]
    comb = [combine_logpool(pf, pp, weights.alpha, weights.beta) for pf, pp, _ in valid]

    r2_pub = pseudo_r2(total_loglik(pub, winners), uni)
    r2_fund = pseudo_r2(total_loglik(fund, winners), uni)
    r2_comb = pseudo_r2(total_loglik(comb, winners), uni)
    return {
        "r2_public": r2_pub,
        "r2_fund": r2_fund,
        "r2_combined": r2_comb,
        "delta_r2": r2_comb - r2_pub,
        "n": len(valid),
    }


def save_blend_weights(weights: BlendWeights, path: str) -> None:
    """合成の重み (α, β) を JSON へ保存する。"""
    import json
    import os

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"alpha": weights.alpha, "beta": weights.beta}, f, ensure_ascii=False, indent=2)


def load_blend_weights(path: str) -> BlendWeights | None:
    """保存済みの重みを読み込む。無ければ None。"""
    import json
    import os

    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        d = json.load(f)
    return BlendWeights(alpha=float(d["alpha"]), beta=float(d["beta"]))
