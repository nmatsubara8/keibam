"""ベイズ Kelly（Step5）— 能力事後分布を E[log W] まで伝播する資金配分。

設計の核: **λ（fractional Kelly 係数）を学習・調整しない**。縮小率は推定されるもの。
    Ability posterior N(μ,σ²) → Monte Carlo → PL → 事後予測確率 p̄ → E[log W] 最大化 f*

事後分布が効く場所（理論メモ・実装検査で保証）:
    単一期の期待対数資産 E[log W] は結果確率に**線形**なので、f* は事後予測確率
    p̄_i = E_posterior[ softmax(log q + a + r + β)_i ] だけで決まる（それ以上の積分は不要・厳密）。
    事後分布の寄与は softmax の**非線形性**を通る所で入る（E[softmax(a)] ≠ softmax(E[a])）。
    Jensen の向きは自分の強度に対する勝率（ロジスティック）の凸性で決まり、**符号は一様でない**:
      - p>1/2 の本命: 凹領域 → 分散が p̄ を**下げる**（不確実な本命ほど賭けが縮む）
      - p<1/2 の馬:   凸領域 → 分散が p̄ を**上げる**（上振れで勝つ分、勝率は本当に高い）
    Kelly の掛け金は本命側に集中するため、実務上の正味効果は「不確実性→縮小」だが、
    中穴の分散を無視しない点が係数 λ 近似との本質的な違い（「85±1 と 85±6」は p̄ 自体が変わる）。
    残る λ の役割＝モデル自体の誤特定リスクは、係数でなく placebo/OOS ゲートで遮断する方針。

Monte Carlo で十分（18頭×100サンプル程度・GPU不要）。フィルタが per-horse 独立なので
事後は馬ごとに独立な正規で引ける。scale=0（能力合流を切る）で p̄≡帰無（Step1/3 の P）に
厳密退化する（機構検査）。scale は学習レイヤで較正する合流係数。

E[log W] 最適化（単勝ブック・同一レース内は排反事象）:
    E(f) = Σ_h p̄_h·log(1 − F + f_h·o_h) + (1 − Σ_h p̄_h)·log(1 − F),   F = Σ_h f_h
    log(アフィン) の和なので f に対し凹 → 大域解。∂E/∂f_h|_{f=0} = p̄_h·o_h − 1 なので
    **EV≤1 の脚は f*=0**（帰無＝市場そのもの＋控除 なら全脚 f*=0＝賭けない）。
    複勝/連系の joint 最適化は結果空間の拡張が要るため将来分（単勝ブックが本体）。

レイヤ: portfolio（ドメイン）。numpy のみ・I/O なし。scipy は fail-soft。
"""
from __future__ import annotations

import math
from typing import Mapping

import numpy as np

from src.policies._ability_filter import AbilityFilter
from src.policies._ability_filter import AbilityState
from src.policies._mixture_pl import mixture_win_probs
from src.portfolio._kelly import kelly_fraction


def posterior_predictive_probs(
    odds_map: Mapping,
    ability_states: Mapping[int, AbilityState] | None = None,
    flt: AbilityFilter | None = None,
    *,
    residual: Mapping[int, float] | None = None,
    styles: Mapping[int, str] | None = None,
    beta: Mapping | None = None,
    pace_probs: Mapping[str, float] | None = None,
    scale: float = 1.0,
    n_samples: int = 100,
    seed: int = 0,
) -> tuple[dict[int, float], dict[int, float]]:
    """事後予測勝率 p̄_i = E_a[ P_i(a) ] を Monte Carlo で求める（Kelly の唯一の入力）。

    各サンプルで a_h 〜 N(strength_h, variance_h)（per-horse 独立）を引き、residual に
    scale·a_h を合流させて Mixture-PL 勝率を計算し、サンプル平均する（正規化は保存される）。
    ability_states/flt=None または scale=0 → サンプリング無しで帰無（Step1/3 の確率）を返す。

    Returns: (p̄, p_std)。p_std はサンプル標準偏差（診断・VOI 可視化用。f* には使わない —
    E[log W] は確率に線形なので p̄ が全て）。
    """
    r0 = dict(residual or {})
    if ability_states is None or flt is None or scale == 0.0 or n_samples <= 0:
        p = mixture_win_probs(odds_map, r0, styles, beta, pace_probs)
        return p, {h: 0.0 for h in p}

    rng = np.random.default_rng(seed)
    means = {h: flt.strength(st) for h, st in ability_states.items()}
    sds = {h: math.sqrt(max(0.0, flt.variance(st))) for h, st in ability_states.items()}
    acc: dict[int, float] = {}
    acc2: dict[int, float] = {}
    for _ in range(n_samples):
        offs = {h: scale * rng.normal(means[h], sds[h]) for h in ability_states}
        r = {h: r0.get(h, 0.0) + offs.get(h, 0.0) for h in set(r0) | set(offs)}
        p = mixture_win_probs(odds_map, r, styles, beta, pace_probs)
        for h, v in p.items():
            acc[h] = acc.get(h, 0.0) + v
            acc2[h] = acc2.get(h, 0.0) + v * v
    n = float(n_samples)
    p_bar = {h: v / n for h, v in acc.items()}
    p_std = {
        h: math.sqrt(max(0.0, acc2[h] / n - p_bar[h] ** 2)) for h in acc
    }
    return p_bar, p_std


def expected_log_wealth(
    probs: Mapping[int, float], odds: Mapping[int, float], fractions: Mapping[int, float]
) -> float:
    """単勝ブックの期待対数資産 E(f) = Σ p_h·log(1−F+f_h·o_h) + p_none·log(1−F)。

    F=Σf_h。どの候補も勝たない確率 p_none = 1−Σp_h（確率は排反）。1−F≤0 は −inf。
    """
    fr = {h: max(0.0, float(v)) for h, v in fractions.items()}
    total_f = sum(fr.values())
    if total_f >= 1.0:
        return -float("inf")
    p_sum = 0.0
    out = 0.0
    for h, f in fr.items():
        p = float(probs.get(h, 0.0))
        if p <= 0:
            continue
        p_sum += p
        w = 1.0 - total_f + f * float(odds[h])
        out += p * (math.log(w) if w > 0 else -1e6)
    p_none = max(0.0, 1.0 - p_sum)
    if p_none > 0:
        out += p_none * math.log(1.0 - total_f)
    return out


def optimal_win_fractions(
    probs: Mapping[int, float],
    odds: Mapping[int, float],
    *,
    max_race_fraction: float = 0.5,
) -> dict[int, float]:
    """E[log W] を最大化する単勝配分 f*（フル・ベイズ Kelly。λ 係数なし）。

    候補は事後予測 EV>1 の脚のみ（∂E/∂f_h|_0 = p̄·o−1 ≤ 0 なら f*=0 が厳密なので除外は無損失）。
    凹計画（SLSQP・制約 Σf ≤ max_race_fraction）。scipy 不在/失敗時は per-bet Kelly を
    Σf でスケールする fail-soft（近似）。帰無（p̄≡市場・控除あり）では候補が空＝全 f*=0。
    """
    cands = [
        (h, float(probs[h]), float(odds[h]))
        for h in probs
        if float(probs.get(h, 0.0)) > 0
        and float(odds.get(h, 0.0)) > 1.0
        and float(probs[h]) * float(odds[h]) > 1.0
    ]
    if not cands:
        return {}
    keys = [h for h, _, _ in cands]

    try:
        from scipy.optimize import minimize

        def neg(fs):
            return -expected_log_wealth(
                probs, odds, dict(zip(keys, fs, strict=False))
            )

        # 初期値: per-bet Kelly を cap 内へスケール
        f0 = [kelly_fraction(p, o) for _, p, o in cands]
        t0 = sum(f0)
        if t0 > max_race_fraction and t0 > 0:
            f0 = [f * max_race_fraction / t0 for f in f0]
        res = minimize(
            neg, f0, method="SLSQP",
            bounds=[(0.0, max_race_fraction)] * len(keys),
            constraints=[{"type": "ineq",
                          "fun": lambda fs: max_race_fraction - sum(fs)}],
        )
        if res.success:
            return {h: max(0.0, float(f)) for h, f in zip(keys, res.x, strict=False)}
    except Exception:  # noqa: BLE001 — scipy 不在/失敗は per-bet Kelly へフォールバック
        pass
    fb = {h: kelly_fraction(p, o) for h, p, o in cands}
    tot = sum(fb.values())
    if tot > max_race_fraction and tot > 0:
        fb = {h: f * max_race_fraction / tot for h, f in fb.items()}
    return fb
