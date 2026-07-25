"""損益（log資産成長）を評価関数にする**市場アンカー**ケリー馬券シミュレータ。

「回収率を目的関数に」の正しい実装。ただし当てるのは真の分布ではなく **市場との差分（Edge）**:
真勝率 P_i = softmax(log q_i + r_i)（q=市場implied, r=モデル残差）→ 券種別EV → 資金配分 →
log資産成長。分類器の学習ロスではなく、ここ（ポリシー層）で損益を最大化する。

**帰無が機構的に綺麗**: 残差 r≡0 のとき P≡q。控除（takeout）Z=Σ(1/odds)>1 のため任意の馬で
EV_i = P_i·odds_i = 1/Z < 1 となり、賭けが 1 点も出ない。つまり「市場に何も足せなければ賭けない」。

入力（レース単位・辞書）:
- odds:      {馬番: 単勝オッズ}（必須。ここから市場implied q を作る）
- residual:  {馬番: 残差 r_i}（任意。無ければ帰無＝市場そのもの＝賭け0）
- predictive:{馬番: 事後予測勝率 p̄}（任意。あれば odds+residual からの内部計算を**上書き**する。
             _bayes_kelly.posterior_predictive_probs の出力＝能力事後分布を伝播した確率を渡す口）
- ranks:     {馬番: 着順}（決済用）
- place_odds:{馬番: 複勝オッズ}（任意。あれば複勝も対象）

資金配分（sizing）:
- elogw=True: **フル・ベイズ Kelly**（_bayes_kelly.optimal_win_fractions・E[log W] 最大化）。
  λ（kelly_lambda）は使わない — 縮小は事後予測 p̄ の Jensen 平坦化が担う（λを学習しない）。
  単勝ブックのみ joint 最適化（複勝脚は従来 per-leg Kelly のまま・将来拡張）。
- elogw=False（既定）: 従来のフラクショナル per-leg Kelly（kelly_lambda·f*）。
- flat=True: 固定率（サイジング寄与の分離）。

評価指標:
- log_growth / final_wealth / flat_roi / n_bets / hit
- max_drawdown / es_5（Expected Shortfall 5%）: Kelly は期待値最大化でドローダウンに敏感
  （競馬は数百連敗があり得る）ため**評価だけは必ず**出す（目的関数には入れない）。
対照（placebo・有意性検査）:
- placebo=True: residual をレース内シャッフル（モデル信号の破壊）。
- placebo_odds=True: **市場オッズをレース内シャッフルして意思決定に使う**（決済は真のオッズ）。
  市場アンカーが本当に効いているかの検査 — アンカーが本物なら成績は崩壊するはず。
  崩壊しなければ市場以外の経路のリーク/過学習を疑う。
"""
from __future__ import annotations

import math
from typing import Iterable, Mapping

import numpy as np

from src.policies._harville import PlaceExponents
from src.policies._harville import place_probs_corrected
from src.policies._market_residual import true_probs
from src.policies._position_dist import place_prob
from src.portfolio._kelly import kelly_fraction
from src.simulation._risk_metrics import expected_shortfall
from src.simulation._risk_metrics import max_drawdown


def evaluate_pnl(
    races: Iterable[Mapping],
    *,
    ev_threshold: float = 1.0,
    kelly_lambda: float = 0.25,
    max_race_fraction: float = 0.5,
    flat_fraction: float = 0.02,
    include_place: bool = False,
    place_exponents: "PlaceExponents | None" = None,
    elogw: bool = False,
    flat: bool = False,
    placebo: bool = False,
    placebo_odds: bool = False,
    seed: int = 0,
) -> dict:
    """レース列を市場アンカー確率で評価し、log資産成長・リスク指標・flat回収率を返す。

    各レースで p̄（predictive があればそれ・無ければ softmax(log q + r)）を作り賭ける。
    elogw=True は E[log W] 最大化の joint 配分（λ不使用）。place_exponents（γ,δ）を渡すと
    複勝確率に Benter べき乗補正を適用。flat=True は選定馬に flat_fraction（サイジング無効化）。
    """
    rng = np.random.default_rng(seed)
    wealth = 1.0
    log_growth = 0.0
    n_bets = hit = 0
    stake_sum = ret_sum = 0.0  # flat_roi 用（単位stake）
    wealth_path: list[float] = [1.0]
    race_returns: list[float] = []

    for r in races:
        odds = r.get("odds", {})
        if not odds:
            continue
        # placebo_odds: 意思決定に使うオッズをレース内で並び替え（決済は真のオッズのまま）
        decision_odds = dict(odds)
        if placebo_odds:
            keys = list(decision_odds)
            vals = list(decision_odds.values())
            rng.shuffle(vals)
            decision_odds = dict(zip(keys, vals, strict=False))
        residual = dict(r.get("residual", {}))
        if placebo and residual:  # 残差を馬にランダム再割当（Edge 信号破壊）
            keys = list(residual)
            vals = list(residual.values())
            rng.shuffle(vals)
            residual = dict(zip(keys, vals, strict=False))
        # 事後予測の上書き口（_bayes_kelly 由来）。placebo 系はレース内シャッフルで同様に壊す。
        predictive = r.get("predictive")
        if predictive is not None:
            p_true = dict(predictive)
            if placebo and p_true:
                keys = list(p_true)
                vals = list(p_true.values())
                rng.shuffle(vals)
                p_true = dict(zip(keys, vals, strict=False))
        else:
            p_true = true_probs(decision_odds, residual)  # r≡0 → P≡q → 全EV<1 → 賭け0（帰無）
        if not p_true:
            continue
        ranks = r.get("ranks", {})
        place_odds = r.get("place_odds", {}) if include_place else {}
        # 複勝確率: γ補正あり→Benter補正 marginal を一括計算 / なし→素の Harville
        place_map = (
            place_probs_corrected(p_true, place_exponents)
            if (place_exponents is not None and place_odds)
            else None
        )

        legs = []  # (won: bool, payoff_multiple: float, frac: float)

        # ── 単勝: elogw なら E[logW] joint 最適化（λなし）/ 従来は per-leg Kelly ──
        if elogw and not flat:
            from src.portfolio._bayes_kelly import optimal_win_fractions

            fstar = optimal_win_fractions(
                p_true, decision_odds, max_race_fraction=max_race_fraction
            )
            for h, f in fstar.items():
                if f > 0:
                    legs.append((ranks.get(h) == 1, float(odds[h]), f))
        else:
            for h, pi in p_true.items():
                o = decision_odds.get(h)
                if o and float(o) > 0:
                    ev = pi * float(o)
                    if ev > ev_threshold:
                        f = (
                            flat_fraction
                            if flat
                            else kelly_lambda * kelly_fraction(pi, float(o))
                        )
                        if f > 0:
                            legs.append((ranks.get(h) == 1, float(odds[h]), f))

        # ── 複勝: per-leg Kelly（elogw の joint 化は結果空間拡張が要るため将来分）──
        for h in p_true:
            po = place_odds.get(h)
            if po and float(po) > 0:
                pl = place_map[h] if place_map is not None else place_prob(p_true, h)
                if pl * float(po) > ev_threshold:
                    f = flat_fraction if flat else kelly_lambda * kelly_fraction(pl, float(po))
                    if f > 0:
                        rk = ranks.get(h)
                        legs.append((rk is not None and rk <= 3, float(po), f))

        if not legs:
            continue
        tot = sum(f for _, _, f in legs)
        scale = min(1.0, max_race_fraction / tot) if tot > 0 else 1.0

        ret_frac = 0.0
        for won, mult, f in legs:
            s = f * scale
            n_bets += 1
            stake_sum += 1.0                     # flat_roi: 単位stake
            payoff_unit = mult if won else 0.0
            ret_sum += payoff_unit               # flat_roi: 単位payoff
            if won:
                hit += 1
            ret_frac += s * mult - s if won else -s   # 資産比の純増減

        race_returns.append(ret_frac)
        wealth *= (1.0 + ret_frac)
        wealth_path.append(max(wealth, 1e-9))
        if wealth <= 0:                          # 破産（資産0以下）で打ち切り
            log_growth += -10.0
            wealth = 1e-9
            continue
        log_growth += math.log(1.0 + ret_frac) if (1.0 + ret_frac) > 0 else -10.0

    flat_roi = ret_sum / stake_sum if stake_sum > 0 else 0.0
    return {
        "log_growth": log_growth, "final_wealth": wealth, "flat_roi": flat_roi,
        "n_bets": n_bets, "hit": hit, "geo_growth_per_race": (log_growth / max(n_bets, 1)),
        "max_drawdown": max_drawdown(wealth_path),
        "es_5": expected_shortfall(race_returns, alpha=0.05),
    }
