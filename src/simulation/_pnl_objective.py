"""損益（log資産成長）を評価関数にする**市場アンカー**ケリー馬券シミュレータ。

「回収率を目的関数に」の正しい実装。ただし当てるのは真の分布ではなく **市場との差分（Edge）**:
真勝率 P_i = softmax(log q_i + r_i)（q=市場implied, r=モデル残差）→ 券種別EV → フラクショナル・
ケリー配分 → log資産成長。分類器の学習ロスではなく、ここ（ポリシー層）で損益を最大化する。

**帰無が機構的に綺麗**: 残差 r≡0 のとき P≡q。控除（takeout）Z=Σ(1/odds)>1 のため任意の馬で
EV_i = P_i·odds_i = 1/Z < 1 となり、賭けが 1 点も出ない。つまり「市場に何も足せなければ賭けない」。
Edge は r に信号が入って初めて立つ。過去の生回収率フィットのような後知恵の自由度は無い。

入力（レース単位・辞書）:
- odds:      {馬番: 単勝オッズ}（必須。ここから市場implied q を作る）
- residual:  {馬番: 残差 r_i}（任意。無ければ帰無＝市場そのもの＝賭け0）
- ranks:     {馬番: 着順}（決済用）
- place_odds:{馬番: 複勝オッズ}（任意。あれば複勝も対象）

評価指標:
- log_growth: Σ log(資産比)。ケリーの真の目的関数（幾何成長率）。
- final_wealth / flat_roi / n_bets / hit。
対照:
- flat=True でケリーを使わず固定率（サイジング寄与の分離）。
- placebo=True で residual をレース内シャッフル（Edge 信号を壊す＝有意性検査）。
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


def evaluate_pnl(
    races: Iterable[Mapping],
    *,
    ev_threshold: float = 1.0,
    kelly_lambda: float = 0.25,
    max_race_fraction: float = 0.5,
    flat_fraction: float = 0.02,
    include_place: bool = False,
    place_exponents: "PlaceExponents | None" = None,
    flat: bool = False,
    placebo: bool = False,
    seed: int = 0,
) -> dict:
    """レース列を市場アンカー確率で評価し、log資産成長・最終資産・flat回収率を返す。

    各レースで P_i = softmax(log q_i + r_i) を作り、EV>閾値 の脚にケリー配分する。
    ケリー: stake_frac = kelly_lambda · f*(P_i, odds)。1レース総率は max_race_fraction で頭打ち。
    place_exponents（γ,δ）を渡すと複勝確率に Benter べき乗補正を適用（素の Harville の
    人気馬複勝過大評価を是正。models/place_exponents.json は `_calibrate.fit_and_save_place_exponents`
    が OOS モデル勝率で fit したもの）。flat=True は選定馬に flat_fraction を賭ける
    （サイジング無効化＝ゾーン/選定の寄与だけ見る）。
    """
    rng = np.random.default_rng(seed)
    wealth = 1.0
    log_growth = 0.0
    n_bets = hit = 0
    stake_sum = ret_sum = 0.0  # flat_roi 用（単位stake）

    for r in races:
        odds = r.get("odds", {})
        if not odds:
            continue
        residual = dict(r.get("residual", {}))
        if placebo and residual:  # 残差を馬にランダム再割当（Edge 信号破壊）
            keys = list(residual)
            vals = list(residual.values())
            rng.shuffle(vals)
            residual = dict(zip(keys, vals, strict=False))
        p_true = true_probs(odds, residual)  # r≡0 → P≡q → 全EV<1 → 賭け0（帰無）
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

        # 券種別に EV>閾値 の脚を集める（単勝=P(1着)、複勝=P(top3)）
        legs = []  # (won: bool, payoff_multiple: float, frac: float)
        for h, pi in p_true.items():
            o = odds.get(h)
            if o and float(o) > 0:
                ev = pi * float(o)
                if ev > ev_threshold:
                    f = flat_fraction if flat else kelly_lambda * kelly_fraction(pi, float(o))
                    if f > 0:
                        legs.append((ranks.get(h) == 1, float(o), f))
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
            ret_frac += s * mult - s if won else -s   # ケリー: 資産比の純増減

        wealth *= (1.0 + ret_frac)
        if wealth <= 0:                          # 破産（資産0以下）で打ち切り
            log_growth += -10.0
            wealth = 1e-9
            continue
        log_growth += math.log(1.0 + ret_frac) if (1.0 + ret_frac) > 0 else -10.0

    flat_roi = ret_sum / stake_sum if stake_sum > 0 else 0.0
    return {
        "log_growth": log_growth, "final_wealth": wealth, "flat_roi": flat_roi,
        "n_bets": n_bets, "hit": hit, "geo_growth_per_race": (log_growth / max(n_bets, 1)),
    }
