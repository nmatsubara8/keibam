"""着順の完全分布（Plackett-Luce / Harville）から着位別確率を導出する。

強度 s_i（1頭1スカラー）→ 勝率 p_i=softmax(s) → 着順分布。ここでは「勝率ベクトル」を入力に、
各馬の **着位別確率 P(i が k着)** と **P(i が top-k)** を返す（券種別 EV の素になる）。

P(i が top-k) は Harville の逐次選択（_place_prob._prob_in_top）で厳密算出。
P(i がちょうど k着) = P(top-k) − P(top-(k-1))。

（勝率→連系/順列系の同時確率は既存 _harville を使う。本モジュールは単勝/複勝の素＝着位別marginal）。
"""
from __future__ import annotations

import math
from typing import Mapping

from src.preprocessing._place_prob import _prob_in_top, normalize


def softmax_strengths(strengths: Mapping[int, float]) -> dict[int, float]:
    """強度 s_i（実数）→ 勝率 p_i=softmax（レース内 Σ=1）。数値安定のため最大値を引く。"""
    if not strengths:
        return {}
    m = max(strengths.values())
    exp = {k: math.exp(float(v) - m) for k, v in strengths.items()}
    z = sum(exp.values())
    return {k: v / z for k, v in exp.items()} if z > 0 else {k: 0.0 for k in strengths}


def position_probs(win_probs: Mapping[int, float], k_positions: int = 3) -> dict[int, dict]:
    """各馬の着位別確率を返す。

    Returns
    -------
    {umaban: {"exact": [P(1着), P(2着), …, P(k着)], "top": [P(top1), P(top2), …, P(topk)]}}
    exact[m-1] = ちょうど m 着 / top[m-1] = m 着以内。P(top1)=P(1着)=勝率。
    """
    p = normalize(dict(win_probs))
    out: dict[int, dict] = {}
    for h in p:
        others = [u for u in p if u != h]
        tops = [_prob_in_top(p, h, others, m) for m in range(1, k_positions + 1)]
        exact = [tops[0]] + [tops[m] - tops[m - 1] for m in range(1, k_positions)]
        out[h] = {"exact": exact, "top": tops}
    return out


def win_prob(win_probs: Mapping[int, float], horse: int) -> float:
    """単勝＝P(1着)＝正規化勝率。"""
    return normalize(dict(win_probs)).get(horse, 0.0)


def place_prob(win_probs: Mapping[int, float], horse: int, n_places: int = 3) -> float:
    """複勝＝P(top-n_places)。"""
    p = normalize(dict(win_probs))
    others = [u for u in p if u != horse]
    return _prob_in_top(p, horse, others, n_places)


def position_matrix(win_probs: Mapping[int, float], k_positions: int = 3) -> list[list[float]]:
    """着順確率行列 Π[i][k] = P(馬i が (k+1)着)（行=馬順・列=着位）。整合性検査用。

    各行の合計 ≤ 1（k_positions までの marginal）、各列の合計 ≈ 1（その着位は誰か1頭）。
    """
    pp = position_probs(win_probs, k_positions)
    order = list(pp)
    return [pp[h]["exact"] for h in order]


def market_anchored_position_probs(
    odds_map: Mapping, residual: Mapping[int, float] | None = None, k_positions: int = 3
) -> dict[int, dict]:
    """市場アンカー残差モデルの真勝率 P=softmax(log q + r) を素に着位別確率を返す。

    :func:`_market_residual.true_probs` で市場implied q に残差 r を足した勝率を作り、
    :func:`position_probs`（Harville 逐次）に流す。residual=None は帰無（P≡q）。
    連系（馬単/三連単…）の joint が必要なら得た P を ``_harville`` に渡す（べき乗補正はそちら）。
    """
    from src.policies._market_residual import true_probs

    return position_probs(true_probs(odds_map, residual), k_positions)
