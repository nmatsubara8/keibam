"""勝率の正規化と Harville 複勝（top-N 着内）確率の純粋計算。

`policies._harville`（馬券戦略）と `preprocessing._market_signals`（特徴量化）の双方が
使う低レベルの確率プリミティブ。レイヤ規約（policies > preprocessing）に従い、共有される
純粋関数はここ（preprocessing）に置き、上位の policies から import する（下方向＝合法）。

純粋関数のみ（pandas/IO 非依存・constants にも依存しない）。
"""

from __future__ import annotations

from typing import Mapping

Probabilities = Mapping[int, float]


def normalize(win_probs: Probabilities) -> dict[int, float]:
    """勝率をレース内で正規化して総和を 1 にする。

    モデル出力（較正済み勝率）は厳密には総和 1 にならないため、Harville の前提
    （ある馬が1着になる確率の総和 = 1）を満たすよう正規化する。
    """
    total = float(sum(win_probs.values()))
    if total <= 0:
        raise ValueError("勝率の総和が0以下です。正規化できません。")
    return {umaban: float(prob) / total for umaban, prob in win_probs.items()}


def prob_place(win_probs: Probabilities, horse: int, n_places: int = 3) -> float:
    """複勝（指定馬が n_places 着以内に入る）の確率。

    P(horse が k 着) を k=1..n_places について合計する。
    """
    p = normalize(win_probs)
    others = [u for u in p if u != horse]
    return _prob_in_top(p, horse, others, n_places)


def _prob_in_top(p: dict[int, float], horse: int, others: list[int], depth: int) -> float:
    """horse が残り depth 個の枠（1着含む）のいずれかに入る確率を再帰的に計算する。"""
    if depth <= 0 or p.get(horse, 0.0) <= 0:
        return 0.0
    remaining_total = p[horse] + sum(p[o] for o in others)
    if remaining_total <= 0:
        return 0.0
    # この枠で horse が選ばれる確率
    prob_here = p[horse] / remaining_total
    if depth == 1:
        return prob_here
    # この枠で他馬 o が選ばれ、その後 horse が残る枠に入る確率
    prob_later = 0.0
    for o in others:
        prob_o_here = p[o] / remaining_total
        rest = [x for x in others if x != o]
        prob_later += prob_o_here * _prob_in_top(p, horse, rest, depth - 1)
    return prob_here + prob_later
