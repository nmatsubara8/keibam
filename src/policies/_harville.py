"""着順確率を単勝（勝率）から導出する Harville モデル。

このモジュールは副作用を持たない純粋関数の集合であり、外部レイヤ（training / simulation /
policies）から特定の実装に依存せず利用できる。入力は「馬番 -> 勝率」の対応のみで、
スケーリングやデータ取得には関与しない（単一責務）。

参考: Harville (1973). 勝率 p_i から
    P(i が1着)            = p_i
    P(i->j の順)          = p_i * p_j / (1 - p_i)
    P(i->j->k の順)       = p_i * p_j / (1 - p_i) * p_k / (1 - p_i - p_j)
を逐次的に導出する。
"""

from __future__ import annotations

from itertools import permutations
from typing import Mapping
from typing import Sequence

from src.constants._bet_types import BetType
from src.constants._bet_thresholds import RiskLimits

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


def prob_exacta(win_probs: Probabilities, first: int, second: int) -> float:
    """馬単（1着 first -> 2着 second の順序付き）の確率。"""
    p = normalize(win_probs)
    p_first = p[first]
    if p_first >= 1.0:
        return 0.0
    return p_first * p[second] / (1.0 - p_first)


def prob_trifecta(win_probs: Probabilities, first: int, second: int, third: int) -> float:
    """三連単（1着->2着->3着の順序付き）の確率。"""
    p = normalize(win_probs)
    p_first = p[first]
    if p_first >= 1.0:
        return 0.0
    remain_after_first = 1.0 - p_first
    p_second = p[second]
    remain_after_second = remain_after_first - p_second
    if remain_after_second <= 0:
        return 0.0
    return p_first * (p_second / remain_after_first) * (p[third] / remain_after_second)


def prob_quinella(win_probs: Probabilities, horse_a: int, horse_b: int) -> float:
    """馬連（順不同で1・2着）の確率。両順序の馬単確率の和。"""
    return prob_exacta(win_probs, horse_a, horse_b) + prob_exacta(win_probs, horse_b, horse_a)


def prob_trio(win_probs: Probabilities, horse_a: int, horse_b: int, horse_c: int) -> float:
    """三連複（順不同で1・2・3着）の確率。全6順列の三連単確率の和。"""
    return sum(
        prob_trifecta(win_probs, first, second, third)
        for first, second, third in permutations((horse_a, horse_b, horse_c))
    )


def prob_wide(win_probs: Probabilities, horse_a: int, horse_b: int) -> float:
    """ワイド（指定 2 頭が共に 3 着以内）の確率。

    馬連（共に 2 着以内）より緩い条件なので確率は大きい。a, b が共に top3 に入る事象は
    「top3 の集合が {a, b, c}（c は他馬）」で c について互いに排反に尽くせるため、
    ``Σ_c prob_trio(a, b, c)`` で厳密に求められる（三連複＝top3 集合の確率の和）。
    """
    p = normalize(win_probs)
    others = [u for u in p if u not in (horse_a, horse_b)]
    return sum(prob_trio(win_probs, horse_a, horse_b, c) for c in others)


def prob_place(win_probs: Probabilities, horse: int, n_places: int = 3) -> float:
    """複勝（指定馬が n_places 着以内に入る）の確率。

    P(horse が k 着) を k=1..n_places について合計する。
    """
    p = normalize(win_probs)
    others = [u for u in p if u != horse]
    return _prob_in_top(p, horse, others, n_places)


def combo_probability(bet_type: str, win_probs: Probabilities, combo: Sequence[int]) -> float:
    """馬券種に応じた組合せ的中確率を返すディスパッチ。

    EV 計算（モデル勝率）と推定オッズ（市場勝率）の双方から再利用される単一の入口。
    """
    combo = list(combo)
    if bet_type == BetType.TANSHO:
        return normalize(win_probs)[combo[0]]
    if bet_type == BetType.FUKUSHO:
        return prob_place(win_probs, combo[0], RiskLimits.FUKUSHO_PLACES)
    if bet_type == BetType.UMAREN:
        return prob_quinella(win_probs, combo[0], combo[1])
    if bet_type == BetType.UMATAN:
        return prob_exacta(win_probs, combo[0], combo[1])
    if bet_type == BetType.WIDE:
        # ワイドは2頭が共に3着以内（馬連より緩い）。Σ_c prob_trio で厳密に求める。
        return prob_wide(win_probs, combo[0], combo[1])
    if bet_type == BetType.SANRENPUKU:
        return prob_trio(win_probs, combo[0], combo[1], combo[2])
    if bet_type == BetType.SANRENTAN:
        return prob_trifecta(win_probs, combo[0], combo[1], combo[2])
    raise ValueError(f"未知の馬券種: {bet_type}")


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


# ---------------------------------------------------------------------------
# Place ヘッド（top3 直接予測）から連系の joint を導く近似
# ---------------------------------------------------------------------------


def normalize_place(place_probs: Probabilities, n_places: int = 3) -> dict[int, float]:
    """複勝（top3）marginal を「3枠の固定サイズ制約」に合わせ総和=n_places に正規化する。

    Place ヘッドの較正出力は馬ごとに独立に較正されるため Σ_h P(top3) は厳密に 3 にならない。
    固定サイズ抽出（ちょうど 3 頭が top3）の枠制約を満たすようスケールする。
    """
    total = float(sum(place_probs.values()))
    if total <= 0:
        raise ValueError("複勝確率の総和が0以下です。正規化できません。")
    scale = n_places / total
    # 個々の確率が 1 を超えないようにクリップ（極端な較正値の保険）
    return {h: min(1.0, float(p) * scale) for h, p in place_probs.items()}


def prob_wide_from_place(
    place_probs: Probabilities, horse_a: int, horse_b: int, n_places: int = 3
) -> float:
    """Place ヘッドの top3 marginal から **ワイド**（a,b が共に3着内）の joint を近似する。

    固定サイズ（=n_places 頭が top3）抽出の二次近似（Hájek）:
        π_ab ≈ p_a p_b ( 1 − (1−p_a)(1−p_b)/d ),   d = Σ_k p_k(1−p_k)
    独立仮定 p_a·p_b と違い、3枠の取り合いによる**負の相関**を再現する（π_ab ≤ p_a p_b）。
    Win 由来の Harville（Plackett-Luce）と異なり、ペースや展開の相関を学習した Place ヘッドの
    情報を直接使える。入力は top3 marginal のみ。

    返り値は [0, min(p_a, p_b)] にクリップ（確率の整合性ガード）。
    """
    if horse_a not in place_probs or horse_b not in place_probs:
        return 0.0
    p = normalize_place(place_probs, n_places)
    pa, pb = p[horse_a], p[horse_b]
    if pa <= 0 or pb <= 0:
        return 0.0
    d = sum(pi * (1.0 - pi) for pi in p.values())
    if d <= 0:
        joint = pa * pb
    else:
        joint = pa * pb * (1.0 - (1.0 - pa) * (1.0 - pb) / d)
    return max(0.0, min(joint, pa, pb))
