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

import dataclasses
from itertools import permutations
from math import log
from typing import Mapping
from typing import Sequence

from src.constants._bet_types import BetType
from src.constants._bet_thresholds import RiskLimits
# 勝率正規化・複勝確率は低レイヤ（preprocessing._place_prob）の純粋実装を再利用する。
# preprocessing._market_signals も同じ実装を使い、レイヤ逆流（preprocessing→policies）を解消。
# 再 export により既存の `harville.normalize` / `harville.prob_place` 呼び出しを温存する。
from src.preprocessing._place_prob import _prob_in_top  # noqa: F401  （後方互換の再 export）
from src.preprocessing._place_prob import normalize
from src.preprocessing._place_prob import prob_place

Probabilities = Mapping[int, float]


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


def combo_probability(
    bet_type: str,
    win_probs: Probabilities,
    combo: Sequence[int],
    exponents: "PlaceExponents | None" = None,
) -> float:
    """馬券種に応じた組合せ的中確率を返すディスパッチ。

    EV 計算（モデル勝率）と推定オッズ（市場勝率）の双方から再利用される単一の入口。
    ``exponents`` を渡すと**順序づけを伴う券種（馬単/馬連/三連単/三連複）に Benter べき乗補正**を
    適用する（素の Harville の 2/3着バイアスを是正）。単勝・複勝・ワイドは補正対象外
    （複勝/ワイドは Place ヘッド直接が正路のため win-Harville 補正の対象にしない）。
    """
    combo = list(combo)
    if bet_type == BetType.TANSHO:
        return normalize(win_probs)[combo[0]]
    if bet_type == BetType.FUKUSHO:
        return prob_place(win_probs, combo[0], RiskLimits.FUKUSHO_PLACES)
    if bet_type == BetType.UMAREN:
        if exponents is not None:
            return prob_quinella_corrected(win_probs, combo[0], combo[1], exponents)
        return prob_quinella(win_probs, combo[0], combo[1])
    if bet_type == BetType.UMATAN:
        if exponents is not None:
            return prob_exacta_corrected(win_probs, combo[0], combo[1], exponents)
        return prob_exacta(win_probs, combo[0], combo[1])
    if bet_type == BetType.WIDE:
        # ワイドは2頭が共に3着以内（馬連より緩い）。Σ_c prob_trio で厳密に求める。
        return prob_wide(win_probs, combo[0], combo[1])
    if bet_type == BetType.SANRENPUKU:
        if exponents is not None:
            return prob_trio_corrected(win_probs, combo[0], combo[1], combo[2], exponents)
        return prob_trio(win_probs, combo[0], combo[1], combo[2])
    if bet_type == BetType.SANRENTAN:
        if exponents is not None:
            return prob_trifecta_corrected(win_probs, combo[0], combo[1], combo[2], exponents)
        return prob_trifecta(win_probs, combo[0], combo[1], combo[2])
    raise ValueError(f"未知の馬券種: {bet_type}")


# ---------------------------------------------------------------------------
# Benter (1994) べき乗補正 Harville — 三連単/三連複/馬単/馬連の順序確率を補正する
#
# 素の Harville（式6）は 2着・3着の条件付き確率を系統的にバイアスする（人気馬の
# 複勝/連対を過大評価）。Benter は勝率 π から「着位ごとに尖り方を変えた」配列を作って
# 補正する（Henery 1981 / Stern 1990 / Lo & Bacon-Shone 1992 の単純法）:
#     σ_i = π_i^γ / Σ_j π_j^γ        （2着用・式7）
#     τ_i = π_i^δ / Σ_j π_j^δ        （3着用・式8）
#     P(i→j→k) = π_i · σ_j/(1-σ_i) · τ_k/(1-τ_i-τ_j)        （式9）
# γ,δ は過去レースの 1-2-3 着順の最尤推定で求める（競馬場ごとに異なる。Benter の香港データ
# では γ≈0.81, δ≈0.65＝1未満で人気馬の2/3着確率を下げる方向）。γ=δ=1 で素の Harville に一致。
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class PlaceExponents:
    """着位別べき指数。γ=2着, δ=3着。既定 1.0（素の Harville）。

    Benter (1994) の香港データの参考値は γ=0.81, δ=0.65（普遍定数ではなく要・自前 fit）。
    """

    gamma: float = 1.0
    delta: float = 1.0

    # Benter (1994) 香港データの参考値（初期値・既定にはしない＝競馬場依存のため）
    BENTER_HK: "PlaceExponents | None" = None


PlaceExponents.BENTER_HK = PlaceExponents(gamma=0.81, delta=0.65)


def place_adjusted(win_probs: Probabilities, exponent: float) -> dict[int, float]:
    """勝率を exponent 乗して再正規化した配列を返す（式7/8）。exponent=1 は恒等。

    exponent<1 で分布が平坦化（人気馬の比重を下げる）、>1 で尖る。π_i=0 は 0 のまま。
    """
    p = normalize(win_probs)
    if exponent == 1.0:
        return p
    powered = {u: (pv ** exponent if pv > 0 else 0.0) for u, pv in p.items()}
    total = float(sum(powered.values()))
    if total <= 0:
        return p  # 退化時は素の勝率へフォールバック
    return {u: v / total for u, v in powered.items()}


def prob_exacta_corrected(
    win_probs: Probabilities, first: int, second: int, exp: PlaceExponents
) -> float:
    """馬単（first→second）のべき乗補正版。2着に σ(γ乗) を使う。"""
    pi = normalize(win_probs)
    sigma = place_adjusted(win_probs, exp.gamma)
    p1 = pi[first]
    denom2 = 1.0 - sigma[first]
    if p1 >= 1.0 or denom2 <= 0:
        return 0.0
    return p1 * (sigma[second] / denom2)


def prob_quinella_corrected(
    win_probs: Probabilities, horse_a: int, horse_b: int, exp: PlaceExponents
) -> float:
    """馬連（順不同）のべき乗補正版。両順序の補正馬単の和。"""
    return prob_exacta_corrected(win_probs, horse_a, horse_b, exp) + prob_exacta_corrected(
        win_probs, horse_b, horse_a, exp
    )


def prob_trifecta_corrected(
    win_probs: Probabilities, first: int, second: int, third: int, exp: PlaceExponents
) -> float:
    """三連単（first→second→third）のべき乗補正版（Benter 式9）。

    1着=π, 2着=σ(γ乗), 3着=τ(δ乗)。各着位で対応する配列の和から既出馬を引いて条件付け。
    γ=δ=1 のとき :func:`prob_trifecta` と一致する。
    """
    pi = normalize(win_probs)
    sigma = place_adjusted(win_probs, exp.gamma)
    tau = place_adjusted(win_probs, exp.delta)
    p1 = pi[first]
    if p1 >= 1.0:
        return 0.0
    denom2 = 1.0 - sigma[first]
    if denom2 <= 0:
        return 0.0
    denom3 = 1.0 - tau[first] - tau[second]
    if denom3 <= 0:
        return 0.0
    return p1 * (sigma[second] / denom2) * (tau[third] / denom3)


def prob_trio_corrected(
    win_probs: Probabilities, horse_a: int, horse_b: int, horse_c: int, exp: PlaceExponents
) -> float:
    """三連複（順不同）のべき乗補正版。全6順列の補正三連単の和。"""
    return sum(
        prob_trifecta_corrected(win_probs, f, s, t, exp)
        for f, s, t in permutations((horse_a, horse_b, horse_c))
    )


def fit_place_exponents(
    races: Sequence[tuple[Probabilities, tuple[int, int, int]]],
    *,
    init: tuple[float, float] = (0.81, 0.65),
) -> PlaceExponents:
    """過去レースの (勝率, 観測 1-2-3着) から (γ, δ) を最尤推定する。

    観測着順の補正三連単確率の対数尤度を最大化（Nelder-Mead）。races の勝率はリーク回避の
    ため **out-of-sample のモデル勝率**を渡すこと（Benter: 合成・補正は OOS 推定で評価）。
    scipy 未導入や最適化失敗時は init をそのまま返す（fail-soft）。
    """
    valid = [(wp, order) for wp, order in races if order and len(order) == 3]
    if not valid:
        return PlaceExponents(*init)

    def nll(theta: Sequence[float]) -> float:
        g, d = float(theta[0]), float(theta[1])
        if g <= 0 or d <= 0:
            return 1e18
        exp = PlaceExponents(gamma=g, delta=d)
        total = 0.0
        for wp, (f, s, t) in valid:
            p = prob_trifecta_corrected(wp, f, s, t, exp)
            total -= log(p if p > 1e-12 else 1e-12)
        return total

    try:
        from scipy.optimize import minimize

        res = minimize(nll, list(init), method="Nelder-Mead")
        g, d = float(res.x[0]), float(res.x[1])
        if g > 0 and d > 0:
            return PlaceExponents(gamma=g, delta=d)
    except Exception:  # noqa: BLE001 — scipy 未導入/最適化失敗は init へフォールバック
        pass
    return PlaceExponents(*init)


def save_place_exponents(exp: PlaceExponents, path: str) -> None:
    """較正済み (γ, δ) を JSON（既定 models/place_exponents.json）へ保存する。"""
    import json
    import os

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"gamma": exp.gamma, "delta": exp.delta}, f, ensure_ascii=False, indent=2)


def load_place_exponents(path: str) -> PlaceExponents | None:
    """保存済み (γ, δ) を読み込む。ファイルが無ければ None（素の Harville を使う想定）。"""
    import json
    import os

    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        d = json.load(f)
    return PlaceExponents(gamma=float(d["gamma"]), delta=float(d["delta"]))


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
