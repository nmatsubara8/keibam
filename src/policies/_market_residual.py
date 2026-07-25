"""市場アンカー残差モデル（Benter/予測市場理論）＝「真の分布を当てる」のではなく
「市場が間違っている差分（Edge）を見つける」ための機構。

**＝市場を事前分布とする階層ベイズランキングモデルの第1層。** 強度の定義
    強度   s_i = log q_i + r_i          （q_i=市場implied勝率, r_i=モデル残差）
    真勝率 P_i = softmax(s)_i = q_i·e^{r_i} / Σ_j q_j·e^{r_j}
    エッジ Edge_i = log(P_i / q_i)       （確率空間では P_i − q_i）
は softmax を通すとベイズ更新そのもの: **市場 q = prior、モデル e^{r} = 尤度、P = posterior**。

この定義の要点は **残差ゼロ (r≡0) のとき P ≡ q** になること。softmax(log q)=normalize(q)=q
なので、モデルが何も言わなければ「真の分布＝市場」に一致し、Edge=0 → 賭けが1点も出ない。
これが正しい帰無仮説（null）で、過去の生回収率フィット（placebo で 8–38% しか壊れなかった）
と違い、機構自体には後知恵の自由度が無い。r に信号を入れて初めて Edge が立ち、賭けが出る。
**公開データだけで Edge≈0 に着地するのは「失敗」ではなく「正常終了」**（逆に公開データで
Edge>0 が出たら、まずリーク/過学習/評価方法を疑う）。

市場 q の位置付け: 十分統計量（＝パラメータの全情報保持）とまでは言えない。市場は公開情報・
参加者知識・オッズ形成の集約＝**公開情報に対する非常に強力な要約量（強力な prior）**であって、
未知情報までは保持しない。エッジの源泉は市場に乗っていない直交情報（例: JRDB系）にのみ有り得る。

r_i の作り方（本モジュールはここは受け取るだけ・学習は別レイヤ）:
    r_i = f_θ(x_i)  … 特徴量からの残差ヘッド。教師は市場アンカー listwise NLL（下記）。
    正則化必須: L = L_PL + λΣ_i f_i²（or λ|f_i|）。公開情報だけなら本来 f≈0 であるべきなので、
    **「市場から離れるには証拠を要求する」**性質を明示的に持たせる（過学習防止の要）。

純粋関数のみ（pandas/IO 非依存）。q は :func:`implied_from_odds` に一本化して二重定義を避ける。
"""
from __future__ import annotations

import math
from typing import Mapping

from src.preprocessing._place_prob import implied_from_odds
from src.preprocessing._place_prob import normalize


def market_probs(odds_map: Mapping) -> dict[int, float]:
    """単勝オッズ → 市場implied勝率 q_i（レース内 Σ=1・控除抜き）。

    ``implied_from_odds(normalized=True)`` の薄いラッパ（呼び名を Edge 文脈に合わせる）。
    非正/NaN のオッズは除外される。
    """
    return implied_from_odds(odds_map, normalized=True)


def zero_residual(keys) -> dict[int, float]:
    """残差ゼロ辞書。機構検査（P≡q・Edge≡0・賭け0点）の帰無入力に使う。"""
    return {k: 0.0 for k in keys}


def anchored_strengths(
    q: Mapping[int, float], residual: Mapping[int, float]
) -> dict[int, float]:
    """市場アンカー強度 s_i = log q_i + r_i。q_i<=0 は log を避けて除外する。

    q は市場implied勝率（:func:`market_probs`）、residual はモデルの log 比残差 r_i。
    """
    out: dict[int, float] = {}
    for h, qi in q.items():
        if qi and qi > 0:
            out[h] = math.log(qi) + float(residual.get(h, 0.0))
    return out


def _softmax(strengths: Mapping[int, float]) -> dict[int, float]:
    """強度 → softmax（レース内 Σ=1）。数値安定のため最大値を引く。"""
    if not strengths:
        return {}
    m = max(strengths.values())
    exp = {k: math.exp(float(v) - m) for k, v in strengths.items()}
    z = sum(exp.values())
    return {k: v / z for k, v in exp.items()} if z > 0 else {k: 0.0 for k in strengths}


def true_probs(
    odds_map: Mapping, residual: Mapping[int, float] | None = None
) -> dict[int, float]:
    """市場アンカー残差モデルの真勝率 P_i = softmax(log q + r)。

    residual=None（または全0）なら **P ≡ q（市場そのもの）** を返す（帰無）。
    """
    q = market_probs(odds_map)
    r = residual or {}
    return _softmax(anchored_strengths(q, r))


def edge(
    p_true: Mapping[int, float], q: Mapping[int, float]
) -> dict[int, float]:
    """対数エッジ Edge_i = log(P_i / q_i)。P も q も正の馬だけ返す。

    Edge>0 = 市場が過小評価（買い）、Edge<0 = 過大評価（見送り/売り）。
    確率空間の差分が欲しい場合は :func:`edge_prob` を使う。
    """
    out: dict[int, float] = {}
    for h, pi in p_true.items():
        qi = q.get(h, 0.0)
        if pi > 0 and qi > 0:
            out[h] = math.log(pi / qi)
    return out


def edge_prob(
    p_true: Mapping[int, float], q: Mapping[int, float]
) -> dict[int, float]:
    """確率空間のエッジ P_i − q_i（賭け判定・可視化用）。"""
    return {h: float(pi) - float(q.get(h, 0.0)) for h, pi in p_true.items()}


def kl_from_market(
    p_true: Mapping[int, float], q: Mapping[int, float]
) -> float:
    """VOI（情報価値）指標 D_KL(P ∥ q) = Σ_i P_i·log(P_i/q_i)。

    「市場からどれだけ新しい情報を得たか」の情報理論的な量（nats）。P≡q（帰無）で 0。
    Edge の期待値 E_P[Edge_i] と一致する（Edge_i=log(P_i/q_i) の定義から）。

    使い方（VOI 解析）: 特徴量群（例: JRDB）を追加したモデルと無しのモデルで
    ΔKL = mean_races[ KL(P_with‖q) − KL(P_without‖q) ] を測る。ΔNLL/ΔECE/ΔROI と併せて
    「その特徴量が市場に対する情報量をどれだけ増やしたか」を単独で定量化できる。
    注意: KL 増加は情報量の増加であって**正しさの保証ではない**（過学習でも増える）。
    必ず ΔNLL（proper scoring・OOS）で正しさを確認した上で VOI を解釈する。
    """
    out = 0.0
    for h, pi in p_true.items():
        qi = q.get(h, 0.0)
        if pi > 0 and qi > 0:
            out += float(pi) * math.log(float(pi) / float(qi))
    return max(0.0, out)


def market_anchored_nll(
    q: Mapping[int, float],
    residual: Mapping[int, float],
    winner: int,
    *,
    l2: float = 0.0,
    l1: float = 0.0,
) -> float:
    """市場アンカー listwise NLL（1着＝winner の負の対数尤度）＋残差正則化。

    L = −log softmax(log q + r)[winner] + l2·Σr² + l1·Σ|r|。
    r≡0 のとき正則化項も 0 で L=−log q_winner（＝市場の尤度）に一致し、モデルは
    「市場を上回った分から正則化コストを引いた分」だけ損失を下げられる（Residual Modeling）。

    正則化の意味: 公開情報だけなら本来 r≈0 であるべき。l2（Gaussian prior）/ l1（Laplace prior）
    は**「市場から離れるには証拠を要求する」**制約で、残差ヘッドの過学習を機構的に抑える。
    proper scoring rule なので過小/過大評価に対して不偏。学習レイヤはこれを全レース平均で最小化する。
    """
    p = _softmax(anchored_strengths(q, residual))
    pw = p.get(winner, 0.0)
    nll = -math.log(pw) if pw > 0 else 30.0
    if l2 > 0:
        nll += l2 * sum(float(v) ** 2 for v in residual.values())
    if l1 > 0:
        nll += l1 * sum(abs(float(v)) for v in residual.values())
    return nll
