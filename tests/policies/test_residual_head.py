"""市場アンカー残差ヘッド(_residual_head)の単体テスト。"""
from __future__ import annotations

import math

from src.policies._residual_head import (
    fit_residual_head,
    residual_predict,
    residual_win_probs,
)


def test_zero_theta_recovers_market():
    odds = {1: 2.0, 2: 4.0, 3: 8.0}
    feats = {1: {"a": 0.5}, 2: {"a": -0.3}, 3: {"a": 0.1}}
    from src.policies._market_residual import market_probs
    p = residual_win_probs(odds, feats, {"a": 0.0})
    q = market_probs(odds)
    for h in q:
        assert abs(p[h] - q[h]) < 1e-9          # θ≡0 → P≡q（帰無＝市場）


def test_residual_predict_linear():
    assert residual_predict({"a": 2.0, "b": -1.0}, {"a": 0.5, "b": 2.0}) == 0.5 * 2 + 2 * -1


def test_fit_recovers_informative_sign():
    # 特徴 a が高い馬が勝ちやすい合成データ → fit した θ_a は正になるはず
    races = []
    for k in range(400):
        # 3頭、市場は均等っぽく、a の大きい馬(=馬番1)が実際に勝つ
        odds = {1: 3.0, 2: 3.0, 3: 3.0}
        feats = {1: {"a": 1.0}, 2: {"a": 0.0}, 3: {"a": -1.0}}
        winner = 1 if (k % 4 != 0) else 2       # 75% は a最大馬が勝つ
        races.append({"odds": odds, "feats": feats, "winner": winner})
    theta = fit_residual_head(races, ["a"], l2=0.01)
    assert theta["a"] > 0.2                      # 情報のある向きを回収

def test_empty_or_no_features_returns_zero():
    assert fit_residual_head([], ["a"], l2=1.0) == {"a": 0.0}
    assert fit_residual_head([{"odds": {1: 2.0}, "feats": {1: {}}, "winner": 1}], []) == {}
