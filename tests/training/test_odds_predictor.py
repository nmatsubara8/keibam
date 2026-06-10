"""Layer2 オッズ予測のテスト。"""

import numpy as np
import pandas as pd
import pytest

from src.training._odds_predictor import IdentityOddsPredictor
from src.training._odds_predictor import LgbOddsPredictor
from src.training._odds_predictor import _normalize_within_race


def test_identity_returns_current():
    feats = pd.DataFrame({"current_odds": [2.0, 5.0, 10.0]}, index=["r1", "r1", "r1"])
    pred = IdentityOddsPredictor().predict(feats)
    assert list(pred) == [2.0, 5.0, 10.0]


def test_normalize_preserves_overround():
    cur = pd.Series([2.0, 4.0, 8.0], index=["r1", "r1", "r1"])
    pred = pd.Series([3.0, 3.0, 3.0], index=["r1", "r1", "r1"])
    norm = _normalize_within_race(pred, cur)
    # 正規化後の implied-prob 総和は現在オッズの総和に一致
    assert (1.0 / norm).sum() == pytest.approx((1.0 / cur).sum())


def test_lgb_predicts_positive_and_normalized():
    rng = np.random.default_rng(0)
    n = 200
    current = rng.uniform(1.5, 30.0, size=n)
    # 確定オッズ = 現在オッズ * ノイズ
    final = current * rng.uniform(0.8, 1.2, size=n)
    race_ids = ["r%d" % (i // 4) for i in range(n)]
    feats = pd.DataFrame(
        {"current_odds": current, "minutes_to_post": rng.uniform(0, 1440, size=n), "ninki": rng.integers(1, 18, size=n)},
        index=race_ids,
    )
    predictor = LgbOddsPredictor(
        feature_cols=["current_odds", "minutes_to_post", "ninki"], min_child_samples=5, n_estimators=20, verbose=-1
    )
    predictor.fit(feats, final)
    pred = predictor.predict(feats)
    assert (pred > 0).all()
    assert len(pred) == n


def test_lgb_requires_fit():
    feats = pd.DataFrame({"current_odds": [2.0]}, index=["r1"])
    with pytest.raises(RuntimeError):
        LgbOddsPredictor(feature_cols=["current_odds"]).predict(feats)
