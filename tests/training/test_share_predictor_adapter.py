"""SharePredictorAdapter（AbstractOddsPredictor 互換）のテスト。"""

import pandas as pd
import pytest

from src.constants._odds_phases import OddsPhase
from src.training._odds_dynamics import IdentityShareModel
from src.training._odds_predictor import AbstractOddsPredictor
from src.training._share_predictor_adapter import SharePredictorAdapter
from src.training._share_predictor_adapter import shares_to_odds


def test_shares_to_odds_parimutuel():
    shares = pd.Series([0.4, 0.4, 0.2])
    odds = shares_to_odds(shares, takeout=0.2)
    # (1 - 0.2) / 0.4 = 2.0
    assert odds.tolist() == pytest.approx([2.0, 2.0, 4.0])
    # 逆変換: (1-takeout)/odds を正規化するとシェアに戻る
    back = (0.8 / odds)
    assert (back / back.sum()).tolist() == pytest.approx(shares.tolist())


def test_adapter_satisfies_odds_predictor_contract():
    obs = {
        "r1": {OddsPhase.THIRTY_MIN: pd.Series([0.5, 0.3, 0.2], index=["1", "2", "3"])},
    }
    adapter = SharePredictorAdapter(IdentityShareModel(), obs_lookup=lambda rid: obs.get(rid, {}))
    assert isinstance(adapter, AbstractOddsPredictor)

    features = pd.DataFrame(
        {"current_odds": [1.6, 2.7, 4.0]},
        index=pd.Index(["r1", "r1", "r1"], name="race_id"),
    )
    pred = adapter.predict(features)
    assert isinstance(pred, pd.Series)
    assert len(pred) == 3
    assert (pred > 0).all()
    # identity モデル + takeout 0.2: odds = 0.8/share
    assert pred.tolist() == pytest.approx([0.8 / 0.5, 0.8 / 0.3, 0.8 / 0.2])


def test_adapter_falls_back_to_current_odds_when_no_obs():
    adapter = SharePredictorAdapter(IdentityShareModel(), obs_lookup=lambda rid: {})
    features = pd.DataFrame(
        {"current_odds": [2.0, 5.0]},
        index=pd.Index(["rX", "rX"], name="race_id"),
    )
    pred = adapter.predict(features)
    assert pred.tolist() == [2.0, 5.0]
