"""確信度スコアラのテスト。"""

import pytest

from src.portfolio._confidence import CompositeConfidenceScorer
from src.portfolio._confidence import ConfidenceSignals
from src.portfolio._confidence import agreement_from_predictions


def test_score_in_unit_interval():
    scorer = CompositeConfidenceScorer()
    s = scorer.score(ConfidenceSignals(model_agreement=0.8, odds_certainty=0.6, ev_margin=0.5))
    assert 0.0 <= s <= 1.0


def test_missing_signals_neutral():
    scorer = CompositeConfidenceScorer()
    assert scorer.score(ConfidenceSignals()) == 1.0


def test_ev_margin_monotonic():
    scorer = CompositeConfidenceScorer(weights={"ev": 1.0})
    low = scorer.score(ConfidenceSignals(ev_margin=0.1))
    high = scorer.score(ConfidenceSignals(ev_margin=2.0))
    assert high > low


def test_negative_ev_margin_is_zero_component():
    scorer = CompositeConfidenceScorer(weights={"ev": 1.0})
    assert scorer.score(ConfidenceSignals(ev_margin=-1.0)) == 0.0


def test_agreement_high_when_predictions_close():
    close = agreement_from_predictions([0.50, 0.51, 0.49])
    far = agreement_from_predictions([0.10, 0.90])
    assert close > far
    assert 0.0 <= far <= 1.0 and 0.0 <= close <= 1.0


def test_agreement_single_prediction_is_one():
    assert agreement_from_predictions([0.5]) == 1.0
