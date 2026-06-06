"""§8 分類性能指標 classification_metrics のテスト。"""

import numpy as np
import pytest

from src.simulation._metrics import _f1_at_topk, classification_metrics


class TestClassificationMetrics:
    def test_keys_present(self):
        y = np.array([0, 1, 0, 1, 0, 1])
        p = np.array([0.1, 0.9, 0.2, 0.8, 0.3, 0.7])
        m = classification_metrics(y, p, top_n=3)
        assert "log_loss" in m
        assert "brier_score" in m
        assert "auc" in m
        assert "f1_score_top1" in m
        assert "f1_score_top3" in m

    def test_perfect_prediction_low_loss(self):
        y = np.array([0, 0, 1, 1])
        p = np.array([0.01, 0.02, 0.98, 0.99])
        m = classification_metrics(y, p)
        assert m["log_loss"] < 0.1
        assert m["brier_score"] < 0.01
        assert m["auc"] == pytest.approx(1.0)

    def test_brier_score_value(self):
        y = np.array([1, 0])
        p = np.array([0.7, 0.2])
        m = classification_metrics(y, p)
        # ((0.7-1)^2 + (0.2-0)^2)/2 = (0.09 + 0.04)/2 = 0.065
        assert m["brier_score"] == pytest.approx(0.065)

    def test_auc_nan_single_class(self):
        y = np.array([0, 0, 0])
        p = np.array([0.1, 0.5, 0.9])
        m = classification_metrics(y, p)
        assert np.isnan(m["auc"])

    def test_log_loss_computable_single_class(self):
        """単一クラスでも labels=[0,1] 指定で LogLoss は計算可能。"""
        y = np.array([0, 0, 0])
        p = np.array([0.1, 0.2, 0.3])
        m = classification_metrics(y, p)
        assert not np.isnan(m["log_loss"])

    def test_probabilities_clipped_no_inf(self):
        """確率 0/1 でも clip で inf にならない。"""
        y = np.array([1, 0])
        p = np.array([1.0, 0.0])
        m = classification_metrics(y, p)
        assert np.isfinite(m["log_loss"])

    def test_custom_top_n_key(self):
        y = np.array([0, 1, 0, 1])
        p = np.array([0.2, 0.8, 0.1, 0.9])
        m = classification_metrics(y, p, top_n=2)
        assert "f1_score_top2" in m


class TestF1AtTopK:
    def test_perfect_top1(self):
        y = np.array([0, 0, 1])
        p = np.array([0.1, 0.2, 0.9])
        # top-1 predicted = index 2, which is the actual positive → F1 = 1.0
        assert _f1_at_topk(y, p, 1) == pytest.approx(1.0)

    def test_empty_returns_nan(self):
        assert np.isnan(_f1_at_topk(np.array([]), np.array([]), 1))

    def test_k_capped_at_length(self):
        y = np.array([1, 0])
        p = np.array([0.9, 0.1])
        # k=5 but only 2 samples → top-2 predicted as positive
        result = _f1_at_topk(y, p, 5)
        assert np.isfinite(result)

    def test_all_negative_no_positive_pred_nan(self):
        y = np.array([0, 0, 0])
        p = np.array([0.1, 0.2, 0.3])
        # k=0 → no positive prediction and no positive truth → nan
        assert np.isnan(_f1_at_topk(y, p, 0))
