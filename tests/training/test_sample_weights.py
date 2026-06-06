"""§2 / §2h サンプル重みユーティリティのテスト。"""

import numpy as np
import pytest

from src.constants._bet_thresholds import TrainingWeights
from src.training._sample_weights import (
    compute_ev_weights,
    ev_sigmoid_weights,
    normalize_within_race,
)


class TestEvSigmoidWeights:
    def test_ev_equals_center_gives_half(self):
        """EV = center (=1.0) のサンプルは重み 0.5。"""
        # pred * odds = 1.0 → EV = center
        pred = np.array([0.5])
        odds = np.array([2.0])
        w = ev_sigmoid_weights(pred, odds, k=5.0, center=1.0)
        assert w[0] == pytest.approx(0.5)

    def test_high_ev_weight_near_one(self):
        """EV >> 1 のサンプルは重み ≈ 1。"""
        pred = np.array([0.9])
        odds = np.array([10.0])  # EV = 9.0
        w = ev_sigmoid_weights(pred, odds, k=5.0, center=1.0)
        assert w[0] > 0.99

    def test_low_ev_weight_near_zero(self):
        """EV << 1 のサンプルは重み ≈ 0。"""
        pred = np.array([0.01])
        odds = np.array([1.5])  # EV = 0.015
        w = ev_sigmoid_weights(pred, odds, k=5.0, center=1.0)
        assert w[0] < 0.05

    def test_monotonic_in_ev(self):
        """EV が増えると重みも単調増加。"""
        pred = np.array([0.1, 0.3, 0.5, 0.7])
        odds = np.array([2.0, 2.0, 2.0, 2.0])  # EV = 0.2, 0.6, 1.0, 1.4
        w = ev_sigmoid_weights(pred, odds)
        assert np.all(np.diff(w) > 0)

    def test_no_overflow_extreme_values(self):
        """極端な EV でもオーバーフローしない。"""
        pred = np.array([1.0, 0.0])
        odds = np.array([1000.0, 1.0])
        w = ev_sigmoid_weights(pred, odds, k=100.0)
        assert np.all(np.isfinite(w))
        assert w[0] == pytest.approx(1.0)

    def test_default_k_from_constant(self):
        """既定の k は TrainingWeights.SIGMOID_K。"""
        pred = np.array([0.5])
        odds = np.array([2.0])
        w_default = ev_sigmoid_weights(pred, odds)
        w_explicit = ev_sigmoid_weights(pred, odds, k=TrainingWeights.SIGMOID_K)
        assert w_default[0] == pytest.approx(w_explicit[0])


class TestNormalizeWithinRace:
    def test_weights_sum_to_one_per_race(self):
        weights = np.array([1.0, 3.0, 2.0, 2.0])
        race_ids = np.array(["r1", "r1", "r2", "r2"])
        result = normalize_within_race(weights, race_ids)
        # r1: [1,3] → sum 4 → [0.25, 0.75]; r2: [2,2] → [0.5,0.5]
        assert result[race_ids == "r1"].sum() == pytest.approx(1.0)
        assert result[race_ids == "r2"].sum() == pytest.approx(1.0)

    def test_correct_normalized_values(self):
        weights = np.array([1.0, 3.0])
        race_ids = np.array(["r1", "r1"])
        result = normalize_within_race(weights, race_ids)
        assert result[0] == pytest.approx(0.25)
        assert result[1] == pytest.approx(0.75)

    def test_zero_sum_race_uniform_fallback(self):
        """全重み 0 のレースは均等配分。"""
        weights = np.array([0.0, 0.0, 0.0])
        race_ids = np.array(["r1", "r1", "r1"])
        result = normalize_within_race(weights, race_ids)
        assert np.allclose(result, 1.0 / 3.0)

    def test_equal_contribution_across_race_sizes(self):
        """頭数が異なるレースが等価な貢献度（合計 1）を持つ。"""
        weights = np.array([1.0] * 6 + [1.0] * 16)
        race_ids = np.array(["small"] * 6 + ["big"] * 16)
        result = normalize_within_race(weights, race_ids)
        assert result[race_ids == "small"].sum() == pytest.approx(1.0)
        assert result[race_ids == "big"].sum() == pytest.approx(1.0)

    def test_length_mismatch_raises(self):
        with pytest.raises(ValueError):
            normalize_within_race(np.array([1.0, 2.0]), np.array(["r1"]))


class TestComputeEvWeights:
    def test_normalized_output_sums_per_race(self):
        pred = np.array([0.5, 0.6, 0.3, 0.8])
        odds = np.array([2.0, 2.0, 2.0, 2.0])
        race_ids = np.array(["r1", "r1", "r2", "r2"])
        result = compute_ev_weights(pred, odds, race_ids)
        assert result[race_ids == "r1"].sum() == pytest.approx(1.0)
        assert result[race_ids == "r2"].sum() == pytest.approx(1.0)

    def test_normalize_false_skips_normalization(self):
        pred = np.array([0.5, 0.5])
        odds = np.array([2.0, 2.0])
        race_ids = np.array(["r1", "r1"])
        result = compute_ev_weights(pred, odds, race_ids, normalize=False)
        # 正規化なし → 各重みは sigmoid(0)=0.5
        assert np.allclose(result, 0.5)
