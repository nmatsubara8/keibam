"""勝率較正レイヤ（src.policies._calibration）のテスト。"""

import numpy as np
import pytest

from src.policies import _calibration as C
from src.policies._calibration import IsotonicCalibrator


def _distorted_dataset(trues, raws, n=2000):
    """各 (raw, true) で n サンプル、勝敗を true 頻度で割り当てた決定論的データ。"""
    raw_samples, out_samples = [], []
    for r, t in zip(raws, trues, strict=False):
        n1 = round(t * n)
        raw_samples += [r] * n
        out_samples += [1] * n1 + [0] * (n - n1)
    return raw_samples, out_samples


class TestIsotonicFit:
    def test_corrects_monotone_distortion(self):
        # raw = sqrt(true): 低勝率を過大・高勝率を過小評価（人気-穴バイアスの形）
        trues = [0.05, 0.10, 0.20, 0.35, 0.50]
        raws = [float(np.sqrt(t)) for t in trues]
        raw_s, out_s = _distorted_dataset(trues, raws)
        cal = C.fit_isotonic_calibrator(raw_s, out_s)
        for r, t in zip(raws, trues, strict=False):
            assert cal.predict([r])[0] == pytest.approx(t, abs=0.03)

    def test_reduces_calibration_error(self):
        trues = [0.05, 0.10, 0.20, 0.35, 0.50]
        raws = [float(np.sqrt(t)) for t in trues]
        raw_s, out_s = _distorted_dataset(trues, raws)
        cal = C.fit_isotonic_calibrator(raw_s, out_s)
        err_raw = C.calibration_error(raw_s, out_s)
        err_cal = C.calibration_error(cal.predict(raw_s), out_s)
        assert err_cal < err_raw

    def test_monotone_nondecreasing(self):
        trues = [0.05, 0.10, 0.20, 0.35, 0.50]
        raws = [float(np.sqrt(t)) for t in trues]
        cal = C.fit_isotonic_calibrator(*_distorted_dataset(trues, raws))
        preds = cal.predict(sorted(raws))
        assert np.all(np.diff(preds) >= -1e-9)


class TestWithinRaceNormalize:
    def test_sums_to_one_per_race(self):
        cal = IsotonicCalibrator(x=(0.0, 1.0), y=(0.0, 1.0))  # 恒等
        race_ids = ["r1", "r1", "r1", "r2", "r2"]
        raw = [0.6, 0.3, 0.1, 0.5, 0.5]
        out = C.calibrate_within_race(race_ids, raw, cal)
        assert out[:3].sum() == pytest.approx(1.0)
        assert out[3:].sum() == pytest.approx(1.0)

    def test_preserves_order_within_race(self):
        cal = IsotonicCalibrator(x=(0.0, 1.0), y=(0.0, 1.0))
        race_ids = ["r1", "r1", "r1"]
        raw = [0.1, 0.6, 0.3]
        out = C.calibrate_within_race(race_ids, raw, cal)
        # 元の大小関係（2番目>3番目>1番目）が保たれる
        assert out[1] > out[2] > out[0]


class TestPersist:
    def test_round_trip(self, tmp_path):
        path = str(tmp_path / "cal.json")
        cal = IsotonicCalibrator(x=(0.0, 0.2, 0.5, 1.0), y=(0.0, 0.3, 0.6, 1.0))
        C.save_calibrator(cal, path)
        loaded = C.load_calibrator(path)
        assert loaded == cal
        assert loaded.predict([0.35])[0] == pytest.approx(np.interp(0.35, cal.x, cal.y))

    def test_missing_none(self, tmp_path):
        assert C.load_calibrator(str(tmp_path / "x.json")) is None

    def test_empty_calibrator_is_identity(self):
        cal = IsotonicCalibrator(x=(), y=())
        assert cal.predict([0.4, 0.7]).tolist() == [0.4, 0.7]
