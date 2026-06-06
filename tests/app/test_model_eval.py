"""app/_model_eval.py の単体テスト。

DataSplitter を使わず、ミニマムなスタブデータで各算出関数を検証する。
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from app._model_eval import (
    _get_splits,
    _split_by_date,
    compute_calib_curves,
    compute_ev_sweep,
    compute_stacking_auc,
    load_featured_data,
)
from src.constants._results_cols import ResultsCols


# ---------------------------------------------------------------------------
# スタブ featured_data
# ---------------------------------------------------------------------------

def _make_featured(n_races: int = 20, horses_per_race: int = 8, seed: int = 0) -> pd.DataFrame:
    """テスト用の最小 featured_data。date / rank / TANSHO_ODDS / feat を持つ。"""
    rng = np.random.default_rng(seed)
    rows = []
    base_date = pd.Timestamp("2023-01-01")
    for i in range(n_races):
        race_id = f"race_{i:03d}"
        date = base_date + pd.Timedelta(days=i)
        for h in range(1, horses_per_race + 1):
            rows.append(
                {
                    ResultsCols.UMABAN: h,
                    "rank": 1 if h == 1 else 0,  # 1 番馬が必ず勝つ（テスト用）
                    "date": date,
                    ResultsCols.TANSHO_ODDS: float(rng.uniform(2.0, 20.0)),
                    "feat1": rng.normal(),
                    "feat2": rng.normal(),
                }
            )
    return pd.DataFrame(rows, index=[f"race_{i // horses_per_race:03d}" for i in range(n_races * horses_per_race)])


# ---------------------------------------------------------------------------
# スタブモデル（predict_proba を持つ最小モデル）
# ---------------------------------------------------------------------------

class _StubModel:
    def predict_proba(self, x):
        n = len(x)
        p = np.clip(np.random.default_rng(42).uniform(0.1, 0.9, n), 0, 1)
        return np.column_stack([1 - p, p])


class _StubBase:
    def predict_proba(self, x):
        n = len(x)
        p = np.clip(np.random.default_rng(0).uniform(0.1, 0.8, n), 0, 1)
        return np.column_stack([1 - p, p])


class _StubCalibModel:
    """CalibratedModel の最小スタブ（_base_model / predict_proba）。"""

    class _FakeStacking:
        def __init__(self):
            self._base_models = [_StubBase(), _StubBase()]

        def predict_proba(self, x):
            n = len(x)
            p = np.full(n, 0.3)
            return np.column_stack([1 - p, p])

        def base_predictions(self, x):
            return [m.predict_proba(x)[:, 1] for m in self._base_models]

    def __init__(self):
        self._base_model = self._FakeStacking()

    def predict_proba(self, x):
        n = len(x)
        p = np.full(n, 0.35)
        return np.column_stack([1 - p, p])


class _KeibaAIStub:
    """KeibaAI 風スタブ: effective_model / _calibrated_model プロパティ。"""

    def __init__(self):
        self._calibrated_model = _StubCalibModel()

    @property
    def effective_model(self):
        return self._calibrated_model

    def predict_proba(self, x):
        return self._calibrated_model.predict_proba(x)


# ---------------------------------------------------------------------------
# _split_by_date
# ---------------------------------------------------------------------------

def test_split_by_date_sizes():
    df = _make_featured(n_races=10, horses_per_race=4)
    train, test = _split_by_date(df, test_size=0.2)
    total_races = df.index.nunique()
    assert abs(test.index.nunique() - round(total_races * 0.2)) <= 1


def test_split_by_date_no_overlap():
    df = _make_featured(n_races=10, horses_per_race=4)
    train, test = _split_by_date(df, test_size=0.3)
    assert set(train.index.unique()) & set(test.index.unique()) == set()


def test_split_by_date_temporal_order():
    df = _make_featured(n_races=10, horses_per_race=4)
    train, test = _split_by_date(df, test_size=0.3)
    max_train_date = train["date"].max()
    min_test_date = test["date"].min()
    assert max_train_date <= min_test_date


# ---------------------------------------------------------------------------
# _get_splits
# ---------------------------------------------------------------------------

def test_get_splits_keys():
    df = _make_featured(n_races=20, horses_per_race=4)
    splits = _get_splits(df, test_size=0.2, valid_size=0.2)
    for key in ["X_calib", "y_calib", "X_test", "X_test_model", "y_test"]:
        assert key in splits


def test_get_splits_x_test_model_drops_odds():
    df = _make_featured(n_races=20, horses_per_race=4)
    splits = _get_splits(df, test_size=0.2, valid_size=0.2)
    assert ResultsCols.TANSHO_ODDS not in splits["X_test_model"].columns


def test_get_splits_x_test_keeps_odds():
    df = _make_featured(n_races=20, horses_per_race=4)
    splits = _get_splits(df, test_size=0.2, valid_size=0.2)
    assert ResultsCols.TANSHO_ODDS in splits["X_test"].columns


# ---------------------------------------------------------------------------
# load_featured_data
# ---------------------------------------------------------------------------

def test_load_featured_data_missing_returns_none(tmp_path):
    result = load_featured_data(str(tmp_path / "no_such.pkl"))
    assert result is None


def test_load_featured_data_roundtrip(tmp_path):
    import pickle
    df = _make_featured(n_races=5)
    path = str(tmp_path / "feat.pkl")
    with open(path, "wb") as f:
        pickle.dump(df, f)
    loaded = load_featured_data(path)
    assert loaded is not None
    assert len(loaded) == len(df)


# ---------------------------------------------------------------------------
# compute_calib_curves
# ---------------------------------------------------------------------------

def test_compute_calib_curves_none_without_calibrated_model():
    feat = _make_featured(n_races=20, horses_per_race=4)
    result = compute_calib_curves(_StubModel(), feat)
    assert result is None


def test_compute_calib_curves_returns_dict():
    feat = _make_featured(n_races=20, horses_per_race=4)
    ai = _KeibaAIStub()
    result = compute_calib_curves(ai, feat)
    assert result is not None
    assert "y_true" in result and "prob_pre" in result and "prob_post" in result


def test_compute_calib_curves_probs_in_range():
    feat = _make_featured(n_races=20, horses_per_race=4)
    ai = _KeibaAIStub()
    result = compute_calib_curves(ai, feat)
    assert result is not None
    assert np.all((result["prob_pre"] >= 0) & (result["prob_pre"] <= 1))
    assert np.all((result["prob_post"] >= 0) & (result["prob_post"] <= 1))


def test_compute_calib_curves_lengths_match():
    feat = _make_featured(n_races=20, horses_per_race=4)
    ai = _KeibaAIStub()
    result = compute_calib_curves(ai, feat)
    assert result is not None
    assert len(result["y_true"]) == len(result["prob_pre"]) == len(result["prob_post"])


# ---------------------------------------------------------------------------
# compute_stacking_auc
# ---------------------------------------------------------------------------

def test_compute_stacking_auc_none_without_stacking():
    feat = _make_featured(n_races=20, horses_per_race=4)
    assert compute_stacking_auc(_StubModel(), feat) is None


def test_compute_stacking_auc_returns_dict():
    feat = _make_featured(n_races=20, horses_per_race=4)
    ai = _KeibaAIStub()
    result = compute_stacking_auc(ai, feat)
    assert result is not None
    assert "base_probs" in result and "meta_probs" in result and "base_names" in result


def test_compute_stacking_auc_two_base_models():
    feat = _make_featured(n_races=20, horses_per_race=4)
    ai = _KeibaAIStub()
    result = compute_stacking_auc(ai, feat)
    assert result is not None
    assert len(result["base_probs"]) == 2
    assert len(result["base_names"]) == 2


# ---------------------------------------------------------------------------
# compute_ev_sweep
# ---------------------------------------------------------------------------

def test_compute_ev_sweep_returns_dataframe():
    feat = _make_featured(n_races=20, horses_per_race=4)
    ai = _KeibaAIStub()
    df = compute_ev_sweep(ai, feat, thresholds=[1.0, 1.2, 1.5])
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["threshold", "return_rate", "sharpe_ratio", "n_bets"]


def test_compute_ev_sweep_row_count():
    feat = _make_featured(n_races=20, horses_per_race=4)
    ai = _KeibaAIStub()
    thresholds = [1.0, 1.2, 1.5]
    df = compute_ev_sweep(ai, feat, thresholds=thresholds)
    assert len(df) == len(thresholds)


def test_compute_ev_sweep_n_bets_nonneg():
    feat = _make_featured(n_races=20, horses_per_race=4)
    ai = _KeibaAIStub()
    df = compute_ev_sweep(ai, feat, thresholds=[0.5, 1.0, 2.0])
    assert (df["n_bets"] >= 0).all()


def test_compute_ev_sweep_high_threshold_zero_bets():
    feat = _make_featured(n_races=20, horses_per_race=4)
    ai = _KeibaAIStub()
    df = compute_ev_sweep(ai, feat, thresholds=[999.0])
    assert df.iloc[0]["n_bets"] == 0


def test_compute_ev_sweep_fallback_for_plain_model():
    """predict_proba を直接持つモデルでも動く。"""
    feat = _make_featured(n_races=20, horses_per_race=4)
    df = compute_ev_sweep(_StubModel(), feat, thresholds=[1.0, 1.5])
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
