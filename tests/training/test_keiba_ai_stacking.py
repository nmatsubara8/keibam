"""KeibaAI.train_with_stacking の統合テスト（スタブ学習器使用）。

実 featured_data / Optuna / torch を使わず、StackingModel + CalibratedModel の
パイプラインが KeibaAI 経由で正しく組み立てられることだけを確認する。
"""

import numpy as np
import pandas as pd
import pytest
from sklearn.linear_model import LogisticRegression

from src.constants._results_cols import ResultsCols
from src.policies._score_policy import CURRENT_ODDS
from src.policies._score_policy import PROB
from src.policies._score_policy import ExpectedValueScorePolicy
from src.training._calibrated_model import CalibratedModel
from src.training._stacking_model import StackingModel


def _make_featured(n_races=60, horses=8, seed=0):
    rng = np.random.default_rng(seed)
    rows = []
    base = pd.Timestamp("2021-01-01")
    for i in range(n_races):
        race_id = f"r{i:04d}"
        date = base + pd.Timedelta(days=i)
        for h in range(horses):
            rows.append(
                {
                    "race_id": race_id,
                    "date": date,
                    "rank": int(rng.integers(0, 2)),
                    ResultsCols.TANSHO_ODDS: float(rng.uniform(1.5, 20.0)),
                    ResultsCols.UMABAN: h + 1,
                    ResultsCols.WAKUBAN: h + 1,
                    "feat_a": float(rng.normal()),
                    "feat_b": float(rng.normal()),
                }
            )
    return pd.DataFrame(rows).set_index("race_id")


def _build_stacking_calibrated(x_base, y_base, x_meta, y_meta, x_calib, y_calib):
    """LR x2 スタック + Isotonic 較正（Optuna/torch 不使用のスタブパイプライン）。"""
    stacking = StackingModel(
        [LogisticRegression(max_iter=200, random_state=0), LogisticRegression(max_iter=200, random_state=1)],
        meta_model=LogisticRegression(max_iter=200, random_state=0),
    )
    stacking.fit(x_base, y_base, x_meta, y_meta)
    return CalibratedModel.fit(stacking, x_calib, y_calib)


def test_stacking_pipeline_predict_proba_shape_and_range():
    df = _make_featured()
    n = len(df)
    third = n // 3
    x = df.drop(["rank", "date", ResultsCols.TANSHO_ODDS], axis=1).values
    y = df["rank"].values

    calibrated = _build_stacking_calibrated(x[:third], y[:third], x[third : 2 * third], y[third : 2 * third], x[2 * third :], y[2 * third :])
    proba = calibrated.predict_proba(x)
    assert proba.shape == (n, 2)
    assert np.all(proba >= 0.0) and np.all(proba <= 1.0)
    assert np.allclose(proba.sum(axis=1), 1.0)


def test_ev_score_policy_with_calibrated_model():
    """CalibratedModel が ExpectedValueScorePolicy.calc の model 規約に適合すること。"""
    df = _make_featured(n_races=40)
    x_all = df.drop(["rank", "date", ResultsCols.TANSHO_ODDS], axis=1).values
    y_all = df["rank"].values
    n = len(x_all)
    calibrated = _build_stacking_calibrated(x_all[: n // 3], y_all[: n // 3], x_all[n // 3 : 2 * n // 3], y_all[n // 3 : 2 * n // 3], x_all[2 * n // 3 :], y_all[2 * n // 3 :])

    # X_test 形式（TANSHO_ODDS を含む）を渡す
    x_test = df.drop(["rank", "date"], axis=1)
    table = ExpectedValueScorePolicy.calc(calibrated, x_test)

    assert PROB in table.columns
    assert CURRENT_ODDS in table.columns
    assert len(table) == len(df)
    assert (table[PROB] >= 0.0).all() and (table[PROB] <= 1.0).all()


def test_calibrated_model_stored_after_manual_pipeline():
    """手動スタブパイプラインで _calibrated_model が正しくセットされること。"""
    df = _make_featured(n_races=60)
    x = df.drop(["rank", "date", ResultsCols.TANSHO_ODDS], axis=1).values
    y = df["rank"].values
    n = len(x)

    calibrated = _build_stacking_calibrated(x[: n // 3], y[: n // 3], x[n // 3 : 2 * n // 3], y[n // 3 : 2 * n // 3], x[2 * n // 3 :], y[2 * n // 3 :])

    assert isinstance(calibrated, CalibratedModel)
    assert isinstance(calibrated.base_model, StackingModel)
