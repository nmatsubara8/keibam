"""src/simulation/_plot.py の単体テスト。

すべて純粋関数（I/O なし）のため、スタブデータで Figure の型・軸ラベル・
計算結果のみを検証する。matplotlib を使うが plt.show() は呼ばないので
ヘッドレス環境でも動作する。
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import matplotlib
matplotlib.use("Agg")  # GUI なし

from src.simulation._plot import (
    _reliability_curve,
    best_ev_threshold,
    plot_calibration,
    plot_ev_threshold_sweep,
    plot_odds_prediction_accuracy,
    plot_stacking_contribution,
    run_ev_threshold_sweep,
)


# ---------------------------------------------------------------------------
# ユーティリティ
# ---------------------------------------------------------------------------

def _binary_data(n=200, seed=0):
    rng = np.random.default_rng(seed)
    y = rng.integers(0, 2, size=n).astype(float)
    prob_pre = rng.uniform(0.0, 1.0, size=n)
    # 較正後はラベルに近い確率
    prob_post = 0.7 * y + 0.15 + rng.uniform(-0.1, 0.1, size=n)
    prob_post = np.clip(prob_post, 0.01, 0.99)
    return y, prob_pre, prob_post


# ---------------------------------------------------------------------------
# _reliability_curve
# ---------------------------------------------------------------------------

def test_reliability_curve_returns_equal_length():
    y, pre, _ = _binary_data()
    cx, cr = _reliability_curve(y, pre, n_bins=5)
    assert len(cx) == len(cr)


def test_reliability_curve_values_in_range():
    y, pre, _ = _binary_data()
    cx, cr = _reliability_curve(y, pre)
    assert np.all((cx >= 0.0) & (cx <= 1.0))
    assert np.all((cr >= 0.0) & (cr <= 1.0))


# ---------------------------------------------------------------------------
# plot_calibration
# ---------------------------------------------------------------------------

def test_plot_calibration_returns_figure():
    import matplotlib.pyplot as plt
    y, pre, post = _binary_data()
    fig = plot_calibration(y, pre, post)
    assert isinstance(fig, plt.Figure)
    plt.close(fig)


def test_plot_calibration_has_three_lines():
    import matplotlib.pyplot as plt
    y, pre, post = _binary_data()
    fig = plot_calibration(y, pre, post)
    ax = fig.axes[0]
    assert len(ax.lines) >= 3  # 対角線 + 較正前 + 較正後
    plt.close(fig)


def test_plot_calibration_axis_labels():
    import matplotlib.pyplot as plt
    y, pre, post = _binary_data()
    fig = plot_calibration(y, pre, post)
    ax = fig.axes[0]
    assert "確率" in ax.get_xlabel() or "prob" in ax.get_xlabel().lower()
    plt.close(fig)


# ---------------------------------------------------------------------------
# plot_ev_threshold_sweep
# ---------------------------------------------------------------------------

def _sweep_df():
    thresholds = np.arange(1.0, 1.5, 0.1)
    return pd.DataFrame({
        "threshold": thresholds,
        "return_rate": 0.9 + thresholds * 0.1,
        "sharpe_ratio": (0.9 + thresholds * 0.1 - 1.0) / 0.2,
        "n_bets": (100 - thresholds * 30).astype(int),
    })


def test_plot_ev_threshold_sweep_returns_figure():
    import matplotlib.pyplot as plt
    fig = plot_ev_threshold_sweep(_sweep_df())
    assert isinstance(fig, plt.Figure)
    plt.close(fig)


def test_plot_ev_threshold_sweep_two_axes():
    import matplotlib.pyplot as plt
    fig = plot_ev_threshold_sweep(_sweep_df())
    # 2 サブプロット + twinx で ax 数は 3
    assert len(fig.axes) >= 2
    plt.close(fig)


def test_plot_ev_threshold_sweep_with_optimal():
    import matplotlib.pyplot as plt
    fig = plot_ev_threshold_sweep(_sweep_df(), optimal_threshold=1.2)
    assert isinstance(fig, plt.Figure)
    plt.close(fig)


def test_best_ev_threshold_picks_max_return_rate():
    df = _sweep_df()
    th = best_ev_threshold(df, min_bets=1)
    # return_rate は threshold が大きいほど大きい → 最後尾
    assert th == pytest.approx(df["threshold"].iloc[-1], abs=1e-6)


def test_best_ev_threshold_respects_min_bets():
    df = _sweep_df()
    # n_bets: 70, 67, 64, 61, 58 → min_bets=65 で 1.0, 1.1 のみ通過
    th = best_ev_threshold(df, min_bets=65)
    filtered = df[df["n_bets"] >= 65]
    expected = float(filtered.loc[filtered["return_rate"].idxmax(), "threshold"])
    assert th == pytest.approx(expected, abs=1e-6)


def test_run_ev_threshold_sweep_shape():
    thresholds = [1.0, 1.1, 1.2]

    def _candidates(th):
        return [] if th > 1.05 else [object()]

    def _simulator(candidates):
        n = len(candidates)
        return {"return_rate": 1.0 + n * 0.1, "sharpe_ratio": n * 0.05, "n_bets": n}

    df = run_ev_threshold_sweep(_candidates, thresholds, _simulator)
    assert list(df.columns) == ["threshold", "return_rate", "sharpe_ratio", "n_bets"]
    assert len(df) == len(thresholds)


# ---------------------------------------------------------------------------
# plot_odds_prediction_accuracy
# ---------------------------------------------------------------------------

def _odds_data(n=100, seed=1):
    rng = np.random.default_rng(seed)
    y_true = rng.uniform(1.5, 30.0, size=n)
    y_pred = y_true * rng.uniform(0.8, 1.2, size=n)
    return y_true, y_pred


def test_plot_odds_prediction_accuracy_returns_figure():
    import matplotlib.pyplot as plt
    y_true, y_pred = _odds_data()
    fig = plot_odds_prediction_accuracy(y_true, y_pred)
    assert isinstance(fig, plt.Figure)
    plt.close(fig)


def test_plot_odds_prediction_accuracy_two_subplots():
    import matplotlib.pyplot as plt
    y_true, y_pred = _odds_data()
    fig = plot_odds_prediction_accuracy(y_true, y_pred)
    assert len(fig.axes) == 2
    plt.close(fig)


def test_plot_odds_prediction_accuracy_empty_safe():
    import matplotlib.pyplot as plt
    fig = plot_odds_prediction_accuracy(np.array([]), np.array([]))
    assert isinstance(fig, plt.Figure)
    plt.close(fig)


def test_plot_odds_prediction_accuracy_rmse_in_title():
    import matplotlib.pyplot as plt
    y_true, y_pred = _odds_data()
    fig = plot_odds_prediction_accuracy(y_true, y_pred)
    title_text = fig.axes[0].get_title()
    assert "RMSE" in title_text
    plt.close(fig)


# ---------------------------------------------------------------------------
# plot_stacking_contribution
# ---------------------------------------------------------------------------

def _stacking_data(n=200, seed=2):
    rng = np.random.default_rng(seed)
    y = rng.integers(0, 2, size=n).astype(float)
    b1 = np.clip(y * 0.6 + rng.uniform(0, 0.4, n), 0, 1)
    b2 = np.clip(y * 0.5 + rng.uniform(0, 0.5, n), 0, 1)
    meta = np.clip((b1 + b2) / 2 + rng.uniform(-0.05, 0.05, n), 0, 1)
    return y, [b1, b2], meta


def test_plot_stacking_contribution_returns_figure():
    import matplotlib.pyplot as plt
    y, bases, meta = _stacking_data()
    fig = plot_stacking_contribution(y, bases, meta)
    assert isinstance(fig, plt.Figure)
    plt.close(fig)


def test_plot_stacking_contribution_bar_count():
    import matplotlib.pyplot as plt
    y, bases, meta = _stacking_data()
    fig = plot_stacking_contribution(y, bases, meta)
    ax = fig.axes[0]
    # base x2 + meta x1 = 3本
    assert len(ax.patches) == 3
    plt.close(fig)


def test_plot_stacking_contribution_custom_names():
    import matplotlib.pyplot as plt
    y, bases, meta = _stacking_data()
    fig = plot_stacking_contribution(y, bases, meta, base_names=["LightGBM", "NN"])
    ax = fig.axes[0]
    labels = [t.get_text() for t in ax.get_xticklabels()]
    assert "LightGBM" in labels
    assert "NN" in labels
    plt.close(fig)


def test_plot_stacking_contribution_meta_auc_not_nan():
    import matplotlib.pyplot as plt
    y, bases, meta = _stacking_data()
    fig = plot_stacking_contribution(y, bases, meta)
    ax = fig.axes[0]
    # AUC テキストがすべて数値であること
    for text in ax.texts:
        val = text.get_text()
        assert val.replace(".", "").isdigit() or "nan" not in val
    plt.close(fig)
