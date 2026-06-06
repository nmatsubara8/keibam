"""シミュレーション結果・モデル評価の可視化（純粋関数）。

各関数は matplotlib.figure.Figure を返し、plt.show() は呼ばない。
I/O・モデル読込に依存しないためスタブデータで単体テストが可能であり、
Streamlit の st.pyplot(fig) および CLI の fig.savefig() 両方から利用できる。
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    import matplotlib.figure


# ---------------------------------------------------------------------------
# 既存関数（後方互換のため残す）
# ---------------------------------------------------------------------------

def plot_single_threshold(df, N_SAMPLES, label=" "):  # noqa: N803
    import matplotlib.pyplot as plt

    print(f"df:{df.head()}")
    plt.figure(dpi=100)
    plt.fill_between(df.index, y1=df["return_rate"] - df["std"], y2=df["return_rate"] + df["std"], alpha=0.3)
    plt.plot(df.index, df["return_rate"], label=label)
    plt.legend()
    plt.grid(True)
    plt.xlabel("threshold")
    plt.ylabel("return_rate")
    plt.show()


# ---------------------------------------------------------------------------
# 新規: 較正プロット
# ---------------------------------------------------------------------------

def _reliability_curve(y_true: np.ndarray, prob: np.ndarray, n_bins: int = 10):
    """確率を n_bins 等幅ビンに分割し、各ビンの平均予測確率と実際の的中率を返す。"""
    bins = np.linspace(0.0, 1.0, n_bins + 1)
    bin_centers, actual_rates = [], []
    for lo, hi in zip(bins[:-1], bins[1:]):
        mask = (prob >= lo) & (prob < hi)
        if mask.sum() == 0:
            continue
        bin_centers.append(float(prob[mask].mean()))
        actual_rates.append(float(y_true[mask].mean()))
    return np.array(bin_centers), np.array(actual_rates)


def plot_calibration(
    y_true: np.ndarray,
    prob_pre: np.ndarray,
    prob_post: np.ndarray,
    n_bins: int = 10,
) -> "matplotlib.figure.Figure":
    """較正前後の信頼性ダイアグラム（Reliability diagram）。

    Parameters
    ----------
    y_true : 実際のラベル（0/1）配列
    prob_pre : 較正前の正例確率（スタッキング出力 raw）
    prob_post : Isotonic 較正後の確率
    n_bins : 等幅ビン数

    Returns
    -------
    matplotlib.figure.Figure
    """
    import matplotlib.pyplot as plt

    y_true = np.asarray(y_true, dtype=float)
    prob_pre = np.clip(np.asarray(prob_pre, dtype=float), 0.0, 1.0)
    prob_post = np.clip(np.asarray(prob_post, dtype=float), 0.0, 1.0)

    fig, ax = plt.subplots(figsize=(6, 5), dpi=100)
    ax.plot([0, 1], [0, 1], "k--", lw=1, label="完全較正")

    cx_pre, cr_pre = _reliability_curve(y_true, prob_pre, n_bins)
    ax.plot(cx_pre, cr_pre, "o-", label="較正前")

    cx_post, cr_post = _reliability_curve(y_true, prob_post, n_bins)
    ax.plot(cx_post, cr_post, "s-", label="較正後 (Isotonic)")

    ax.set_xlabel("予測確率")
    ax.set_ylabel("実際の的中率")
    ax.set_title("較正プロット（Reliability Diagram）")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.legend()
    ax.grid(True, alpha=0.4)
    fig.tight_layout()
    return fig


# ---------------------------------------------------------------------------
# 新規: 閾値スイープ
# ---------------------------------------------------------------------------

def run_ev_threshold_sweep(
    candidates_fn,
    thresholds: list[float],
    simulator_fn,
) -> pd.DataFrame:
    """EV 閾値をスイープして成績指標 DataFrame を返す。

    Parameters
    ----------
    candidates_fn : threshold を受け取り BetCandidate リストを返す callable
    thresholds : スイープする閾値リスト
    simulator_fn : BetCandidate リストを受け取り {return_rate, sharpe_ratio, n_bets, ...} を返す callable

    Returns
    -------
    pd.DataFrame (columns: threshold, return_rate, sharpe_ratio, n_bets)
    """
    rows = []
    for th in thresholds:
        candidates = candidates_fn(th)
        metrics = simulator_fn(candidates)
        rows.append(
            {
                "threshold": th,
                "return_rate": metrics.get("return_rate", float("nan")),
                "sharpe_ratio": metrics.get("sharpe_ratio", float("nan")),
                "n_bets": metrics.get("n_bets", 0),
            }
        )
    return pd.DataFrame(rows)


def plot_ev_threshold_sweep(
    sweep_df: pd.DataFrame,
    *,
    optimal_threshold: float | None = None,
) -> "matplotlib.figure.Figure":
    """EV 閾値スイープ結果の可視化。

    Parameters
    ----------
    sweep_df : columns [threshold, return_rate, sharpe_ratio, n_bets] の DataFrame
    optimal_threshold : 縦線で示す最適閾値（None で非表示）

    Returns
    -------
    matplotlib.figure.Figure
    """
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(2, 1, figsize=(7, 7), dpi=100, sharex=True)

    ax1 = axes[0]
    ax1.plot(sweep_df["threshold"], sweep_df["return_rate"], "o-", label="回収率", color="steelblue")
    ax1_r = ax1.twinx()
    ax1_r.plot(sweep_df["threshold"], sweep_df["sharpe_ratio"], "s--", label="シャープ比", color="darkorange")
    ax1.axhline(1.0, color="gray", lw=0.8, ls=":")
    ax1.set_ylabel("回収率")
    ax1_r.set_ylabel("シャープ比")
    ax1.set_title("EV 閾値スイープ")
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax1_r.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, fontsize=8)
    ax1.grid(True, alpha=0.4)

    ax2 = axes[1]
    ax2.bar(sweep_df["threshold"], sweep_df["n_bets"], width=0.02, alpha=0.7, color="steelblue")
    ax2.set_xlabel("EV 閾値")
    ax2.set_ylabel("賭け回数")
    ax2.grid(True, alpha=0.4)

    if optimal_threshold is not None:
        for ax in axes:
            ax.axvline(optimal_threshold, color="red", lw=1.2, ls="--", label=f"最適閾値 {optimal_threshold:.2f}")

    fig.tight_layout()
    return fig


def best_ev_threshold(sweep_df: pd.DataFrame, min_bets: int = 5) -> float:
    """スイープ結果から回収率最大の閾値を返す。min_bets 未満の行は除外。"""
    df = sweep_df[sweep_df["n_bets"] >= min_bets]
    if df.empty:
        return float(sweep_df["threshold"].iloc[0])
    return float(df.loc[df["return_rate"].idxmax(), "threshold"])


# ---------------------------------------------------------------------------
# 新規: オッズ予測精度プロット
# ---------------------------------------------------------------------------

def plot_odds_prediction_accuracy(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    *,
    max_display_odds: float = 50.0,
    title: str = "オッズ予測精度（予測 vs 確定）",
) -> "matplotlib.figure.Figure":
    """予測確定オッズ vs 実確定オッズの散布図。

    Parameters
    ----------
    y_true : 実確定オッズ配列
    y_pred : 予測確定オッズ配列（LgbOddsPredictor 出力等）
    max_display_odds : 表示上限（外れ値を除外）
    title : 図タイトル

    Returns
    -------
    matplotlib.figure.Figure
    """
    import matplotlib.pyplot as plt

    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    mask = (y_true > 0) & (y_pred > 0) & (y_true <= max_display_odds) & (y_pred <= max_display_odds)
    yt, yp = y_true[mask], y_pred[mask]

    rmse = float(np.sqrt(np.mean((yt - yp) ** 2))) if len(yt) > 0 else float("nan")
    mae = float(np.mean(np.abs(yt - yp))) if len(yt) > 0 else float("nan")

    fig, axes = plt.subplots(1, 2, figsize=(10, 4), dpi=100)

    ax1 = axes[0]
    ax1.scatter(yt, yp, alpha=0.3, s=10)
    lim = max(max_display_odds * 0.1, yt.max() if len(yt) else 1.0)
    lim = min(lim, max_display_odds)
    ax1.plot([0, lim], [0, lim], "r--", lw=1)
    ax1.set_xlabel("実確定オッズ")
    ax1.set_ylabel("予測オッズ")
    ax1.set_title(f"{title}\nRMSE={rmse:.2f}  MAE={mae:.2f}")
    ax1.grid(True, alpha=0.4)

    ax2 = axes[1]
    residuals = yp - yt
    ax2.hist(residuals, bins=40, color="steelblue", alpha=0.7)
    ax2.axvline(0, color="red", lw=1.2, ls="--")
    ax2.set_xlabel("残差（予測 − 実）")
    ax2.set_ylabel("度数")
    ax2.set_title("残差分布")
    ax2.grid(True, alpha=0.4)

    fig.tight_layout()
    return fig


# ---------------------------------------------------------------------------
# 新規: スタッキング寄与プロット
# ---------------------------------------------------------------------------

def plot_stacking_contribution(
    y_true: np.ndarray,
    base_probs: list[np.ndarray],
    meta_probs: np.ndarray,
    base_names: list[str] | None = None,
) -> "matplotlib.figure.Figure":
    """各 base モデルと meta スタッキングの AUC を棒グラフで比較。

    Parameters
    ----------
    y_true : 実ラベル（0/1）配列
    base_probs : 各 base モデルの正例確率リスト（StackingModel.base_predictions の出力）
    meta_probs : スタッキング出力の正例確率（CalibratedModel.predict_proba(X)[:, 1]）
    base_names : base モデル名リスト（None で "Base 1", "Base 2", …）

    Returns
    -------
    matplotlib.figure.Figure
    """
    import matplotlib.pyplot as plt
    from sklearn.metrics import roc_auc_score

    y_true = np.asarray(y_true, dtype=float)
    if base_names is None:
        base_names = [f"Base {i + 1}" for i in range(len(base_probs))]

    names, aucs = [], []
    for name, prob in zip(base_names, base_probs):
        p = np.asarray(prob, dtype=float)
        try:
            auc = float(roc_auc_score(y_true, p))
        except Exception:
            auc = float("nan")
        names.append(name)
        aucs.append(auc)

    meta_p = np.asarray(meta_probs, dtype=float)
    try:
        meta_auc = float(roc_auc_score(y_true, meta_p))
    except Exception:
        meta_auc = float("nan")
    names.append("Stacking (meta)")
    aucs.append(meta_auc)

    colors = ["steelblue"] * len(base_probs) + ["darkorange"]
    fig, ax = plt.subplots(figsize=(max(5, len(names) * 1.2), 4), dpi=100)
    bars = ax.bar(names, aucs, color=colors, alpha=0.8)
    ax.set_ylim(max(0.4, min(aucs) - 0.05) if aucs else 0.4, 1.0)
    ax.set_ylabel("AUC")
    ax.set_title("スタッキング寄与（AUC 比較）")
    ax.axhline(0.5, color="gray", lw=0.8, ls=":")
    for bar, auc in zip(bars, aucs):
        if not np.isnan(auc):
            ax.text(bar.get_x() + bar.get_width() / 2, auc + 0.002, f"{auc:.3f}", ha="center", va="bottom", fontsize=9)
    ax.grid(True, axis="y", alpha=0.4)
    fig.tight_layout()
    return fig
