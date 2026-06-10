"""モデル評価データの算出（UI 向け純粋ヘルパ）。

featured_data とモデルから、較正プロット・スタッキング寄与・EV 閾値スイープに
必要なデータを算出する関数群。I/O を持たず、Streamlit の @st.cache_data 下で
使うことを前提としている。

DataSplitter で学習時と同じ時系列分割を再現し、テスト・較正セットの
予測を取得する。重い依存（optuna）はメソッド内で lazy import する。
"""

from __future__ import annotations

import os
import pickle

import numpy as np
import pandas as pd

from src.constants._local_paths import LocalPaths
from src.constants._results_cols import ResultsCols

_DROP_FOR_TRAIN = ["rank", "date", ResultsCols.TANSHO_ODDS]


# ---------------------------------------------------------------------------
# データ読込
# ---------------------------------------------------------------------------

def load_featured_data(path: str = LocalPaths.FEATURED_DATA_PATH) -> pd.DataFrame | None:
    """featured_data pickle を読み込む（ファイルがなければ None）。"""
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return pickle.load(f)


# ---------------------------------------------------------------------------
# 分割ヘルパ（DataSplitter の時系列分割を再現）
# ---------------------------------------------------------------------------

def _split_by_date(df: pd.DataFrame, test_size: float):
    sorted_ids = df.sort_values("date").index.unique()
    n = round(len(sorted_ids) * (1 - test_size))
    train = df.loc[sorted_ids[:n]]
    test = df.loc[sorted_ids[n:]]
    return train, test


def _get_splits(
    featured_data: pd.DataFrame,
    test_size: float = 0.2,
    valid_size: float = 0.2,
    meta_ratio: float = 0.3,
) -> dict:
    """学習時と同じ時系列分割を再現し、各セクションの X / y を返す。"""
    train, test = _split_by_date(featured_data, test_size)
    train_opt, calib = _split_by_date(train, valid_size)

    X_calib = calib.drop(_DROP_FOR_TRAIN, axis=1)
    y_calib = calib["rank"]

    # X_test: TANSHO_ODDS は残す（EV 計算で使う）
    X_test = test.drop(["rank", "date"], axis=1)
    y_test = test["rank"]

    # モデル入力用: TANSHO_ODDS を除いた X_test
    X_test_model = test.drop(_DROP_FOR_TRAIN, axis=1)

    return {
        "X_calib": X_calib,
        "y_calib": y_calib,
        "X_test": X_test,
        "X_test_model": X_test_model,
        "y_test": y_test,
    }


# ---------------------------------------------------------------------------
# 較正曲線データ
# ---------------------------------------------------------------------------

def compute_calib_curves(
    model,
    featured_data: pd.DataFrame,
    test_size: float = 0.2,
    valid_size: float = 0.2,
) -> dict | None:
    """較正前後の確率配列と真ラベルを返す。

    model が CalibratedModel（_base_model + predict_proba）を持たない場合は None。

    Returns
    -------
    {"y_true": ndarray, "prob_pre": ndarray, "prob_post": ndarray}
    """
    # _calibrated_model 属性を持つ（KeibaAI）か、自身が較正モデルか
    eff = getattr(model, "_calibrated_model", None)
    if eff is None:
        eff = model
    # duck typing: _base_model と predict_proba を持つことを確認
    if not (hasattr(eff, "_base_model") and hasattr(eff, "predict_proba")):
        return None

    splits = _get_splits(featured_data, test_size, valid_size)
    X_calib = splits["X_calib"].values
    y_calib = np.asarray(splits["y_calib"]).astype(float)

    try:
        prob_pre = np.asarray(eff._base_model.predict_proba(X_calib))[:, 1]
        prob_post = np.asarray(eff.predict_proba(X_calib))[:, 1]
    except Exception:
        return None

    return {"y_true": y_calib, "prob_pre": prob_pre, "prob_post": prob_post}


# ---------------------------------------------------------------------------
# スタッキング寄与データ
# ---------------------------------------------------------------------------

def compute_stacking_auc(
    model,
    featured_data: pd.DataFrame,
    test_size: float = 0.2,
    valid_size: float = 0.2,
) -> dict | None:
    """各 base モデルと meta モデルのテストセット AUC を返す。

    Returns
    -------
    {"y_true": ndarray, "base_probs": list[ndarray], "meta_probs": ndarray, "base_names": list[str]}
    """
    eff = getattr(model, "_calibrated_model", None)
    if eff is None:
        eff = model
    # duck typing: _base_model（スタッキング）と predict_proba を持つこと
    if not hasattr(eff, "_base_model"):
        return None
    stacking = eff._base_model
    # duck typing: base_predictions と _base_models を持つこと
    if not (hasattr(stacking, "base_predictions") and hasattr(stacking, "_base_models")):
        return None

    splits = _get_splits(featured_data, test_size, valid_size)
    X_test_model = splits["X_test_model"].values
    y_test = np.asarray(splits["y_test"]).astype(float)

    try:
        base_probs = stacking.base_predictions(X_test_model)
        meta_probs = np.asarray(eff.predict_proba(X_test_model))[:, 1]
    except Exception:
        return None

    base_names = []
    for m in stacking._base_models:
        name = type(m).__name__
        if "LGBM" in name or "LightGBM" in name:
            name = "LightGBM"
        elif "NN" in name or "Nn" in name:
            name = "NN"
        base_names.append(name)

    return {
        "y_true": y_test,
        "base_probs": base_probs,
        "meta_probs": meta_probs,
        "base_names": base_names,
    }


# ---------------------------------------------------------------------------
# EV 閾値スイープ（単勝バックテスト）
# ---------------------------------------------------------------------------

def compute_ev_sweep(
    model,
    featured_data: pd.DataFrame,
    thresholds: list[float] | None = None,
    test_size: float = 0.2,
    valid_size: float = 0.2,
) -> pd.DataFrame:
    """テストセットで単勝 EV 閾値スイープを実行し結果 DataFrame を返す。

    EV = P(win) × TANSHO_ODDS を各馬について算出し、EV > threshold の賭けのみを選択。
    実確定オッズ（TANSHO_ODDS）と実着順（rank==1）から回収率を計算する。

    Returns
    -------
    pd.DataFrame (columns: threshold, return_rate, sharpe_ratio, n_bets)
    """
    if thresholds is None:
        thresholds = [round(t, 2) for t in np.arange(1.0, 2.1, 0.1)]

    splits = _get_splits(featured_data, test_size, valid_size)
    X_test = splits["X_test"]                 # TANSHO_ODDS あり
    X_test_model = splits["X_test_model"]      # TANSHO_ODDS なし
    y_test = np.asarray(splits["y_test"])

    try:
        prob_win = np.asarray(model.predict_proba(X_test_model.values))[:, 1]
    except Exception:
        try:
            # KeibaAI ラッパー
            eff = getattr(model, "effective_model", model)
            prob_win = np.asarray(eff.predict_proba(X_test_model.values))[:, 1]
        except Exception:
            return pd.DataFrame(columns=["threshold", "return_rate", "sharpe_ratio", "n_bets"])

    odds_arr = np.asarray(X_test[ResultsCols.TANSHO_ODDS], dtype=float)
    ev_arr = prob_win * odds_arr
    wins = (y_test == 1).astype(float)

    rows = []
    for th in thresholds:
        mask = ev_arr > th
        n = int(mask.sum())
        if n == 0:
            rows.append({"threshold": th, "return_rate": float("nan"), "sharpe_ratio": float("nan"), "n_bets": 0})
            continue
        # 回収率: 的中時はオッズ払戻、外れは 0（100 円ベット基準）
        payouts = odds_arr[mask] * wins[mask]
        rr = float(payouts.sum()) / n
        # シャープ: (回収率 - 1) / std。レース単位の損益系列から算出
        pnl = payouts - 1.0
        std = float(pnl.std()) if n > 1 else 0.0
        sharpe = (rr - 1.0) / std if std > 0 else 0.0
        rows.append({"threshold": th, "return_rate": rr, "sharpe_ratio": sharpe, "n_bets": n})

    return pd.DataFrame(rows)
