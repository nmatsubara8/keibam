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

# training/_data_splitter.py の _DROP_FOR_TRAIN と一致させる
_DROP_FOR_TRAIN = ["rank", "date", "horse_id", ResultsCols.TANSHO_ODDS, ResultsCols.RANK]


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

    X_calib = calib.drop(_DROP_FOR_TRAIN, axis=1, errors="ignore")
    y_calib = calib["rank"]

    # X_test: TANSHO_ODDS は残す（EV 計算で使う）、horse_id と RANK は除外
    X_test = test.drop(["rank", "date", "horse_id", ResultsCols.RANK], axis=1, errors="ignore")
    y_test = test["rank"]

    # モデル入力用: _DROP_FOR_TRAIN を全部除いた X_test（学習時と同一列構成）
    X_test_model = test.drop(_DROP_FOR_TRAIN, axis=1, errors="ignore")

    return {
        "X_calib": X_calib,
        "y_calib": y_calib,
        "X_test": X_test,
        "X_test_model": X_test_model,
        "y_test": y_test,
        # 賭け明細用: 実着順（着順）。X_test からは除外しているため別途保持する。
        "rank_actual": test[ResultsCols.RANK] if ResultsCols.RANK in test.columns else None,
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
    X_calib = splits["X_calib"]  # DataFrame のまま渡す（NN ストリームは列名で抽出）
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
    # stream-aware StackingModel は NN ストリーム導出に列名（DataFrame）を要求するため
    # .values ではなく DataFrame をそのまま渡す（gbdt 側は内部で .values 化される）。
    X_test_model = splits["X_test_model"]
    y_test = np.asarray(splits["y_test"]).astype(float)

    try:
        base_probs = stacking.base_predictions(X_test_model)
        meta_probs = np.asarray(eff.predict_proba(X_test_model))[:, 1]
    except Exception:
        return None

    ai_base_names = getattr(model, "base_model_names_", None)
    if ai_base_names and len(ai_base_names) == len(base_probs):
        base_names = list(ai_base_names)
    else:
        base_names = []
        for m in stacking._base_models:
            name = type(m).__name__
            if "LGBM" in name or "LightGBM" in name:
                name = "LightGBM"
            elif "NN" in name or "Nn" in name:
                name = "NN"
            elif "XGB" in name or "XGBoost" in name:
                name = "XGBoost"
            elif "CatBoost" in name:
                name = "CatBoost"
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


# ---------------------------------------------------------------------------
# フルシミュレーション（AI 推奨通りに購入した場合の通算成績）
# ---------------------------------------------------------------------------

def _load_return_processor():
    """ReturnProcessor を pkl → DB フォールバックで読み込む。失敗時は None。"""
    try:
        from src.preprocessing._return_processor import ReturnProcessor
        return ReturnProcessor(LocalPaths.RAW_RETURN_TABLES_PATH)
    except Exception:
        return None


def _fukusho_payout(rp, race_id, umaban: int) -> float:
    """指定レース・馬番の複勝払戻を返す。的中なし or データなしは 0.0。"""
    try:
        from src.constants._bet_types import BetType
        table = rp.preprocessed_data[BetType.FUKUSHO]
        if race_id not in table.index:
            return 0.0
        row = table.loc[race_id]
        # win_X は文字列のまま（ReturnProcessor で win_transform=None）
        # 複数行ある場合は Series（loc で複数行マッチ）→ 最初の行を使う
        if isinstance(row, pd.DataFrame):
            row = row.iloc[0]
        n_cols = sum(1 for c in row.index if c.startswith("win_"))
        for i in range(n_cols):
            win_val = row.get(f"win_{i}", 0)
            if win_val == 0:
                continue
            s = str(win_val).strip()
            if " " in s:
                s = s.split()[0]
            try:
                if int(s) == int(umaban):
                    return float(row.get(f"return_{i}", 0)) / 100.0
            except (ValueError, TypeError):
                continue
    except Exception:
        pass
    return 0.0


def _build_return_table_df(rp, race_id) -> pd.DataFrame:
    """指定レースの全馬券種払戻を整理した DataFrame を返す。"""
    try:
        from src.constants._bet_types import BetType
        from src.preprocessing._return_processor import _LABEL
        rows = []
        for bet_type, label in _LABEL.items():
            table = rp.preprocessed_data[bet_type]
            if race_id not in table.index:
                continue
            t = table.loc[race_id]
            if isinstance(t, pd.DataFrame):
                t = t.iloc[0]
            n_cols = sum(1 for c in t.index if c.startswith("win_"))
            for i in range(n_cols):
                win_val = t.get(f"win_{i}", 0)
                ret_val = t.get(f"return_{i}", 0)
                if win_val == 0 or ret_val == 0:
                    continue
                rows.append({"馬券種": label, "的中": str(win_val), "払戻(円)": int(ret_val)})
        return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["馬券種", "的中", "払戻(円)"])
    except Exception:
        return pd.DataFrame(columns=["馬券種", "的中", "払戻(円)"])


def compute_full_backtest(
    model,
    featured_data: pd.DataFrame,
    ev_threshold: float = 1.5,
    test_size: float = 0.2,
    valid_size: float = 0.2,
) -> dict:
    """テストセット全レースで EV 閾値を超えた馬に単勝を賭けたときの通算成績。

    Returns
    -------
    {
      "summary": dict,          # return_rate, profit, hit_rate, n_bets, n_races,
                                #   sharpe_ratio, max_drawdown, total_bet_amount
      "per_race": pd.DataFrame, # race_id, n_bets, bet_amount, return_amount,
                                #   hit_or_not, cumulative_profit
      "per_bet": pd.DataFrame,  # 掛け目明細（馬番・単勝・複勝・EV・着順 etc.）
      "return_processor": ReturnProcessor | None,  # 払戻テーブル参照用
    }
    """
    splits = _get_splits(featured_data, test_size, valid_size)
    X_test = splits["X_test"]
    X_test_model = splits["X_test_model"]
    y_test = np.asarray(splits["y_test"])

    try:
        eff = getattr(model, "effective_model", model)
        prob_win = np.asarray(eff.predict_proba(X_test_model.values))[:, 1]
    except Exception:
        return {"summary": {}, "per_race": pd.DataFrame(), "per_bet": pd.DataFrame(), "return_processor": None}

    odds_arr = np.asarray(X_test[ResultsCols.TANSHO_ODDS], dtype=float)
    ev_arr = prob_win * odds_arr
    wins = (y_test == 1).astype(float)

    # race_id はインデックスに格納されている
    race_ids = X_test.index.to_numpy()

    # 掛け目・実着順の明細用（無い場合は None 埋め）
    umaban_arr = (
        np.asarray(X_test[ResultsCols.UMABAN])
        if ResultsCols.UMABAN in X_test.columns
        else np.full(len(race_ids), None)
    )
    rank_actual = splits.get("rank_actual")
    rank_arr = (
        np.asarray(rank_actual) if rank_actual is not None else np.full(len(race_ids), None)
    )

    # 複勝払戻のために ReturnProcessor を遅延ロード
    rp = _load_return_processor()

    per_race_dict: dict = {}
    per_bet_rows: list = []
    for race_id, umaban, prob, ev, odds, win, actual_rank in zip(
        race_ids, umaban_arr, prob_win, ev_arr, odds_arr, wins, rank_arr, strict=False
    ):
        if ev <= ev_threshold:
            continue
        payout = float(odds * win)
        fukusho_ret = _fukusho_payout(rp, race_id, umaban) if rp is not None else None
        if race_id not in per_race_dict:
            per_race_dict[race_id] = {"n_bets": 0, "bet_amount": 0.0, "return_amount": 0.0}
        per_race_dict[race_id]["n_bets"] += 1
        per_race_dict[race_id]["bet_amount"] += 1.0
        per_race_dict[race_id]["return_amount"] += payout
        row: dict = {
            "race_id": race_id,
            "馬番": umaban,
            "予測勝率": float(prob),
            "単勝オッズ": float(odds),
            "EV": float(ev),
            "着順": actual_rank,
            "的中": int(win),
            "払戻": payout,
            "損益": payout - 1.0,
        }
        if fukusho_ret is not None:
            row["複勝払戻"] = fukusho_ret
            row["複勝的中"] = int(fukusho_ret > 0)
        per_bet_rows.append(row)

    if not per_race_dict:
        return {"summary": {}, "per_race": pd.DataFrame(), "per_bet": pd.DataFrame(), "return_processor": rp}

    per_race_df = pd.DataFrame.from_dict(per_race_dict, orient="index")
    per_race_df.index.name = "race_id"
    per_race_df["hit_or_not"] = (per_race_df["return_amount"] > 0).astype(int)
    per_race_df["profit"] = per_race_df["return_amount"] - per_race_df["bet_amount"]
    per_race_df["cumulative_profit"] = per_race_df["profit"].cumsum()
    per_race_df = per_race_df.reset_index()

    from src.simulation._metrics import summarize_returns
    summary = summarize_returns(per_race_df.set_index("race_id"))

    per_bet_df = pd.DataFrame(per_bet_rows)

    return {"summary": summary, "per_race": per_race_df, "per_bet": per_bet_df, "return_processor": rp}


# ---------------------------------------------------------------------------
# 確信度（EV 閾値）スイープ（hit_rate / profit / max_drawdown も追加）
# ---------------------------------------------------------------------------

def compute_confidence_sweep(
    model,
    featured_data: pd.DataFrame,
    thresholds: list[float] | None = None,
    test_size: float = 0.2,
    valid_size: float = 0.2,
) -> pd.DataFrame:
    """単勝 EV 閾値スイープ。return_rate / hit_rate / profit / max_drawdown / sharpe / n_bets を返す。

    Returns
    -------
    pd.DataFrame (columns: threshold, return_rate, hit_rate, profit,
                            max_drawdown, sharpe_ratio, n_bets)
    """
    if thresholds is None:
        thresholds = [round(t, 2) for t in np.arange(1.0, 2.6, 0.1)]

    splits = _get_splits(featured_data, test_size, valid_size)
    X_test = splits["X_test"]
    X_test_model = splits["X_test_model"]
    y_test = np.asarray(splits["y_test"])

    try:
        eff = getattr(model, "effective_model", model)
        prob_win = np.asarray(eff.predict_proba(X_test_model.values))[:, 1]
    except Exception:
        return pd.DataFrame(
            columns=["threshold", "return_rate", "hit_rate", "profit",
                     "max_drawdown", "sharpe_ratio", "n_bets"]
        )

    odds_arr = np.asarray(X_test[ResultsCols.TANSHO_ODDS], dtype=float)
    ev_arr = prob_win * odds_arr
    wins = (y_test == 1).astype(float)
    race_ids = X_test.index.to_numpy()

    rows = []
    for th in thresholds:
        mask = ev_arr > th
        n = int(mask.sum())
        if n == 0:
            rows.append({
                "threshold": th, "return_rate": float("nan"), "hit_rate": float("nan"),
                "profit": float("nan"), "max_drawdown": float("nan"),
                "sharpe_ratio": float("nan"), "n_bets": 0,
            })
            continue

        payouts = odds_arr[mask] * wins[mask]
        rr = float(payouts.sum()) / n
        pnl = payouts - 1.0
        profit = float(pnl.sum())
        std = float(pnl.std()) if n > 1 else 0.0
        sharpe = (rr - 1.0) / std if std > 0 else 0.0

        # hit_rate: レース単位（同一 race_id で 1 頭でも的中したらヒット）
        masked_race_ids = race_ids[mask]
        masked_wins = wins[mask]
        race_hit: dict = {}
        for rid, w in zip(masked_race_ids, masked_wins, strict=False):
            race_hit[rid] = race_hit.get(rid, 0) + w
        n_races = len(race_hit)
        n_hits = sum(1 for v in race_hit.values() if v > 0)
        hit_rate = n_hits / n_races if n_races else 0.0

        # max_drawdown: 賭け単位の損益系列から
        from src.simulation._metrics import max_drawdown as _max_dd
        pnl_series = pd.Series(pnl)
        md = _max_dd(pnl_series)

        rows.append({
            "threshold": th, "return_rate": rr, "hit_rate": hit_rate,
            "profit": profit, "max_drawdown": md, "sharpe_ratio": sharpe, "n_bets": n,
        })

    return pd.DataFrame(rows)
