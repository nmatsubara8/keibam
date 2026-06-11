"""オッズ力学モデルの照会・比較ヘルパ（Streamlit 非依存の計算ロジック）。

- 照会マトリクス: レースごとに「各時点の実績シェア/オッズ」と「その時点で出した
  次時点・確定の予測」を馬×時点の表に整形する（odds_monitor 用）
- 評価比較表: models/odds_dynamics_eval.json の指標をモデル別に整形（model_lab 用）
"""

from __future__ import annotations

import pandas as pd

from src.constants._odds_phases import PHASE_TIMELINE

# 表示用のフェーズ名
PHASE_LABELS = {
    "prev_day": "前日",
    "hours_before": "数時間前",
    "thirty_min": "T-30",
    "t10": "T-10",
    "t5": "T-5",
    "t0": "直前",
}


def inquiry_matrix(predictions: pd.DataFrame, race_id: str, model: str = "ensemble") -> pd.DataFrame:
    """1 レースの照会マトリクス（馬×時点）を作る。

    各チェックポイント列に「実績オッズ」、その右に「次時点予測シェア」
    「確定予測オッズ」を並べる。predictions は odds_watch が保存したテーブル。
    """
    if predictions is None or predictions.empty:
        return pd.DataFrame()
    sel = predictions[
        (predictions["race_id"].astype(str) == str(race_id))
        & (predictions["model"] == model)
    ]
    if sel.empty:
        return pd.DataFrame()

    frames = []
    for phase in PHASE_TIMELINE:
        chk = sel[sel["checkpoint"] == phase]
        if chk.empty:
            continue
        chk = chk.sort_values("predicted_at").groupby("umaban").tail(1).set_index("umaban")
        label = PHASE_LABELS.get(phase, phase)
        frames.append(
            pd.DataFrame(
                {
                    f"実績オッズ@{label}": chk["actual_odds"],
                    f"次時点予測シェア@{label}": chk["pred_next_share"],
                    f"確定予測オッズ@{label}": chk["pred_final_odds"],
                }
            )
        )
    if not frames:
        return pd.DataFrame()
    matrix = pd.concat(frames, axis=1)
    matrix.index.name = "馬番"
    return matrix.sort_index(key=lambda idx: idx.astype(int))


def available_races(predictions: pd.DataFrame) -> list[str]:
    """予測テーブルに存在するレース（新しい順）。"""
    if predictions is None or predictions.empty:
        return []
    order = predictions.sort_values("predicted_at", ascending=False)["race_id"].astype(str)
    return list(dict.fromkeys(order))


def available_models(predictions: pd.DataFrame) -> list[str]:
    if predictions is None or predictions.empty:
        return []
    models = predictions["model"].unique().tolist()
    # ensemble を先頭に
    return sorted(models, key=lambda m: (m != "ensemble", m))


def eval_comparison_table(records: list[dict]) -> pd.DataFrame:
    """評価 JSON（最新評価分）をモデル別比較表に整形する（KL 昇順）。"""
    if not records:
        return pd.DataFrame()
    latest_ts = max(r["evaluated_at"] for r in records)
    rows = [r for r in records if r["evaluated_at"] == latest_ts]
    df = pd.DataFrame(rows).set_index("model")
    cols = [c for c in ("kl_mean", "winner_logloss", "share_mae", "odds_mape",
                        "ensemble_weight", "n_test_races", "n_train_races") if c in df.columns]
    return df[cols].sort_values("kl_mean")
