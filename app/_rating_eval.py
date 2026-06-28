"""レーティング効果検証の純粋ヘルパ（UI 向け・再学習不要のスタンドアロン照会）。

featured_data（elo_* 列を含む）から、Elo の素の説明力を「目視で確認しやすい」指標に
落とす。モデル再学習なしで効果を即照会できる（On/Off A/B の前段）。I/O 非依存・純粋。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.constants._results_cols import ResultsCols

RANK = ResultsCols.RANK            # 着順
TANSHO = ResultsCols.TANSHO_ODDS   # 単勝


def has_ratings(df: pd.DataFrame) -> bool:
    """featured_data に Elo 列が含まれるか（未リビルドの検出用）。"""
    return df is not None and "elo_rating" in df.columns


def standalone_calibration(
    df: pd.DataFrame, prob_col: str = "elo_win_prob", win_col: str = "rank_win", n_bins: int = 10
) -> pd.DataFrame:
    """Elo 強度比勝率（prob_col）の信頼度曲線。予測勝率ビンごとの実勝率を返す。

    columns = [bin_mid, mean_pred, mean_actual, count]。再学習不要の較正照会。
    """
    if df is None or prob_col not in df.columns or win_col not in df.columns:
        return pd.DataFrame(columns=["bin_mid", "mean_pred", "mean_actual", "count"])
    d = df[[prob_col, win_col]].dropna()
    if d.empty:
        return pd.DataFrame(columns=["bin_mid", "mean_pred", "mean_actual", "count"])
    edges = np.linspace(0.0, max(float(d[prob_col].max()), 1e-6), n_bins + 1)
    d = d.assign(_bin=pd.cut(d[prob_col], bins=edges, include_lowest=True))
    g = d.groupby("_bin", observed=True)
    out = pd.DataFrame({
        "mean_pred": g[prob_col].mean(),
        "mean_actual": g[win_col].mean(),
        "count": g[win_col].size(),
    }).reset_index(drop=True)
    out["bin_mid"] = [(edges[i] + edges[i + 1]) / 2 for i in range(len(out))]
    return out[["bin_mid", "mean_pred", "mean_actual", "count"]]


def rank_correlation(df: pd.DataFrame, rating_col: str = "elo_rating", finish_col: str = RANK) -> float:
    """レーティングと実着順の Spearman 相関。負＝高レーティングほど好走（=説明力）。"""
    if df is None or rating_col not in df.columns or finish_col not in df.columns:
        return float("nan")
    d = df[[rating_col, finish_col]].apply(pd.to_numeric, errors="coerce").dropna()
    if len(d) < 3:
        return float("nan")
    return float(d[rating_col].corr(d[finish_col], method="spearman"))


def _per_race_top_hits(df: pd.DataFrame, pick_col: str, ascending: bool, win_col: str = "rank_win") -> tuple[int, int]:
    """各レースで pick_col の最上位馬が1着だった回数と対象レース数を返す。"""
    if df is None or pick_col not in df.columns or win_col not in df.columns:
        return 0, 0
    rid = df.index.name or "race_id"
    d = df.reset_index()
    rid_col = rid if rid in d.columns else d.columns[0]
    d = d[[rid_col, pick_col, win_col]].copy()
    d[pick_col] = pd.to_numeric(d[pick_col], errors="coerce")
    d = d.dropna(subset=[pick_col])
    hit = total = 0
    for _, g in d.groupby(rid_col):
        if g.empty:
            continue
        idx = g[pick_col].idxmin() if ascending else g[pick_col].idxmax()
        total += 1
        if float(g.loc[idx, win_col]) == 1.0:
            hit += 1
    return hit, total


def top_pick_hit_rates(df: pd.DataFrame) -> dict:
    """本命的中率の照会: Elo 最上位馬 vs 市場本命（単勝最小）の1着的中率。"""
    elo_hit, n_races = _per_race_top_hits(df, "elo_rating", ascending=False)
    fav_hit, n_fav = _per_race_top_hits(df, TANSHO, ascending=True)
    return {
        "elo_hit": elo_hit,
        "elo_rate": (elo_hit / n_races) if n_races else float("nan"),
        "fav_hit": fav_hit,
        "fav_rate": (fav_hit / n_fav) if n_fav else float("nan"),
        "n_races": n_races,
    }


def snapshot_ranking(snapshot: dict, top: int = 30, min_races: int = 1) -> pd.DataFrame:
    """最新スナップショット（horse_id→{rating,n_races}）の上位馬テーブル。"""
    rows = [
        {"horse_id": h, "rating": v.get("rating"), "n_races": v.get("n_races")}
        for h, v in (snapshot or {}).items()
        if int(v.get("n_races", 0)) >= min_races
    ]
    out = pd.DataFrame(rows, columns=["horse_id", "rating", "n_races"])
    if out.empty:
        return out
    return out.sort_values("rating", ascending=False).head(top).reset_index(drop=True)
