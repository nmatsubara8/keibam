"""予想印ロング形式 → (race_id, 馬番) 粒度のコンセンサス特徴への集約（純粋・リーク無し）。

`preparing._yoso_marks` のスクレイプ結果（印ロング）を特徴量化する純粋変換。merger
（preprocessing._data_merger）が使うため preprocessing 層に置く（preparing からは再 export）。
"""

from __future__ import annotations

import pandas as pd

from src.constants._yoso import FREE_GOODS_KBN


def aggregate_consensus(df_long: pd.DataFrame) -> pd.DataFrame:
    """印ロング形式を (race_id, 馬番) ごとのコンセンサス特徴に集約する（リーク無し）。

    予想家の顔ぶれはレースで変動するため、個別予想家列でなく集約量を作る:
    - yoso_n_marks    : 印を付けた予想家数（注目度）
    - yoso_n_honmei   : ◎の数
    - yoso_score_sum  : 印スコア合計（◎5..☆1）
    - yoso_score_mean : 印スコア平均（印を付けた予想家内での評価の高さ）
    """
    cols = ["race_id", "馬番", "yoso_n_marks", "yoso_n_honmei", "yoso_score_sum", "yoso_score_mean"]
    if df_long is None or df_long.empty:
        return pd.DataFrame(columns=cols)

    g = df_long.groupby(["race_id", "馬番"])
    out = g.agg(
        yoso_n_marks=("mark_score", "size"),
        yoso_score_sum=("mark_score", "sum"),
        yoso_score_mean=("mark_score", "mean"),
    )
    out["yoso_n_honmei"] = (
        df_long.assign(_h=(df_long["mark"] == "◎").astype(int))
        .groupby(["race_id", "馬番"])["_h"]
        .sum()
    )
    # 無料予想家のみの印数も併設（プレミアム除外の特徴を後で選べるように。最大スキーマの思想）
    if "goods_kbn" in df_long.columns:
        free = df_long[df_long["goods_kbn"].isin(FREE_GOODS_KBN)]
        out["yoso_n_marks_free"] = (
            free.groupby(["race_id", "馬番"]).size() if not free.empty else 0
        )
        out["yoso_n_marks_free"] = out["yoso_n_marks_free"].fillna(0).astype(int)
        cols = cols + ["yoso_n_marks_free"]
    return out.reset_index()[cols]
