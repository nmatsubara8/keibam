"""予想印（yoso marks / predictor profile）由来の特徴量を results に結合する関数群。

`DataMerger` の yoso 族メソッドを ``(results, yoso_marks[, yoso_predictor]) -> results`` の
自由関数として切り出したもの（発走前確定＝リーク無し。空入力は results 無変更で返す）。
DataMerger からは薄い委譲メソッド経由で呼ぶ（状態 self._yoso_* は呼び出し側が渡す）。
"""

from __future__ import annotations

import pandas as pd

def merge_yoso_marks(results, yoso_marks):
    """予想印（ロング）を (race_id, 馬番) のコンセンサス特徴に集約して左結合する。

    予想家の顔ぶれはレースで変動するため個別列でなく集約量（印数/◎数/スコア）を使う。
    発走前確定＝リーク無し。未提供（空）はスキップ。
    """
    if yoso_marks is None or yoso_marks.empty:
        return results
    if "馬番" not in results.columns:
        return results
    from src.preprocessing._yoso_consensus import aggregate_consensus

    long = yoso_marks.reset_index()
    if "race_id" not in long.columns:
        long = long.rename(columns={long.columns[0]: "race_id"})
    if "馬番" not in long.columns:
        return results
    long["race_id"] = long["race_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    consensus = aggregate_consensus(long)
    if consensus.empty:
        return results
    consensus["race_id"] = consensus["race_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    consensus["_umaban_key"] = pd.to_numeric(consensus["馬番"], errors="coerce").astype("Int64")
    value_cols = [c for c in consensus.columns if c.startswith("yoso_")]
    consensus = consensus[["race_id", "_umaban_key"] + value_cols].drop_duplicates(
        ["race_id", "_umaban_key"]
    )

    base = results
    base.index = base.index.astype(str).str.replace(r"\.0$", "", regex=True)
    base.index.name = "race_id"
    left = base.reset_index()
    left["_umaban_key"] = pd.to_numeric(left["馬番"], errors="coerce").astype("Int64")
    merged = left.merge(consensus, on=["race_id", "_umaban_key"], how="left")
    return merged.drop(columns=["_umaban_key"]).set_index("race_id")

def add_yoso_predictor_skill(results, yoso_marks):
    """予想家の as-of ◎的中率で◎を加重した特徴を追加する（自前計算・リーク無し）。

    取得済み yoso_marks（予想家の印）と results（着順）から、各予想家の「◎を付けた馬が
    1着になった率」を**当該レース日より前**で expanding 集計（自分の当該行は除外）し、
    ◎を付けた予想家の skill を馬ごとに合算/最大する。スクレイプ不要・追加取得なし。
    - ``yoso_honmei_skill_sum`` : ◎を付けた予想家の as-of 的中率の合計（質×量）
    - ``yoso_best_skill``       : 同・最大（最も当てる予想家が◎を付けたか）
    skill 不明（履歴ゼロの予想家）の寄与は NaN（best）/0（sum・skipna）。
    """
    import numpy as np  # noqa: F401 — where 経由で利用

    if yoso_marks is None or yoso_marks.empty:
        return results
    if "馬番" not in results.columns or "着順" not in results.columns:
        return results
    long = yoso_marks.reset_index()
    if "race_id" not in long.columns:
        long = long.rename(columns={long.columns[0]: "race_id"})
    if not {"馬番", "predictor_yid", "mark"}.issubset(long.columns):
        return results
    long["race_id"] = long["race_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    long["_umaban_key"] = pd.to_numeric(long["馬番"], errors="coerce").astype("Int64")

    base = results
    base.index = base.index.astype(str).str.replace(r"\.0$", "", regex=True)
    base.index.name = "race_id"
    res = base.reset_index()
    res["_umaban_key"] = pd.to_numeric(res["馬番"], errors="coerce").astype("Int64")
    res["_chaku"] = pd.to_numeric(res["着順"], errors="coerce")
    race_date = res.groupby("race_id")["date"].first() if "date" in res.columns else None

    hon = long[long["mark"] == "◎"].copy()
    if hon.empty or race_date is None:
        return results
    hon = hon.merge(
        res[["race_id", "_umaban_key", "_chaku"]], on=["race_id", "_umaban_key"], how="left"
    )
    hon["_date"] = hon["race_id"].map(race_date)
    hon["_hit"] = (hon["_chaku"] == 1).astype(float)
    hon = hon.sort_values(["_date", "race_id"])
    grp = hon.groupby("predictor_yid")
    n_prior = grp.cumcount()                       # 当該行より前の◎数
    hits_prior = grp["_hit"].cumsum() - hon["_hit"]  # 当該行を除く的中数
    hon["_skill"] = hits_prior / n_prior.where(n_prior > 0)  # 履歴ゼロは NaN

    sk = hon.groupby(["race_id", "_umaban_key"]).agg(
        yoso_honmei_skill_sum=("_skill", "sum"),
        yoso_best_skill=("_skill", "max"),
    ).reset_index()

    merged = res.merge(sk, on=["race_id", "_umaban_key"], how="left")
    return merged.drop(
        columns=["_umaban_key", "_chaku"], errors="ignore"
    ).set_index("race_id")

def add_yoso_profile_skill(results, yoso_marks, yoso_predictor):
    """予想家プロフィール由来スキル（prior）で◎を加重した特徴を追加する。

    各予想家の profile_honmei_winrate（◎1着率の直近集計）を、◎を付けた馬ごとに合算/最大。
    方式A（自前 as-of）が直近窓のみなのに対し、こちらは予想家自身のログ由来で広くカバー
    （現時点スナップショット＝軽微リーク許容。ユーザー指定 B1）。未提供（空）はスキップ。
    """
    if yoso_predictor is None or yoso_predictor.empty:
        return results
    if yoso_marks is None or yoso_marks.empty:
        return results
    if "馬番" not in results.columns:
        return results
    prior = yoso_predictor.reset_index()
    if "predictor_yid" not in prior.columns:
        prior = prior.rename(columns={prior.columns[0]: "predictor_yid"})
    if "profile_honmei_winrate" not in prior.columns:
        return results
    prior["predictor_yid"] = prior["predictor_yid"].astype(str)
    skill = prior.set_index("predictor_yid")["profile_honmei_winrate"]

    long = yoso_marks.reset_index()
    if "race_id" not in long.columns:
        long = long.rename(columns={long.columns[0]: "race_id"})
    if not {"馬番", "predictor_yid", "mark"}.issubset(long.columns):
        return results
    hon = long[long["mark"] == "◎"].copy()
    if hon.empty:
        return results
    hon["race_id"] = hon["race_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    hon["_umaban_key"] = pd.to_numeric(hon["馬番"], errors="coerce").astype("Int64")
    hon["_sk"] = hon["predictor_yid"].astype(str).map(skill)
    agg = hon.groupby(["race_id", "_umaban_key"]).agg(
        yoso_profile_skill_sum=("_sk", "sum"),
        yoso_profile_best=("_sk", "max"),
    ).reset_index()

    base = results
    base.index = base.index.astype(str).str.replace(r"\.0$", "", regex=True)
    base.index.name = "race_id"
    res = base.reset_index()
    res["_umaban_key"] = pd.to_numeric(res["馬番"], errors="coerce").astype("Int64")
    merged = res.merge(agg, on=["race_id", "_umaban_key"], how="left")
    return merged.drop(columns=["_umaban_key"], errors="ignore").set_index("race_id")
