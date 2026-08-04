"""時間前方 expanding target-encoding（スムージング付き）— PyCon2018 A1/A2 の中核。

entity（騎手/調教師/馬主/父…）× context（コース種別/距離/馬場…）ごとに、目的変数（勝ち/複勝/
相対着順…）の **過去平均** を特徴量にする（Target Encoding）。少数カテゴリのブレは全体平均への
**スムージング**で抑える（A2）。

リーク回避（最重要）:
  - 各行の集計は「**当該行より厳密に過去の日付**（date < 自分の date）」の行のみを使う。
  - 同一日（＝同一開催・同一レース内で複数騎乗など）の他行も **使わない**（同日リークの遮断）。
  - keibam 既存の horse_results 集計と同じ「date < target_date」規約に一致。
これにより学習・推論で同一計算となり train/serve skew も出ない。

`_horse_features.py` は馬(horse_id)エンティティの時間前方集計を既に担うため、本モジュールは
**騎手/調教師/馬主/父など「馬以外のエンティティ」×context** を主対象にする（重複回避）。
"""

from __future__ import annotations

import pandas as pd


def _prior_by_date(sums: pd.Series, cnts: pd.Series, n_key_levels: int) -> tuple[pd.Series, pd.Series]:
    """(keys..., date) 粒度の per-date 合計/件数から、各 (keys,date) について
    「その date より前」の累積合計/件数を返す（同日を含めない）。

    keys の levels 内で date 昇順に累積 → 1つ shift して当該 date を除外する。
    """
    lvl = list(range(n_key_levels))  # keys の level（最後の level = date は除く）
    csum = sums.groupby(level=lvl).cumsum()
    ccnt = cnts.groupby(level=lvl).cumsum()
    prior_sum = csum.groupby(level=lvl).shift(1)
    prior_cnt = ccnt.groupby(level=lvl).shift(1)
    return prior_sum, prior_cnt


def expanding_target_encode(
    df: pd.DataFrame,
    keys: list[str],
    target: str,
    date_col: str = "date",
    alpha: float = 20.0,
    global_prior: float | None = None,
    cold_start_prior: float = 0.0,
) -> pd.Series:
    """各行について、``keys`` を共有する **厳密に過去（date<自分）** の ``target`` 平均を
    スムージングして返す（df.index に整列）。

    smoothed = (Σ_past target + α·prior) / (n_past + α)
      - prior: ``global_prior`` 指定時はその定数。None なら **過去のみの expanding 全体平均**
        （日付単位・厳密過去）。履歴が全く無い行は、未来ラベルを参照しない固定値
        ``cold_start_prior`` を使う。
      - α: スムージング強度（大きいほど全体平均へ強く引く。少数カテゴリのブレ抑制）。

    Parameters
    ----------
    keys : entity（+context）列。例 ["jockey_id"] / ["jockey_id","race_type"]
    target : 数値化可能な目的変数列（0/1 でも連続でも可）。
    date_col : 日付列（datetime 変換可能）。
    cold_start_prior : 過去データが一件もない時だけ使う、データ非依存の事前値。
        目的変数に適した値が既知なら呼び出し側で明示する。
    """
    if not keys:
        raise ValueError("keys は1つ以上必要です")
    for c in [*keys, target, date_col]:
        if c not in df.columns:
            raise KeyError(f"列 '{c}' が df にありません")

    d = df[[*keys, date_col, target]].copy()
    d[target] = pd.to_numeric(d[target], errors="coerce")
    d[date_col] = pd.to_datetime(d[date_col], errors="coerce")

    # (keys, date) 粒度で合計・件数
    grp = d.groupby([*keys, date_col], sort=True)[target].agg(_sum="sum", _cnt="count")
    prior_sum, prior_cnt = _prior_by_date(grp["_sum"], grp["_cnt"], len(keys))
    prior = pd.DataFrame({"_psum": prior_sum, "_pcnt": prior_cnt}).reset_index()

    # 全体 prior（スムージングのアンカー）
    if global_prior is None:
        gg = d.groupby(date_col)[target].agg(_s="sum", _c="count").sort_index()
        gcs = gg["_s"].cumsum().shift(1)
        gcc = gg["_c"].cumsum().shift(1)
        prior_glob_by_date = gcs / gcc
        # 最古日には過去の観測が存在しない。全期間平均で埋めると未来ラベルが
        # 混入するため、データ非依存の cold-start prior だけを使用する。
        prior_glob_by_date = prior_glob_by_date.fillna(float(cold_start_prior))
        gmap = prior_glob_by_date.to_dict()
        prior["_pglob"] = prior[date_col].map(gmap).fillna(float(cold_start_prior))
    else:
        prior["_pglob"] = float(global_prior)

    psum = prior["_psum"].fillna(0.0)
    pcnt = prior["_pcnt"].fillna(0.0)
    prior["_te"] = (psum + alpha * prior["_pglob"]) / (pcnt + alpha)

    # 行へ写像（keys+date で結合）。df の元順序・index を保持。
    left = df[[*keys, date_col]].copy()
    left[date_col] = pd.to_datetime(left[date_col], errors="coerce")
    left["_row"] = range(len(left))
    merged = left.merge(prior[[*keys, date_col, "_te"]], on=[*keys, date_col], how="left")
    merged = merged.sort_values("_row")
    return pd.Series(merged["_te"].to_numpy(), index=df.index)


# エンティティ×context×集計の既定スペック（PyCon A1）。
# results 表に存在する列のみ使い、無い context は自動スキップする。
DEFAULT_PERSON_SPECS: list[dict] = [
    # (name, keys, target)
    {"name": "jockey_win_te", "keys": ["jockey_id"], "target": "_win"},
    {"name": "jockey_place_te", "keys": ["jockey_id"], "target": "_place"},
    {"name": "jockey_win_te_by_type", "keys": ["jockey_id", "race_type"], "target": "_win"},
    {"name": "jockey_place_te_by_place", "keys": ["jockey_id", "開催"], "target": "_place"},
    {"name": "trainer_win_te", "keys": ["trainer_id"], "target": "_win"},
    {"name": "trainer_place_te", "keys": ["trainer_id"], "target": "_place"},
    {"name": "trainer_win_te_by_type", "keys": ["trainer_id", "race_type"], "target": "_win"},
    {"name": "owner_win_te", "keys": ["owner_id"], "target": "_win"},
]


# context（開催/クラス等）× 目的変数の expanding target-encoding スペック。
# 高カーディナリティの One-Hot（開催 57 種）や順序カテゴリ（race_class 11 種）を、過去実績に
# 基づくスムージング済みの「その競馬場/クラスでの勝率・複勝率」1 列で表現する（One-Hot と併用可能）。
# person 由来（jockey/trainer/owner）とは別軸なので独立の spec とし、別の env で切替できるようにする。
DEFAULT_CONTEXT_SPECS: list[dict] = [
    # (name, keys, target)
    {"name": "place_place_te", "keys": ["開催"], "target": "_place"},
    {"name": "place_win_te", "keys": ["開催"], "target": "_win"},
    {"name": "race_class_place_te", "keys": ["race_class"], "target": "_place"},
    {"name": "race_class_win_te", "keys": ["race_class"], "target": "_win"},
    # 天候 × 馬場状態 の交互作用（雨→重馬場 等、単独 One-Hot では表せない組合せ効果）。
    {"name": "weather_gs_place_te", "keys": ["weather", "ground_state1"], "target": "_place"},
    {"name": "weather_gs_win_te", "keys": ["weather", "ground_state1"], "target": "_win"},
]


# エンティティ × エンティティ の交互作用 target-encoding スペック。
# 単独エンティティ（騎手/調教師/馬主は person_te、馬は _horse_features）は別途カバー済みなので、
# ここは「組合せ効果」だけを狙う。高カーディナリティで大半が初出＝prior へ縮小されるため、
# スムージング（alpha）で希少組合せのブレを抑える前提。
# 退化的な組（馬×調教師・馬×馬主＝ほぼ 1:1 で定数化）は除外する。
DEFAULT_ENTITY_INTERACTION_SPECS: list[dict] = [
    # (name, keys, target)
    {"name": "jockey_trainer_win_te", "keys": ["jockey_id", "trainer_id"], "target": "_win"},
    {"name": "jockey_trainer_place_te", "keys": ["jockey_id", "trainer_id"], "target": "_place"},
    {"name": "horse_jockey_win_te", "keys": ["horse_id", "jockey_id"], "target": "_win"},
    {"name": "horse_jockey_place_te", "keys": ["horse_id", "jockey_id"], "target": "_place"},
    {"name": "jockey_owner_win_te", "keys": ["jockey_id", "owner_id"], "target": "_win"},
]


def build_person_form_features(
    results: pd.DataFrame,
    specs: list[dict] | None = None,
    date_col: str = "date",
    rank_col: str = "着順",
    alpha: float = 20.0,
) -> pd.DataFrame:
    """results 履歴から person(騎手/調教師/馬主)×context の expanding target-encoding を作る。

    results は「全レース結果の履歴」で、各行が (race, horse) 粒度。1着=_win, 3着内=_place を
    目的変数に、各行の**厳密に過去**の entity 平均（スムージング済み）を列として返す。

    返り値: results.index に整列した特徴量 DataFrame（spec.name 列）。context 列や entity 列が
    results に無い spec は自動スキップ（ログ用に列は作らない）。学習時に results から一括生成し、
    ライブ推論では最新スナップショットで同じ計算を行う想定（別途）。
    """
    specs = specs if specs is not None else DEFAULT_PERSON_SPECS
    rank = pd.to_numeric(results[rank_col], errors="coerce")
    base = results.copy()
    base["_win"] = (rank == 1).astype(float)
    base["_place"] = (rank <= 3).astype(float)

    out = pd.DataFrame(index=results.index)
    for spec in specs:
        keys, target, name = spec["keys"], spec["target"], spec["name"]
        if any(k not in base.columns for k in keys) or target not in base.columns:
            continue  # 必要列が無ければスキップ（owner_id 欠如・context 未整備など）
        out[name] = expanding_target_encode(
            base, keys=keys, target=target, date_col=date_col, alpha=alpha
        )
    return out


def person_te_for_upcoming(
    history_results: pd.DataFrame,
    upcoming: pd.DataFrame,
    race_date,
    specs: list[dict] | None = None,
    date_col: str = "date",
    rank_col: str = "着順",
    alpha: float = 20.0,
) -> pd.DataFrame:
    """ライブ推論: 履歴(着順あり)＋出馬表(着順NaN・date=race_date)を結合し、`build_person_form_features`
    で **出馬表行の as-of encoding だけ** を返す（index=upcoming.index）。

    学習と同一の `expanding_target_encode`（厳密過去）を通すため train/serve skew が出ない。
    出馬表行の date は発走日にそろえ、履歴（発走日より前）のみが集計対象になる。
    """
    hist = history_results.copy()
    up = upcoming.copy()
    up[rank_col] = pd.NA
    up[date_col] = pd.to_datetime(race_date)
    combined = pd.concat([hist, up], ignore_index=True)
    feats = build_person_form_features(
        combined, specs=specs, date_col=date_col, rank_col=rank_col, alpha=alpha
    )
    up_feats = feats.iloc[len(hist):].copy()
    up_feats.index = upcoming.index
    return up_feats
