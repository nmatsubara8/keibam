"""ペース状態 P(z) 予測器（実データフェーズ・タスク1）— Mixture-PL の入力を作る。

Step3 の識別性知見（一様 P(z)+勝者NLL では β 識別不能）により、β(style,z) を実データで
学習するには**発走前情報からレース別 P(z) を出す予測器が前提条件**。本モジュールがそれ。

構成（3段・全て純粋関数、I/O は CLI 側）:
1) 教師ラベル z ∈ {slow, normal, fast}
   horse_results の「ペース」列（"35.1-36.8"＝レースの前半-後半3F）をレースキー
   (日付, 場コード, R)＋(horse_id, date) の2経路で featured に結合し、レース単位の
   (front, back) を得る（horse_id 体系不一致に耐えるため race_pace_balance 参照）。バランス = back − front
   （正が大きい＝前傾＝前半速い＝**ハイペース**）。コース条件でスケールが違うため
   （race_type×距離帯）グループ内の 33/66 分位で3分割 → クラスは構成上ほぼ均等。
   ラベルは事後情報で良い（教師）。**特徴量は発走前のみ**（ここが前進安全の境界）。
2) 発走前特徴量（レース単位）
   各馬の過去走由来 pace_median（第1コーナー位置比・featured 既存列）から
   先行勢比率・逃げ候補頭数・脚質構成の統計量＋距離/頭数/馬場等を集計する。
   f_pace_pressure（卍）の連続・多変量版。
3) LightGBM 多クラス（説明可能性の維持が要件 — SHAP/gain で「何が Slow/Fast を
   決めたか」を出せる。NN は不採用）。学習は必ず rolling-origin の fit_fn 内で行う。

出力 P(z) は _mixture_pl.PACE_STATES のキーに合わせた dict（mixture_win_probs へ直結）。
"""
from __future__ import annotations

import math
import re
from typing import Mapping, Sequence

import numpy as np
import pandas as pd

from src.constants._pace_states import PACE_STATES

# 予測器の特徴量列（レース単位）。存在しない源列は NaN のまま LightGBM に渡す。
PZ_FEATURE_COLS: tuple[str, ...] = (
    "front_ratio",      # 先行型（pace_median<0.5）比率 = f_pace_pressure の連続版
    "nige_count",       # 逃げ候補（pace_median<0.2）頭数
    "senko_count",      # 先行候補（0.2<=pace_median<0.5）頭数
    "mean_pace_median", # 脚質構成の平均（小=前掛かり）
    "min_pace_median",  # 最も前に行く馬の値（単騎逃げ判定）
    "std_pace_median",  # 構成のばらつき
    "inner_front",      # 先行勢の平均枠（内枠に先行馬＝ハナ争い緩和）
    "n_horses",
    "course_len",
    "is_dirt",
    "ground_bad",       # 稍重以下=1
)

_PACE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*$")


def parse_pace_string(s: object) -> tuple[float, float] | None:
    """「ペース」文字列 "35.1-36.8" → (前半, 後半) 秒。解釈不能は None。"""
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return None
    m = _PACE_RE.match(str(s))
    if not m:
        return None
    front, back = float(m.group(1)), float(m.group(2))
    if front <= 0 or back <= 0:
        return None
    return front, back


def race_pace_balance(
    featured_keys: pd.DataFrame, horse_results: pd.DataFrame
) -> pd.Series:
    """レース単位のペースバランス（back−front 秒）を返す（index=race_id）。

    結合は2経路の和集合（実データ検証で horse_id 体系が featured=8桁内部ID /
    horse_results=netkeiba 10桁と**完全に別物**（重なり0）だったため、ID に依存しない
    レースキー結合を主経路にする）:
      A) レースキー: (日付, 場コード, R)。featured 側は race_id の 5-6 桁目=場コード・
         末尾2桁=R。hr 側は「開催」（例 '3中山4'）から数字を除いた場名を
         Master.PLACE_DICT でコード化し、R 列と合わせる。
      B) (horse_id, date): 両側の horse_id を '.0' 除去で正規化して結合（体系が一致する
         環境でのフォールバック）。
    レース内の最頻 (back−front) を採用（同一レースの馬は同値のはずだが取得ゆらぎに多数決）。

    featured_keys: 列 race_id, date（horse_id は任意）。horse_results: 列 日付/date,
    ペース（開催, R, horse_id は任意・ある経路だけ使う）。
    """
    from src.constants._master import Master

    hr = horse_results.copy()
    date_col = "date" if "date" in hr.columns else "日付"
    pace_col = "ペース" if "ペース" in hr.columns else "pace"
    if pace_col not in hr.columns or date_col not in hr.columns:
        return pd.Series(dtype=float)
    keep = [c for c in ("horse_id", date_col, pace_col, "開催", "R") if c in hr.columns]
    hr = hr[keep].dropna(subset=[pace_col, date_col])
    parsed = hr[pace_col].map(parse_pace_string)
    hr = hr[parsed.notna()].copy()
    if hr.empty:
        return pd.Series(dtype=float)
    hr["_bal"] = [b - f for f, b in parsed.dropna()]
    hr["_d"] = pd.to_datetime(hr[date_col], errors="coerce").dt.normalize()

    fk = featured_keys.copy()
    fk["race_id"] = fk["race_id"].astype(str)
    fk["_d"] = pd.to_datetime(fk["date"], errors="coerce").dt.normalize()

    frames: list[pd.DataFrame] = []

    # A) レースキー (日付, 場コード, R) — ID 体系に依存しない主経路
    if "開催" in hr.columns and "R" in hr.columns:
        place_name = hr["開催"].astype(str).str.replace(r"\d", "", regex=True)
        hr_a = hr.assign(
            _place=place_name.map(Master.PLACE_DICT),
            _r=pd.to_numeric(hr["R"], errors="coerce"),
        ).dropna(subset=["_place", "_r", "_d"])
        fk_a = fk.assign(
            _place=fk["race_id"].str[4:6],
            _r=pd.to_numeric(fk["race_id"].str[-2:], errors="coerce"),
        ).dropna(subset=["_r", "_d"])
        a = fk_a.merge(
            hr_a[["_d", "_place", "_r", "_bal"]],
            on=["_d", "_place", "_r"], how="inner",
        )
        if not a.empty:
            frames.append(a[["race_id", "_bal"]])

    # B) (horse_id, date) — 体系が一致する環境でのフォールバック（'.0' 正規化）
    if "horse_id" in hr.columns and "horse_id" in fk.columns:
        norm = lambda s: s.astype(str).str.replace(r"\.0$", "", regex=True)  # noqa: E731
        hr_b = hr.assign(horse_id=norm(hr["horse_id"])).dropna(subset=["_d"])
        fk_b = fk.assign(horse_id=norm(fk["horse_id"])).dropna(subset=["_d"])
        b = fk_b.merge(
            hr_b[["horse_id", "_d", "_bal"]], on=["horse_id", "_d"], how="inner"
        )
        if not b.empty:
            frames.append(b[["race_id", "_bal"]])

    if not frames:
        return pd.Series(dtype=float)
    m = pd.concat(frames, ignore_index=True)
    # レース内の最頻バランス（多数決）。mode が複数なら先頭（決定的）。
    return m.groupby("race_id")["_bal"].agg(lambda s: float(s.mode().iloc[0]))


# JRDB SED race_pace（実測レースペース H/M/S）→ z。H=ハイ(前傾)=fast, M=平均=normal, S=スロー=slow。
_JRDB_PACE_TO_Z = {"H": "fast", "M": "normal", "S": "slow"}


def label_pace_from_sed(sed: pd.DataFrame) -> pd.Series:
    """JRDB SED の race_pace(H/M/S・実測) から教師ラベル z を作る（index=race_id）。

    netkeiba の『ペース』文字列が無い JRDB 経路の正式ラベル源（race_pace は実測レースペースで
    レース内一定＝出走馬行に同値なので race_id で最頻に畳む）。空白/未知コードは除外。
    ラベルは事後情報で可（教師）。前3F/後3F(mae3f/ato3f)からの balance 版が必要なら別途。
    """
    if (sed is None or len(sed) == 0 or "race_pace" not in sed.columns
            or "race_id" not in sed.columns):
        return pd.Series(dtype=object)
    d = sed[["race_id", "race_pace"]].copy()
    d["race_id"] = d["race_id"].astype(str)
    z = d["race_pace"].astype(str).str.strip().str.upper().map(_JRDB_PACE_TO_Z)
    d = d.assign(_z=z).dropna(subset=["_z"])
    if d.empty:
        return pd.Series(dtype=object)
    return d.groupby("race_id")["_z"].agg(lambda s: s.mode().iloc[0])


def label_pace_states(
    balance: pd.Series, groups: pd.DataFrame | None = None
) -> pd.Series:
    """バランス（back−front）→ z ∈ {slow, normal, fast}（index=race_id）。

    groups（列 race_type, dist_band など・index=race_id）を渡すとグループ内 33/66 分位で
    3分割する（距離/馬場種でスケールが違うため**レース条件相対**で切る）。無指定は全体分位。
    バランス大（前傾）= fast、小（後傾/瞬発戦）= slow。

    分割は rank(method='first') ベース: ペース文字列は 0.1 秒刻みで分位境界に同値の塊が
    でき、素の quantile 切りだとクラスが偏る（実データで確認）。順位で切れば同値があっても
    **クラスは厳密に均等**（境界の同値は index 順で決定的に振り分け＝3状態の潜在ラベルとして許容）。
    """
    if balance.empty:
        return pd.Series(dtype=object)

    def _cut(s: pd.Series) -> pd.Series:
        r = s.rank(method="first") / len(s)
        return pd.Series(
            np.where(r <= 1 / 3, "slow", np.where(r <= 2 / 3, "normal", "fast")),
            index=s.index,
        )

    if groups is None or groups.empty:
        return _cut(balance)
    g = groups.reindex(balance.index)
    key = g.astype(str).agg("|".join, axis=1)
    return balance.groupby(key, group_keys=False).apply(_cut)


def build_race_features(featured: pd.DataFrame) -> pd.DataFrame:
    """featured（per-horse 行・index=race_id）→ レース単位の発走前特徴量（PZ_FEATURE_COLS）。

    使う源列（あるものだけ）: pace_median（過去走の第1コーナー位置比・前進安全）、
    枠番、course_len、race_type、ground_state、頭数はレース内行数から。
    """
    df = featured
    rid = pd.Series(df.index.astype(str), index=df.index, name="race_id")
    out = pd.DataFrame(index=pd.Index(sorted(rid.unique()), name="race_id"))

    n = rid.groupby(rid).size()
    out["n_horses"] = n

    if "pace_median" in df.columns:
        pm = pd.to_numeric(df["pace_median"], errors="coerce")
        g = pm.groupby(rid)
        front = (pm < 0.5).astype(float).groupby(rid).sum()
        valid = pm.notna().astype(float).groupby(rid).sum()
        out["front_ratio"] = (front / valid.replace(0, np.nan))
        out["nige_count"] = (pm < 0.2).astype(float).groupby(rid).sum()
        out["senko_count"] = ((pm >= 0.2) & (pm < 0.5)).astype(float).groupby(rid).sum()
        out["mean_pace_median"] = g.mean()
        out["min_pace_median"] = g.min()
        out["std_pace_median"] = g.std()
        if "枠番" in df.columns:
            waku = pd.to_numeric(df["枠番"], errors="coerce")
            w_front = waku.where(pm < 0.5)
            out["inner_front"] = w_front.groupby(rid).mean()

    def _first(col: str) -> pd.Series | None:
        if col not in df.columns:
            return None
        return df[col].groupby(rid).first()

    cl = _first("course_len")
    if cl is not None:
        out["course_len"] = pd.to_numeric(cl, errors="coerce")
    rt = _first("race_type")
    if rt is not None:
        out["is_dirt"] = rt.astype(str).str.contains("ダ").astype(float)
    gs = _first("ground_state")
    if gs is not None:
        out["ground_bad"] = (~gs.astype(str).isin(["良"])).astype(float)

    return out.reindex(columns=list(PZ_FEATURE_COLS))


def fit_pz(
    features: pd.DataFrame,
    labels: pd.Series,
    *,
    num_boost_round: int = 400,
    valid_fraction: float = 0.15,
    params: Mapping | None = None,
):
    """P(z) の LightGBM 多クラス学習（**rolling-origin の fit_fn 内で呼ぶこと**）。

    features は PZ_FEATURE_COLS のレース単位フレーム、labels は z（PACE_STATES 値）。
    valid は**時系列末尾**を切る（レース内でなく年方向のホールドアウト・早期終了用）。
    """
    import lightgbm as lgb

    idx = features.index.intersection(labels.dropna().index)
    x = features.loc[idx]
    y = labels.loc[idx].map({z: i for i, z in enumerate(PACE_STATES)})
    order = np.argsort(idx.astype(str))  # race_id 昇順 ≒ 時系列順
    x, y = x.iloc[order], y.iloc[order]
    n_valid = max(1, int(len(x) * valid_fraction))
    p = {
        "objective": "multiclass", "num_class": len(PACE_STATES),
        "metric": "multi_logloss", "learning_rate": 0.05, "num_leaves": 15,
        "min_data_in_leaf": 50, "feature_fraction": 0.9, "verbosity": -1,
        **(params or {}),
    }
    dtrain = lgb.Dataset(x.iloc[:-n_valid], label=y.iloc[:-n_valid])
    dvalid = lgb.Dataset(x.iloc[-n_valid:], label=y.iloc[-n_valid:], reference=dtrain)
    return lgb.train(p, dtrain, num_boost_round=num_boost_round,
                     valid_sets=[dvalid],
                     callbacks=[lgb.early_stopping(30, verbose=False)])


def predict_pz(model, features: pd.DataFrame) -> pd.DataFrame:
    """P(z) 予測（列 = PACE_STATES・各行 Σ=1・index は features のまま）。"""
    prob = model.predict(features, num_iteration=getattr(model, "best_iteration", None))
    return pd.DataFrame(prob, index=features.index, columns=list(PACE_STATES))


def pz_dict(row: Mapping[str, float]) -> dict[str, float]:
    """予測1行 → mixture_win_probs(pace_probs=) にそのまま渡せる dict。"""
    return {z: float(row[z]) for z in PACE_STATES}


def explain_pz(model, features: pd.DataFrame, top_n: int = 10) -> list[tuple[str, float]]:
    """説明可能性レポート: SHAP（あれば）/ gain 重要度で「何が Slow/Fast を決めたか」。

    LightGBM 採用の理由そのもの（NN だとここがブラックボックスになる）。
    返り値: [(特徴量名, 重要度)] 降順 top_n。
    """
    try:
        import shap

        sv = shap.TreeExplainer(model).shap_values(features)
        imp = np.abs(np.asarray(sv)).mean(axis=(0, 1))
    except Exception:  # noqa: BLE001 — shap 不在は gain 重要度へフォールバック
        imp = model.feature_importance(importance_type="gain")
    names = list(features.columns)
    pairs = sorted(zip(names, imp, strict=False), key=lambda t: -float(t[1]))
    return [(n, float(v)) for n, v in pairs[:top_n]]


def evaluate_pz(
    pred: pd.DataFrame, labels: pd.Series, *, prior: Sequence[float] | None = None
) -> dict:
    """P(z) の品質: multi-logloss と accuracy を、クラス事前分布ベースラインと比較する。

    prior 無指定は一様 1/3。ラベルは構成上ほぼ均等なので一様が自然な帰無。
    ΔlogLoss < 0 が「pace_pressure 系特徴に情報がある」ことの判定（有意性は
    rolling-origin 全体で _model_compare 側の Bootstrap に載せる）。
    """
    idx = pred.index.intersection(labels.dropna().index)
    if len(idx) == 0:
        return {"n": 0}
    p = pred.loc[idx]
    y = labels.loc[idx]
    eps = 1e-12
    ll = float(-np.mean([np.log(max(p.at[i, y.at[i]], eps)) for i in idx]))
    pri = np.asarray(prior if prior is not None else [1 / 3] * len(PACE_STATES))
    pri = pri / pri.sum()
    ll0 = float(-np.mean([np.log(max(pri[list(PACE_STATES).index(y.at[i])], eps)) for i in idx]))
    acc = float(np.mean([p.loc[i].idxmax() == y.at[i] for i in idx]))
    return {"n": int(len(idx)), "logloss": ll, "logloss_prior": ll0,
            "d_logloss": ll - ll0, "accuracy": acc}
