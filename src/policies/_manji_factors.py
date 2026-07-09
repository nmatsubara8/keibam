"""卍式・行動バイアス因子の抽出（JRDB専有指数を使わない範囲）。

各因子は「featured の1バッチ（index=race_id）」を受け取り、行ごとの**バケットラベル**
（文字列）を返す純関数。点数（±）は付けない——点数は Model 2（回収率較正 Layer A）が
学習期間の回収率から決める。ここは「どのバケットに属すか」だけを決定的に返す。

設計方針:
- 必要な生列が無い因子は、全行 "na"（＝中立・0点）を返して安全にスキップする
  （既存パイプラインの「列不在は自動スキップ」の作法に合わせる）。
- レース内相対（馬体重順位・斤量順位など）は index=race_id で groupby して計算する。
- 卍さんの「馬体重は今走でなく前走の順位」等の細部は、featured に前走列がある場合のみ
  近似し、無ければ今走で代用する（各因子の docstring に明記）。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.constants._results_cols import ResultsCols

NA = "na"


def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _parse_body_weight(s: pd.Series) -> pd.Series:
    """'480(+4)' / '480' / '計不' → 先頭の体重(kg)。数値化不能は NaN。"""
    if s is None:
        return pd.Series(np.nan, index=[])
    return pd.to_numeric(s.astype(str).str.extract(r"(\d+)")[0], errors="coerce")


def _parse_sex(s: pd.Series) -> pd.Series:
    """'牡3'/'牝4'/'セ5' → '牡'/'牝'/'セ'。"""
    return s.astype(str).str.extract(r"([牡牝セ])")[0].fillna(NA)


def _parse_age(s: pd.Series) -> pd.Series:
    """'牡3' → 3（歳）。"""
    return pd.to_numeric(s.astype(str).str.extract(r"(\d+)")[0], errors="coerce")


# --- スキーマ耐性リゾルバ: 生netkeiba列でも engineered列でも同じ値を取り出す --------

def _col(df: pd.DataFrame, *names):
    """候補列名のうち最初に存在するものを返す（無ければ None）。"""
    for n in names:
        if n in df.columns:
            return n
    return None


def _age_series(df: pd.DataFrame) -> pd.Series:
    c = _col(df, "年齢", "馬齢", "age")
    if c is not None:
        return _num(df[c])
    if ResultsCols.SEX_AGE in df.columns:
        return _parse_age(df[ResultsCols.SEX_AGE])
    return pd.Series(np.nan, index=df.index)


def _sex_series(df: pd.DataFrame) -> pd.Series:
    c = _col(df, "性別", "性", "sex", "性コード")
    if c is not None:
        return df[c].astype(str).str.extract(r"([牡牝セ])")[0].fillna(NA)
    if ResultsCols.SEX_AGE in df.columns:
        return _parse_sex(df[ResultsCols.SEX_AGE])
    return pd.Series(NA, index=df.index)


def _body_weight_series(df: pd.DataFrame) -> pd.Series:
    c = _col(df, "体重", "馬体重", "body_weight")
    if c is None:
        return pd.Series(np.nan, index=df.index)
    s = df[c]
    if pd.api.types.is_numeric_dtype(s):
        return _num(s)
    return _parse_body_weight(s)  # "480(+4)" 形式


def _weight_diff_series(df: pd.DataFrame) -> pd.Series:
    c = _col(df, "体重変化", "馬体重増減", "weight_diff", "体重増減")
    if c is not None:
        return _num(df[c])
    c2 = _col(df, "馬体重")
    if c2 is not None:
        m = df[c2].astype(str).str.extract(r"\(([-+]?\d+)\)")[0]
        return _num(m)
    return pd.Series(np.nan, index=df.index)


def _interval_series(df: pd.DataFrame) -> pd.Series:
    c = _col(df, "interval", "days", "レース間隔", "休養日数")
    return _num(df[c]) if c is not None else pd.Series(np.nan, index=df.index)


def _race_type_series(df: pd.DataFrame):
    """芝ダ列（文字列 Series）。無ければ None。"""
    c = _col(df, "race_type", "芝ダ", "芝ダート", "track_type", "コース種別")
    return df[c].astype(str) if c is not None else None


def _dist_change_series(df: pd.DataFrame) -> pd.Series:
    c = _col(df, "dist_change", "距離変化", "距離差", "距離増減")
    return _num(df[c]) if c is not None else pd.Series(np.nan, index=df.index)


# --- 個別因子 --------------------------------------------------------------

def f_umaban_parity(df: pd.DataFrame) -> pd.Series:
    """馬番の奇偶（卍: 奇数/偶数で加減点）。"""
    if ResultsCols.UMABAN not in df.columns:
        return pd.Series(NA, index=df.index)
    u = _num(df[ResultsCols.UMABAN])
    return np.where(u.isna(), NA, np.where(u % 2 == 1, "odd", "even"))


def f_sex(df: pd.DataFrame) -> pd.Series:
    return _sex_series(df).to_numpy()


def f_age(df: pd.DataFrame) -> pd.Series:
    """馬齢帯: 2-3=young / 4-5=prime / 6+=old。"""
    a = _age_series(df)
    if a.isna().all():
        return pd.Series(NA, index=df.index)
    out = pd.cut(a, [0, 3.5, 5.5, np.inf], labels=["young", "prime", "old"])
    return out.astype(object).fillna(NA).to_numpy()


def _season(df: pd.DataFrame) -> pd.Series:
    if "date" not in df.columns:
        return pd.Series(NA, index=df.index)
    m = pd.to_datetime(df["date"], errors="coerce").dt.month
    lab = pd.Series(NA, index=df.index, dtype=object)
    lab[m.isin([12, 1, 2])] = "winter"
    lab[m.isin([3, 4, 5])] = "spring"
    lab[m.isin([6, 7, 8])] = "summer"
    lab[m.isin([9, 10, 11])] = "autumn"
    return lab


def f_season(df: pd.DataFrame) -> pd.Series:
    return _season(df).to_numpy()


def f_season_sex(df: pd.DataFrame) -> pd.Series:
    """季節×性別（卍: 夏の牝馬に加点、など）。"""
    if "date" not in df.columns:
        return pd.Series(NA, index=df.index)
    sex = _sex_series(df)
    sea = _season(df)
    combo = sea.astype(str) + "_" + sex.astype(str)
    combo[(sea == NA) | (sex == NA)] = NA
    return combo.to_numpy()


def f_weight_rank(df: pd.DataFrame) -> pd.Series:
    """馬体重のレース内順位帯（light/mid/heavy）。全馬が重い/軽い開催の歪みを
    絶対値でなく順位で吸収する（卍の指摘）。"""
    w = _body_weight_series(df)
    if w.isna().all():
        return pd.Series(NA, index=df.index)
    r = w.groupby(df.index).rank(pct=True)
    out = pd.cut(r, [0, 1 / 3, 2 / 3, 1.0], labels=["light", "mid", "heavy"], include_lowest=True)
    return out.astype(object).where(w.notna(), NA).to_numpy()


def f_rotation(df: pd.DataFrame) -> pd.Series:
    """ローテ（前走からの間隔 interval[日]）: 連闘/中1-3週/中4週+/休養明け。"""
    iv = _interval_series(df)
    if iv.isna().all():
        return pd.Series(NA, index=df.index)
    lab = pd.Series(NA, index=df.index, dtype=object)
    lab[iv <= 8] = "rentai"        # 連闘（中0週）
    lab[(iv > 8) & (iv <= 27)] = "naka1_3"
    lab[(iv > 27) & (iv < 180)] = "naka4plus"
    lab[iv >= 180] = "kyuyoake"    # 休養明け
    return lab.to_numpy()


def f_dist_change(df: pd.DataFrame) -> pd.Series:
    """距離変更（今回−前走, dist_change[100m単位or m]）: 短縮/同/延長。"""
    dc = _dist_change_series(df)
    if dc.isna().all():
        return pd.Series(NA, index=df.index)
    lab = pd.Series(NA, index=df.index, dtype=object)
    lab[dc < 0] = "short"
    lab[dc == 0] = "same"
    lab[dc > 0] = "extend"
    return lab.to_numpy()


def f_kinryo_rank(df: pd.DataFrame) -> pd.Series:
    """斤量のレース内順位帯（light/mid/heavy）。"""
    if ResultsCols.KINRYO not in df.columns:
        return pd.Series(NA, index=df.index)
    k = _num(df[ResultsCols.KINRYO])
    r = k.groupby(df.index).rank(pct=True)
    out = pd.cut(r, [0, 1 / 3, 2 / 3, 1.0], labels=["light", "mid", "heavy"], include_lowest=True)
    return out.astype(object).where(k.notna(), NA).to_numpy()


def f_track_sex(df: pd.DataFrame) -> pd.Series:
    """トラック（芝/ダ）×性別（卍: トラックと性別から回収率を分析し加減）。"""
    rt = _race_type_series(df)
    sex = _sex_series(df)
    if rt is None or (sex == NA).all():
        return pd.Series(NA, index=df.index)
    combo = rt + "_" + sex.astype(str)
    combo[sex.to_numpy() == NA] = NA
    return combo.to_numpy()


def f_career(df: pd.DataFrame) -> pd.Series:
    """キャリア（出走回数帯）。featured に出走回数列がある場合のみ（無ければ na）。
    候補列名を順に探す。"""
    col = _col(df, "times", "n_races", "career", "出走回数", "race_count", "出走数")
    if col is None:
        return pd.Series(NA, index=df.index)
    c = _num(df[col])
    out = pd.cut(c, [-1, 0, 3, 8, np.inf], labels=["debut", "few", "mid", "many"])
    return out.astype(object).fillna(NA).to_numpy()


def f_popularity(df: pd.DataFrame) -> pd.Series:
    """人気帯（卍: 1番人気は過剰人気で減点/中穴に妙味）。人気列優先、無ければ単勝順位。"""
    if ResultsCols.POPULARITY in df.columns and _num(df[ResultsCols.POPULARITY]).notna().any():
        pop = _num(df[ResultsCols.POPULARITY])
    elif ResultsCols.TANSHO_ODDS in df.columns:
        pop = _num(df[ResultsCols.TANSHO_ODDS]).groupby(df.index).rank(method="min")
    else:
        return pd.Series(NA, index=df.index)
    out = pd.cut(pop, [0, 1, 3, 8, np.inf], labels=["fav1", "fav2_3", "mid4_8", "long9plus"])
    return out.astype(object).fillna(NA).to_numpy()


def f_waku(df: pd.DataFrame) -> pd.Series:
    """枠順×芝ダ（卍/データ: 芝は内枠有利・ダは外枠有利）。"""
    rt = _race_type_series(df)
    if ResultsCols.WAKUBAN not in df.columns or rt is None:
        return pd.Series(NA, index=df.index)
    w = _num(df[ResultsCols.WAKUBAN])
    pos = pd.cut(w, [0, 3, 5, 8], labels=["inner", "mid", "outer"])
    combo = rt + "_" + pos.astype(object)
    combo = combo.where(w.notna() & pos.notna(), NA)
    return combo.to_numpy()


def f_body_weight(df: pd.DataFrame) -> pd.Series:
    """馬体重の絶対帯（卍/データ: 大型有利・軽量危険）。"""
    w = _body_weight_series(df)
    if w.isna().all():
        return pd.Series(NA, index=df.index)
    out = pd.cut(w, [0, 440, 470, 500, np.inf], labels=["u440", "440_470", "470_500", "o500"])
    return out.astype(object).where(w.notna(), NA).to_numpy()


def f_weight_diff(df: pd.DataFrame) -> pd.Series:
    """前走比 馬体重増減の帯（卍: 大幅減は割引・大幅増は平場割引/重賞妙味）。"""
    d = _weight_diff_series(df)
    if d.isna().all():
        return pd.Series(NA, index=df.index)
    out = pd.cut(d, [-1000, -12, -3, 3, 12, 1000],
                 labels=["big_minus", "minus", "flat", "plus", "big_plus"])
    return out.astype(object).where(d.notna(), NA).to_numpy()


def f_kinryo_per_weight(df: pd.DataFrame) -> pd.Series:
    """斤量/馬体重 の相対負担帯（卍: 軽量馬×重斤量は危険）。"""
    if "kinryo_per_weight" in df.columns:
        r = _num(df["kinryo_per_weight"])
    else:
        w = _body_weight_series(df)
        if ResultsCols.KINRYO not in df.columns or w.isna().all():
            return pd.Series(NA, index=df.index)
        r = _num(df[ResultsCols.KINRYO]) / w
    q = r.groupby(df.index).rank(pct=True)
    out = pd.cut(q, [0, 1 / 3, 2 / 3, 1.0], labels=["light", "mid", "heavy"], include_lowest=True)
    return out.astype(object).where(r.notna(), NA).to_numpy()


def f_age_rotation(df: pd.DataFrame) -> pd.Series:
    """馬齢×休養明け（卍/データ: 若馬の長期明けは加点・古馬の長期明けは減点）。"""
    a = _age_series(df)
    iv = _interval_series(df)
    if a.isna().all() or iv.isna().all():
        return pd.Series(NA, index=df.index)
    young = a <= 3
    layoff = iv >= 90
    lab = pd.Series("other", index=df.index, dtype=object)
    lab[young & layoff] = "young_layoff"
    lab[(~young) & layoff] = "old_layoff"
    lab[(a.isna()) | (iv.isna())] = NA
    return lab.to_numpy()


def f_dist_age(df: pd.DataFrame) -> pd.Series:
    """距離変更×馬齢（卍: 2歳の距離短縮/延長で回収率が変わる）。"""
    dc = _dist_change_series(df)
    a = _age_series(df)
    if dc.isna().all() or a.isna().all():
        return pd.Series(NA, index=df.index)
    grp = np.where(a <= 3, "young", "old")
    dirn = np.where(dc < 0, "short", np.where(dc > 0, "extend", "same"))
    combo = pd.Series([f"{g}_{d}" for g, d in zip(grp, dirn, strict=False)], index=df.index)
    combo = combo.where(dc.notna() & a.notna(), NA)
    return combo.to_numpy()


def f_prev_finish(df: pd.DataFrame) -> pd.Series:
    """前走着順帯（卍/データ: 前走6着や前走人気で凡走は過小評価=妙味、二桁着はカット）。

    featured に前走着順の列がある場合のみ（無ければ na）。候補列名を寛容に探す。
    """
    col = next((c for c in ("前走着順", "prev_rank", "前走_着順", "着順_1", "rank_prev",
                            "last_rank") if c in df.columns), None)
    if col is None:
        return pd.Series(NA, index=df.index)
    r = _num(df[col])
    out = pd.cut(r, [0, 1, 3, 5, 6, 10, 99],
                 labels=["p1", "p2_3", "p4_5", "p6", "p7_10", "p11plus"])
    return out.astype(object).where(r.notna(), NA).to_numpy()


_SIRE_COLS = ("父", "種牡馬", "sire", "father", "父名", "種牡馬名")


def f_sire_line(df: pd.DataFrame) -> pd.Series:
    """種牡馬の大系統（JRDB 系統コード表による分類）。血統の「洗い替え」因子。

    featured に種牡馬名列があれば大系統に束ねる（例: ヘイロー系/ミスタープロスペクター系）。
    未分類・列不在は na（中立）。生の種牡馬名より疎性が低く、系統×条件の回収率を学習しやすい。
    """
    col = next((c for c in _SIRE_COLS if c in df.columns), None)
    if col is None:
        return pd.Series(NA, index=df.index)
    from src.features._sire_line import daikeito
    vals = df[col].map(daikeito)
    return vals.where(vals != "不明", NA).to_numpy()


# 因子レジストリ（名前 → 抽出関数）。Model 2 はこの名前で点数を較正する。
FACTORS: dict[str, callable] = {
    "umaban_parity": f_umaban_parity,
    "sex": f_sex,
    "age": f_age,
    "season": f_season,
    "season_sex": f_season_sex,
    "weight_rank": f_weight_rank,
    "rotation": f_rotation,
    "dist_change": f_dist_change,
    "kinryo_rank": f_kinryo_rank,
    "track_sex": f_track_sex,
    "career": f_career,
    "sire_line": f_sire_line,
    "popularity": f_popularity,
    "waku": f_waku,
    "body_weight": f_body_weight,
    "weight_diff": f_weight_diff,
    "kinryo_per_weight": f_kinryo_per_weight,
    "age_rotation": f_age_rotation,
    "dist_age": f_dist_age,
    "prev_finish": f_prev_finish,
}


def buckets(df: pd.DataFrame, names: list[str] | None = None) -> pd.DataFrame:
    """指定因子（既定=全因子）のバケットラベルを列に持つ DataFrame を返す。index は df と同一。"""
    names = names or list(FACTORS)
    out = {}
    for name in names:
        vals = FACTORS[name](df)
        out[name] = pd.Series(vals, index=df.index).astype(object).fillna(NA)
    return pd.DataFrame(out, index=df.index)
