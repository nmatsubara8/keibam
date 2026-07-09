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


# --- 個別因子 --------------------------------------------------------------

def f_umaban_parity(df: pd.DataFrame) -> pd.Series:
    """馬番の奇偶（卍: 奇数/偶数で加減点）。"""
    if ResultsCols.UMABAN not in df.columns:
        return pd.Series(NA, index=df.index)
    u = _num(df[ResultsCols.UMABAN])
    return np.where(u.isna(), NA, np.where(u % 2 == 1, "odd", "even"))


def f_sex(df: pd.DataFrame) -> pd.Series:
    if ResultsCols.SEX_AGE not in df.columns:
        return pd.Series(NA, index=df.index)
    return _parse_sex(df[ResultsCols.SEX_AGE]).to_numpy()


def f_age(df: pd.DataFrame) -> pd.Series:
    """馬齢帯: 2-3=young / 4-5=prime / 6+=old。"""
    if ResultsCols.SEX_AGE not in df.columns:
        return pd.Series(NA, index=df.index)
    a = _parse_age(df[ResultsCols.SEX_AGE])
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
    if ResultsCols.SEX_AGE not in df.columns or "date" not in df.columns:
        return pd.Series(NA, index=df.index)
    sex = _parse_sex(df[ResultsCols.SEX_AGE])
    sea = _season(df)
    combo = sea.astype(str) + "_" + sex.astype(str)
    combo[(sea == NA) | (sex == NA)] = NA
    return combo.to_numpy()


def f_weight_rank(df: pd.DataFrame) -> pd.Series:
    """馬体重のレース内順位帯（light/mid/heavy）。全馬が重い/軽い開催の歪みを
    絶対値でなく順位で吸収する（卍の指摘）。"""
    if ResultsCols.WEIGHT_AND_DIFF not in df.columns:
        return pd.Series(NA, index=df.index)
    w = _parse_body_weight(df[ResultsCols.WEIGHT_AND_DIFF])
    r = w.groupby(df.index).rank(pct=True)
    out = pd.cut(r, [0, 1 / 3, 2 / 3, 1.0], labels=["light", "mid", "heavy"], include_lowest=True)
    return out.astype(object).where(w.notna(), NA).to_numpy()


def f_rotation(df: pd.DataFrame) -> pd.Series:
    """ローテ（前走からの間隔 interval[日]）: 連闘/中1-3週/中4週+/休養明け。"""
    if "interval" not in df.columns:
        return pd.Series(NA, index=df.index)
    iv = _num(df["interval"])
    lab = pd.Series(NA, index=df.index, dtype=object)
    lab[iv <= 8] = "rentai"        # 連闘（中0週）
    lab[(iv > 8) & (iv <= 27)] = "naka1_3"
    lab[(iv > 27) & (iv < 180)] = "naka4plus"
    lab[iv >= 180] = "kyuyoake"    # 休養明け
    return lab.to_numpy()


def f_dist_change(df: pd.DataFrame) -> pd.Series:
    """距離変更（今回−前走, dist_change[100m単位or m]）: 短縮/同/延長。"""
    if "dist_change" not in df.columns:
        return pd.Series(NA, index=df.index)
    dc = _num(df["dist_change"])
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
    if "race_type" not in df.columns or ResultsCols.SEX_AGE not in df.columns:
        return pd.Series(NA, index=df.index)
    rt = df["race_type"].astype(str)
    sex = _parse_sex(df[ResultsCols.SEX_AGE])
    combo = rt + "_" + sex.astype(str)
    combo[sex.to_numpy() == NA] = NA
    return combo.to_numpy()


def f_career(df: pd.DataFrame) -> pd.Series:
    """キャリア（出走回数帯）。featured に出走回数列がある場合のみ（無ければ na）。
    候補列名を順に探す。"""
    col = next((c for c in ("n_races", "career", "出走回数", "race_count") if c in df.columns), None)
    if col is None:
        return pd.Series(NA, index=df.index)
    c = _num(df[col])
    out = pd.cut(c, [-1, 0, 3, 8, np.inf], labels=["debut", "few", "mid", "many"])
    return out.astype(object).fillna(NA).to_numpy()


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
}


def buckets(df: pd.DataFrame, names: list[str] | None = None) -> pd.DataFrame:
    """指定因子（既定=全因子）のバケットラベルを列に持つ DataFrame を返す。index は df と同一。"""
    names = names or list(FACTORS)
    out = {}
    for name in names:
        vals = FACTORS[name](df)
        out[name] = pd.Series(vals, index=df.index).astype(object).fillna(NA)
    return pd.DataFrame(out, index=df.index)
