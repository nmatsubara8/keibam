"""血統(peds.pkl) の対象範囲・キー整合の監査（読み取り専用・非破壊・モデル結果非参照）。

peds.pkl が 824 行しかなく featured は 669,034 馬行 — これが「join バグ」なのか
「ソース部分取得(SOURCE_PARTIAL)」なのかを、単なる交差率でなく **ID 形式の原因診断**まで
含めて切り分けるための純粋関数群。CLI 配線は ルートの ``audit_peds_coverage.py``。

正規化後に coverage が上がっても**その場で join 規則を採用しない**（設計書へ固定してから
materialize する）ための診断であり、materialize 判断そのものは行わない。
"""

from __future__ import annotations

from collections import Counter
from typing import Optional

import pandas as pd


def _as_str(series: pd.Series) -> pd.Series:
    """欠損除去 + ".0" 落ち回避の正準文字列。"""
    s = series.dropna()
    out = s.astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    return out[(out != "") & (out.str.lower() != "nan")]


def _to_int_str(series: pd.Series) -> pd.Series:
    """数値化して int の文字列へ（leading zero を消す正規化）。数値化不可は除外。"""
    n = pd.to_numeric(_as_str(series), errors="coerce").dropna()
    return n.astype("int64").astype(str)


def id_profile(series: pd.Series, name: str) -> dict:
    """ID 列の形状（件数・unique・桁分布・leading zero・dtype・例）。"""
    vals = _as_str(series)
    uniq = vals.drop_duplicates()
    return {
        "name": name,
        "rows": int(len(series)),
        "nonnull_rows": int(len(vals)),
        "unique": int(len(uniq)),
        "len_dist": dict(sorted(Counter(int(len(v)) for v in uniq).items())),
        "has_leading_zero": bool(uniq.str.startswith("0").any()),
        "dtype": str(series.dtype),
        "examples": list(uniq.head(10)),
    }


def coverage(feat_ids: pd.Series, peds_ids: pd.Series) -> dict:
    """完全一致 coverage と、数値正規化(leading zero 吸収)後の coverage を分けて返す。"""
    def _cov(left: pd.Series, rset: set) -> dict:
        luniq = left.drop_duplicates()
        return {
            "row_coverage": float(left.isin(rset).mean()) if len(left) else 0.0,
            "unique_coverage": float(luniq.isin(rset).mean()) if len(luniq) else 0.0,
            "matched_unique": int(luniq.isin(rset).sum()),
        }
    raw = _cov(_as_str(feat_ids), set(_as_str(peds_ids)))
    norm = _cov(_to_int_str(feat_ids), set(_to_int_str(peds_ids)))
    return {"exact": raw, "numeric_normalized": norm}


def year_coverage(feat: pd.DataFrame, peds_ids: pd.Series, *, id_col: str,
                  year_col: str) -> dict:
    """featured 側レース年ごとの peds 一致率（部分取得が特定世代に偏っていないか）。"""
    # featured の index は race_id が重複するため位置基準へ振り直す
    f = feat[[id_col, year_col]].copy().reset_index(drop=True)
    ids = _as_str(f[id_col].astype("object"))
    f = f.loc[ids.index]
    f[id_col] = ids
    f[year_col] = pd.to_numeric(f[year_col], errors="coerce")
    f = f.dropna(subset=[year_col])
    f[year_col] = f[year_col].astype(int)
    pset = set(_as_str(peds_ids))
    if len(f) == 0:
        return {}
    f["_hit"] = f[id_col].isin(pset)
    by = f.groupby(year_col)["_hit"].mean().round(4)
    return {int(k): float(v) for k, v in by.items()}


def peds_integrity(peds: pd.DataFrame, *, id_col: str, sire_col: Optional[str],
                   damsire_col: Optional[str]) -> dict:
    """peds 内部の重複・1対多・父母競合・sire/damsire 非欠損率。"""
    out: dict = {}
    ids = _as_str(peds[id_col].astype("object")) if id_col in peds.columns else _as_str(
        pd.Series(peds.index))
    out["rows"] = int(len(peds))
    out["unique_horse"] = int(ids.nunique())
    out["duplicate_horse_rows"] = int(len(ids) - ids.nunique())
    # 同一 horse に複数行 → 1対多
    vc = ids.value_counts()
    out["one_to_many_horses"] = int((vc > 1).sum())
    # 父/母父が同一馬内で競合しているか
    for lbl, col in (("sire", sire_col), ("damsire", damsire_col)):
        if col and col in peds.columns:
            nn = peds[col].notna().mean()
            out[f"{lbl}_col"] = col
            out[f"{lbl}_nonnull_rate"] = float(nn)
            tmp = pd.DataFrame({"_id": ids.values,
                                "_v": peds.loc[ids.index, col].astype(str).values})
            conflict = tmp.groupby("_id")["_v"].nunique()
            out[f"{lbl}_conflicting_horses"] = int((conflict > 1).sum())
        else:
            out[f"{lbl}_col"] = None
    return out


def unmatched_examples(feat_ids: pd.Series, peds_ids: pd.Series, top: int = 30) -> list:
    """peds に無い featured horse_id の例（ID 形式の相違を目視するため）。"""
    fvals = _as_str(feat_ids)
    pset = set(_as_str(peds_ids))
    miss = fvals[~fvals.isin(pset)].drop_duplicates()
    return list(miss.head(top))
