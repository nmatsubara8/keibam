"""JRDB ↔ netkeiba の同一性クロスウォーク（血統登録番号/騎手/調教師コードの橋渡し）。

netkeiba の `horse_id`/`jockey_id`/`trainer_id` は 7 桁前後の代理キーで、JRDB の
血統登録番号・騎手コード・調教師コードとは無関係（`scripts/jrdb_bridge_check.py` で
horse_id 一致率 0% を確認済み）。だが **race_id は両者 100% 一致**し、`(race_id, 馬番)`
はレース内で馬を一意に定める。よって **両方のデータがある「重複年」で (race_id, 馬番) を
突き合わせれば、血統登録番号 ↔ horse_id を機械的に対応づけられる**（騎手/調教師も同様）。

用途: JRDB による上書き統合（2002〜）で、pre-JRDB 年（netkeiba のみ）と JRDB 年をまたぐ
馬の履歴を連結する土台。多対一の揺れは多数決で解決し、confidence（最頻票/総票）を残す。
"""
from __future__ import annotations

import logging
from typing import Optional

import pandas as pd
from sqlalchemy import text

from src.storage._db import get_engine

logger = logging.getLogger(__name__)

# (JRDB 側コード列, netkeiba 側 ID 列, 出力名)。既定は馬/騎手/調教師の3種。
CROSSWALK_KEYS: tuple[tuple[str, str, str], ...] = (
    ("ketto", "horse_id", "horse"),
    ("kishu_code", "jockey_id", "jockey"),
    ("chokyo_code", "trainer_id", "trainer"),
)
_XWALK_TABLE = {name: f"jrdb_xwalk_{name}" for _, _, name in CROSSWALK_KEYS}


def _norm_join(df: pd.DataFrame, umaban_col: str) -> pd.DataFrame:
    """race_id（列 or index）と 馬番 を _rid/_uma に正準化した DataFrame を返す。"""
    d = df.copy()
    if "race_id" in d.columns:
        d["_rid"] = d["race_id"].astype(str)
    else:  # netkeiba raw は race_id を index に持つことがある
        d["_rid"] = d.index.astype(str)
    d["_uma"] = pd.to_numeric(d[umaban_col], errors="coerce").astype("Int64")
    return d


def _resolve_one(pairs: pd.DataFrame, jcol: str, ncol: str) -> pd.DataFrame:
    """(jrdb_code, nk_id) の票から、コードごとに最頻 ID を1つ選び confidence を付す。"""
    sub = pairs[[jcol, ncol]].copy()
    for c in (jcol, ncol):
        sub[c] = sub[c].map(lambda v: str(v).strip() if pd.notna(v) else "")
    sub = sub[(sub[jcol] != "") & (sub[ncol] != "")]
    if sub.empty:
        return pd.DataFrame(columns=[jcol, ncol, "support", "total", "confidence"])
    votes = sub.groupby([jcol, ncol]).size().reset_index(name="support")
    total = votes.groupby(jcol)["support"].sum().rename("total")
    # コードごとに support 最大（同数は nk_id 昇順で決定的）を採用
    votes = votes.sort_values([jcol, "support", ncol], ascending=[True, False, True])
    best = votes.drop_duplicates(jcol, keep="first").merge(total, on=jcol)
    best["confidence"] = best["support"] / best["total"]
    return best[[jcol, ncol, "support", "total", "confidence"]].reset_index(drop=True)


def build_crosswalk(
    netkeiba: pd.DataFrame,
    jrdb: pd.DataFrame,
    *,
    keys: tuple[tuple[str, str, str], ...] = CROSSWALK_KEYS,
    umaban_nk: str = "馬番",
    umaban_jr: str = "umaban",
) -> dict[str, pd.DataFrame]:
    """重複年の (race_id, 馬番) 突合から JRDB コード↔netkeiba ID の対応表を作る。

    Parameters
    ----------
    netkeiba : race_id(列 or index)+ 馬番 + horse_id/jockey_id/trainer_id を持つ表
               （通常 raw_results）。
    jrdb     : race_id + umaban + ketto/kishu_code/chokyo_code を持つ表（通常 KYI）。
    keys     : (jrdb_col, netkeiba_col, out_name) の並び。
    Returns  : {out_name: DataFrame[jrdb_col, netkeiba_col, support, total, confidence]}。
               support=最頻票数, total=総票数, confidence=support/total（1.0 で無矛盾）。
    """
    if netkeiba is None or jrdb is None or netkeiba.empty or jrdb.empty:
        return {name: pd.DataFrame() for _, _, name in keys}
    nk = _norm_join(netkeiba, umaban_nk)
    jr = _norm_join(jrdb, umaban_jr)
    merged = jr.merge(nk, on=["_rid", "_uma"], how="inner", suffixes=("_jr", "_nk"))
    out: dict[str, pd.DataFrame] = {}
    for jcol, ncol, name in keys:
        if jcol not in jr.columns or ncol not in nk.columns:
            out[name] = pd.DataFrame()
            continue
        out[name] = _resolve_one(merged, jcol, ncol)
        logger.info("[xwalk] %s: %d コードを対応（重複突合 %d 行）", name, len(out[name]), len(merged))
    return out


def coverage(out: dict[str, pd.DataFrame]) -> dict[str, dict]:
    """各対応表の件数と、多数決で曖昧だった（confidence<1）割合を要約する。"""
    rep: dict[str, dict] = {}
    for name, df in out.items():
        if df is None or df.empty:
            rep[name] = {"mapped": 0, "ambiguous": 0, "ambiguous_rate": 0.0}
            continue
        amb = int((df["confidence"] < 1.0).sum())
        rep[name] = {"mapped": len(df), "ambiguous": amb,
                     "ambiguous_rate": amb / len(df)}
    return rep


def save_crosswalk(out: dict[str, pd.DataFrame], *, db_path: Optional[str] = None) -> None:
    """対応表を jrdb_xwalk_<name>（JRDB コードを主キー）へ INSERT OR REPLACE 保存する。"""
    engine = get_engine(db_path)
    for name, df in out.items():
        if df is None or df.empty:
            continue
        jcol = df.columns[0]
        table = _XWALK_TABLE.get(name, f"jrdb_xwalk_{name}")
        cols = list(df.columns)
        col_sql = ", ".join(f'"{c}"' for c in cols)
        others = ", ".join(f'"{c}" TEXT' for c in cols if c != jcol)
        with engine.begin() as conn:
            conn.execute(text(
                f'CREATE TABLE IF NOT EXISTS "{table}" (\n'
                f'  "{jcol}" TEXT PRIMARY KEY{"," if others else ""}\n'
                f'  {others}\n)'
            ))
            param_sql = ", ".join(f":p{i}" for i in range(len(cols)))
            rows = [{f"p{i}": (str(v) if pd.notna(v) else None) for i, v in enumerate(r)}
                    for r in df.itertuples(index=False, name=None)]
            conn.execute(text(
                f'INSERT OR REPLACE INTO "{table}" ({col_sql}) VALUES ({param_sql})'
            ), rows)
        logger.info("[xwalk] 保存 %s: %d 行", table, len(df))


def read_crosswalk(name: str, *, db_path: Optional[str] = None) -> pd.DataFrame:
    """保存済みの対応表を読み出す（無ければ空 DataFrame）。"""
    engine = get_engine(db_path)
    table = _XWALK_TABLE.get(name, f"jrdb_xwalk_{name}")
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(f'SELECT * FROM "{table}"'), conn)
    except Exception:  # noqa: BLE001 — 未作成テーブル等は空で返す
        return pd.DataFrame()
