"""馬主 ID 空間の不一致監査（読み取り専用・非破壊・モデル結果非参照）。

featured/results の ``owner_id`` と person_yearly の owner ``entity_id`` が別コード体系で
join がほぼ死ぬ（実測一致率〜5.7%）問題を、**ID 空間の不一致**と**名前の曖昧性**に分けて
定量化するための純粋関数群。CLI 配線は ルートの ``audit_owner_namespace.py``。

重要な設計上の制約（永続データの事実）:
- ``person_yearly.pkl`` は entity_id/year/成績のみ＝**名前列を持たない**。よって person_yearly 側
  単独では名前照合できない。
- ``horse_info`` の生データは ``owner_id``（netkeiba db の owner ページ href 由来・6桁 db ID）と
  馬主名の双方を持つ。db owner ID は person_yearly.entity_id と**同じ空間**の可能性が高い。
- 一方 ``results.owner_id``（DataMerger が join に使う側）は別コード体系の疑い（例 000031）。

したがって本監査は **3つの ID 空間**を突き合わせる:
  (1) featured/results.owner_id      … 現行 join のキー
  (2) horse_info.owner_id            … db owner ID（DataMerger が drop している側）
  (3) person_yearly.entity_id(owner) … 年度別成績側

名前正規化は**監査用途に限定**し、自動 materialize しない。法人表記・空白・全半角・旧字体を
吸収した結果、複数 owner が同名化した場合は衝突として明示（fail-closed 候補）。
"""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Iterable, Optional

import pandas as pd

from src.preprocessing._entity_resolver import normalize_breeder_name


def _nonnull_str(series: pd.Series) -> pd.Series:
    """欠損を除き、".0" 落ちを避けた正準文字列 Series（owner_id は素通し正準化）。"""
    s = series.dropna()
    out = s.astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    return out[out != ""]


def id_space_profile(series: pd.Series) -> dict:
    """単一 ID 列の形状プロファイル（非欠損率・unique・桁分布・leading zero・dtype・例）。"""
    total = int(len(series))
    vals = _nonnull_str(series)
    nonnull = int(len(vals))
    uniq = vals.drop_duplicates()
    len_dist = Counter(int(len(v)) for v in uniq)
    has_leading_zero = bool((uniq.str.startswith("0")).any())
    return {
        "total_rows": total,
        "nonnull_rows": nonnull,
        "nonnull_rate": (nonnull / total) if total else 0.0,
        "unique": int(len(uniq)),
        "len_dist": dict(sorted(len_dist.items())),
        "has_leading_zero": has_leading_zero,
        "dtype": str(series.dtype),
        "examples": list(uniq.head(10)),
    }


def exact_match(left: pd.Series, right: pd.Series) -> dict:
    """left の ID が right の ID 集合にどれだけ一致するか（行レベル/unique レベル両方）。"""
    lvals = _nonnull_str(left)
    rset = set(_nonnull_str(right))
    if len(lvals) == 0:
        return {"row_match_rate": 0.0, "unique_match_rate": 0.0,
                "matched_unique": 0, "left_unique": 0, "right_unique": len(rset)}
    row_hits = int(lvals.isin(rset).sum())
    luniq = lvals.drop_duplicates()
    uniq_hits = int(luniq.isin(rset).sum())
    return {
        "row_match_rate": row_hits / len(lvals),
        "unique_match_rate": uniq_hits / len(luniq) if len(luniq) else 0.0,
        "matched_unique": uniq_hits,
        "left_unique": int(len(luniq)),
        "right_unique": len(rset),
    }


def year_join_coverage(feat: pd.DataFrame, py: pd.DataFrame, *, id_col: str,
                       year_col: str, py_id_col: str = "entity_id",
                       py_year_col: str = "year", as_of_lag: int = 1) -> dict:
    """ID 段階と (ID×前年) 段階の一致率を分離して測る。

    as_of_lag=1 は「レース年-1 の年度別成績を as-of で結合」を意味する。
    ID だけの一致で死んでいるのか、年 join で死んでいるのかを切り分ける。
    """
    f = feat[[id_col, year_col]].copy()
    f[id_col] = _nonnull_str(f[id_col].astype("object"))
    f = f.dropna(subset=[id_col, year_col])
    f[year_col] = pd.to_numeric(f[year_col], errors="coerce")
    f = f.dropna(subset=[year_col])
    f[year_col] = f[year_col].astype(int)

    p = py[[py_id_col, py_year_col]].copy()
    pid = _nonnull_str(p[py_id_col].astype("object"))
    p = p.loc[pid.index]
    p[py_id_col] = pid
    p[py_year_col] = pd.to_numeric(p[py_year_col], errors="coerce")
    p = p.dropna(subset=[py_year_col])
    p[py_year_col] = p[py_year_col].astype(int)

    id_set = set(p[py_id_col])
    id_year_set = set(zip(p[py_id_col], p[py_year_col]))
    n = len(f)
    if n == 0:
        return {"rows": 0, "id_match_rate": 0.0, "id_and_year_match_rate": 0.0}
    id_hits = f[id_col].isin(id_set)
    want_year = f[year_col] - as_of_lag
    iy_hits = [(i, y) in id_year_set for i, y in zip(f[id_col], want_year)]
    return {
        "rows": int(n),
        "id_match_rate": float(id_hits.mean()),
        "id_and_year_match_rate": float(pd.Series(iy_hits).mean()),
        "as_of_lag": as_of_lag,
    }


def name_id_consistency(names: Iterable, ids: Iterable) -> dict:
    """名前正規化による名寄せの曖昧性を監査（自動 materialize しない）。

    - 正規化名 → 複数 owner_id  : 別人物が同名化した衝突（fail-closed 候補）
    - owner_id → 複数正規化名    : 表記ゆれ（同一 ID に別名）
    """
    name_to_ids: dict = defaultdict(set)
    id_to_names: dict = defaultdict(set)
    pairs = 0
    for nm, oid in zip(names, ids):
        norm = normalize_breeder_name(nm)
        s = str(oid).strip()
        if not norm or not s or s.lower() == "nan":
            continue
        pairs += 1
        name_to_ids[norm].add(s)
        id_to_names[s].add(norm)
    collide = {n: sorted(ids_) for n, ids_ in name_to_ids.items() if len(ids_) > 1}
    multi = {i: sorted(ns) for i, ns in id_to_names.items() if len(ns) > 1}
    return {
        "pairs": pairs,
        "unique_norm_names": len(name_to_ids),
        "unique_ids": len(id_to_names),
        "name_collisions": len(collide),          # 同名複数ID（衝突）
        "id_alias_spread": len(multi),            # 同一IDに別名
        "collision_examples": dict(list(collide.items())[:15]),
        "alias_examples": dict(list(multi.items())[:15]),
    }


def unmatched_top(left: pd.Series, right: pd.Series, top: int = 30) -> list:
    """right に無い left ID を出現回数つき上位で返す（原因調査の手掛かり）。"""
    lvals = _nonnull_str(left)
    rset = set(_nonnull_str(right))
    miss = lvals[~lvals.isin(rset)]
    return Counter(miss).most_common(top)
