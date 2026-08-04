#!/usr/bin/env python
"""馬主 ID 空間の不一致を監査する診断スクリプト（読み取り専用・非破壊・モデル結果非参照）。

featured/results.owner_id と person_yearly の owner entity_id が別コード体系で join がほぼ
死ぬ問題を、3つの ID 空間で切り分ける:
  (1) results.owner_id   … 現行 DataMerger の join キー
  (2) horse_info.owner_id … netkeiba db owner ID（DataMerger が drop している側）
  (3) person_yearly.entity_id(owner)

使い方:
  python audit_owner_namespace.py                 # 既定 pkl を読み監査
  python audit_owner_namespace.py --top 30
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO_ROOT = str(Path(__file__).resolve().parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import pandas as pd  # noqa: E402

from src.constants._local_paths import LocalPaths  # noqa: E402
from src.preprocessing._owner_namespace_audit import (  # noqa: E402
    bridge_via_horse_info, exact_match, id_space_profile, name_id_consistency,
    results_owner_temporal_variability, unmatched_top, year_join_coverage,
)


def _load(path: str) -> pd.DataFrame:
    try:
        from src.pipeline._ingestion import load_raw
        return load_raw(path)
    except Exception:
        try:
            return pd.read_pickle(path)
        except Exception:
            return pd.DataFrame()


def _featured_year(feat: pd.DataFrame) -> pd.Series:
    """featured からレース年を推定（date → race_id 先頭4桁 → year 列）。"""
    for col in ("date", "日付"):
        if col in feat.columns:
            y = pd.to_datetime(feat[col], errors="coerce").dt.year
            if y.notna().any():
                return y
    if "year" in feat.columns:
        return pd.to_numeric(feat["year"], errors="coerce")
    idx = feat.index.astype(str) if feat.index.name in (None, "race_id") else pd.Series(
        feat.index.astype(str))
    y = pd.to_numeric(idx.str[:4], errors="coerce")
    return pd.Series(y, index=feat.index)


def _owner_entity(py: pd.DataFrame) -> pd.DataFrame:
    """person_yearly から owner 行を entity_id/year で取り出す（index 揺れを吸収）。"""
    if py is None or py.empty:
        return pd.DataFrame(columns=["entity_id", "year"])
    df = py.copy()
    if "entity_id" not in df.columns:
        df = df.reset_index()
    if "entity_type" in df.columns:
        df = df[df["entity_type"] == "owner"]
    keep = [c for c in ("entity_id", "year") if c in df.columns]
    return df[keep] if keep else pd.DataFrame(columns=["entity_id", "year"])


def _pp(title: str, d: dict) -> None:
    print(f"\n=== {title} ===")
    for k, v in d.items():
        print(f"  {k}: {v}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--featured", default=LocalPaths.FEATURED_DATA_PATH)
    ap.add_argument("--horse-info", default=LocalPaths.RAW_HORSE_INFO_PATH)
    ap.add_argument("--person-yearly", default=LocalPaths.RAW_PERSON_YEARLY_PATH)
    ap.add_argument("--top", type=int, default=30)
    args = ap.parse_args()

    feat = _load(args.featured)
    hinfo = _load(args.horse_info)
    py = _owner_entity(_load(args.person_yearly))

    if feat.empty:
        print(f"[FATAL] featured 読込不可: {args.featured}")
        return 2
    print(f"featured rows={len(feat):,}  horse_info rows={len(hinfo):,}  "
          f"person_yearly(owner) rows={len(py):,}")

    # --- ID 空間プロファイル（3系） ---
    if "owner_id" in feat.columns:
        _pp("(1) results.owner_id 形状", id_space_profile(feat["owner_id"]))
    else:
        print("\n[WARN] featured に owner_id 列なし")
    if "owner_id" in hinfo.columns:
        _pp("(2) horse_info.owner_id 形状(db owner ID)", id_space_profile(hinfo["owner_id"]))
    else:
        print("\n[WARN] horse_info に owner_id 列なし")
    if not py.empty:
        _pp("(3) person_yearly.entity_id(owner) 形状", id_space_profile(py["entity_id"]))
    else:
        print("\n[WARN] person_yearly に owner 行なし")

    # --- ID 完全一致率（どの空間が person_yearly と合うか） ---
    if not py.empty and "owner_id" in feat.columns:
        _pp("一致率 (1) results.owner_id → person_yearly",
            exact_match(feat["owner_id"], py["entity_id"]))
    if not py.empty and "owner_id" in hinfo.columns:
        _pp("一致率 (2) horse_info.owner_id → person_yearly",
            exact_match(hinfo["owner_id"], py["entity_id"]))

    # --- 年 join を ID とは分離して測る（現行キーで） ---
    feat2 = feat.copy()
    feat2["_yr"] = _featured_year(feat2)
    if not py.empty and "owner_id" in feat.columns:
        _pp("年 join 分離 (results.owner_id)",
            year_join_coverage(feat2, py, id_col="owner_id", year_col="_yr"))

    # --- horse_id → horse_info.owner_id → person_yearly ブリッジ（行重み・前年込み・年別・競合） ---
    if not py.empty and not hinfo.empty:
        _pp("ブリッジ (horse_id→horse_info.owner_id→person_yearly)",
            bridge_via_horse_info(feat2, hinfo, py, year_col="_yr"))

    # --- results.owner_id の時点依存性（race-time 馬主 か static か） ---
    _pp("results.owner_id 時系列変動(馬内で年により変わるか)",
        results_owner_temporal_variability(feat))
    if not hinfo.empty and "owner_id" in hinfo.columns:
        _hid = "horse_id" if "horse_id" in hinfo.columns else hinfo.index.name or "index"
        print(f"  ※ horse_info は馬マスタ(1馬1行想定)＝owner_id は『現在/最終馬主』の疑い。"
              f"静的ブリッジは過去年へ現在馬主を適用する時点誤りに注意（bridge の "
              f"horses_with_multiple_owner_in_horse_info と併読）。")

    # --- 名前照合（horse_info 側のみ可能。person_yearly は名前列を持たない） ---
    print("\n=== 名前照合の可否 ===")
    print("  person_yearly.pkl は entity_name を持たない → person_yearly 側の名前照合は不可。")
    if "馬主" in hinfo.columns and "owner_id" in hinfo.columns:
        _pp("horse_info 名前↔owner_id 整合(db 空間)",
            name_id_consistency(hinfo["馬主"], hinfo["owner_id"]))
    else:
        print("  horse_info に 馬主 名列なし → 名前↔ID 整合も測れない（生成側で名前保持が必要）。")

    # --- unmatched 上位（現行キー） ---
    if not py.empty and "owner_id" in feat.columns:
        print(f"\n=== unmatched 上位 {args.top} (results.owner_id ∉ person_yearly) ===")
        for oid, n in unmatched_top(feat["owner_id"], py["entity_id"], args.top):
            print(f"  {n:>7,}  {oid!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
