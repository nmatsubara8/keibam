#!/usr/bin/env python
"""血統(peds.pkl) の対象範囲・キー整合を監査（読み取り専用・非破壊・モデル結果非参照）。

peds.pkl(〜824行) が featured(669,034 馬行) に対しどこまで覆えるか、そして不足が
「join バグ」か「ソース部分取得」かを ID 形式診断まで含めて切り分ける。

使い方:
  python audit_peds_coverage.py
  python audit_peds_coverage.py --sire-col peds_0 --damsire-col peds_32 --top 30
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
from src.preprocessing._peds_coverage_audit import (  # noqa: E402
    coverage, id_profile, peds_integrity, unmatched_examples, year_coverage,
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


def _peds_horse_id(peds: pd.DataFrame) -> pd.Series:
    if "horse_id" in peds.columns:
        return peds["horse_id"]
    return pd.Series(peds.index, name="horse_id")


def _featured_year(feat: pd.DataFrame) -> pd.Series:
    for col in ("date", "日付"):
        if col in feat.columns:
            y = pd.to_datetime(feat[col], errors="coerce").dt.year
            if y.notna().any():
                return y
    if "year" in feat.columns:
        return pd.to_numeric(feat["year"], errors="coerce")
    idx = pd.Series(feat.index.astype(str))
    return pd.Series(pd.to_numeric(idx.str[:4], errors="coerce").values, index=feat.index)


def _pp(title: str, d) -> None:
    print(f"\n=== {title} ===")
    if isinstance(d, dict):
        for k, v in d.items():
            print(f"  {k}: {v}")
    else:
        print(f"  {d}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--featured", default=LocalPaths.FEATURED_DATA_PATH)
    ap.add_argument("--peds", default=LocalPaths.RAW_PEDS_PATH)
    ap.add_argument("--sire-col", default="peds_0")
    ap.add_argument("--damsire-col", default="peds_32")
    ap.add_argument("--top", type=int, default=30)
    args = ap.parse_args()

    feat = _load(args.featured)
    peds = _load(args.peds)
    if feat.empty:
        print(f"[FATAL] featured 読込不可: {args.featured}")
        return 2
    if peds.empty:
        print(f"[FATAL] peds 読込不可: {args.peds}")
        return 2
    if "horse_id" not in feat.columns:
        print("[FATAL] featured に horse_id 列なし")
        return 2

    peds_id = _peds_horse_id(peds)
    print(f"featured rows={len(feat):,}  peds rows={len(peds):,}  "
          f"peds cols={list(peds.columns)[:6]}{'...' if peds.shape[1] > 6 else ''}")

    _pp("featured.horse_id 形状", id_profile(feat["horse_id"], "featured.horse_id"))
    _pp("peds.horse_id 形状", id_profile(peds_id, "peds.horse_id"))

    _pp("coverage(完全一致 / 数値正規化後)", coverage(feat["horse_id"], peds_id))

    feat2 = feat.copy()
    feat2["_yr"] = _featured_year(feat2)
    yc = year_coverage(feat2, peds_id, id_col="horse_id", year_col="_yr")
    _pp("年別 coverage(完全一致)", yc if yc else "年推定不可")

    _pp("peds 内部整合(重複/1対多/父母競合/非欠損率)",
        peds_integrity(peds, id_col="horse_id",
                       sire_col=args.sire_col if args.sire_col in peds.columns else None,
                       damsire_col=args.damsire_col if args.damsire_col in peds.columns else None))

    print(f"\n=== unmatched horse_id 例(featured ∉ peds・上位 {args.top}) ===")
    for hid in unmatched_examples(feat["horse_id"], peds_id, args.top):
        print(f"  {hid!r}")

    # 判定（順に評価すると混乱しない）
    ex = coverage(feat["horse_id"], peds_id)
    ucov = ex["exact"]["unique_coverage"]
    ncov = ex["numeric_normalized"]["unique_coverage"]
    integ = peds_integrity(peds, id_col="horse_id",
                           sire_col=args.sire_col if args.sire_col in peds.columns else None,
                           damsire_col=args.damsire_col if args.damsire_col in peds.columns else None)
    sire_nn = integ.get("sire_nonnull_rate")
    conflict = (integ.get("sire_conflicting_horses", 0) or 0) + \
        (integ.get("damsire_conflicting_horses", 0) or 0)
    matched_uniq = ex["numeric_normalized"]["matched_unique"]
    print("\n=== 判定 ===")
    if ucov < 0.01 and ncov > 0.1:
        print(f"  → KEY_FORMAT_MISMATCH（exact≈0・正規化で {ncov:.3f} へ上昇＝leading zero/dtype）。"
              "正規化規則を設計書へ固定してから materialize。")
    elif matched_uniq <= 900 and max(ucov, ncov) < 0.05:
        print(f"  → SOURCE_PARTIAL（正規化後も一致 {matched_uniq} 頭前後＝peds.pkl が部分取得）。"
              "全期間の血統再取得まで本番投入不可。")
    elif max(ucov, ncov) >= 0.5 and sire_nn is not None and sire_nn < 0.05:
        print("  → PROCESSOR_OR_MATERIALIZE_FAILURE（overlap 十分だが sire/damsire がほぼ NaN）。"
              "peds_processor/履歴集約側の不具合。")
    elif conflict > 0:
        print(f"  → SOURCE_CONFLICT（同一 horse に父/母父の複数値 {conflict} 頭）。ソース整合の解決が先。")
    else:
        print(f"  → overlap unique_coverage(exact={ucov:.3f}, norm={ncov:.3f}, matched={matched_uniq})。"
              "年別 coverage で世代/開催偏りを確認（部分ソースは『血統』でなく『取得済みか』を学ぶ）。")
    if yc:
        yrs = sorted(yc)
        print(f"  年別 coverage: 最古={yrs[0]}:{yc[yrs[0]]:.3f} 最新={yrs[-1]}:{yc[yrs[-1]]:.3f}"
              f"（特定世代/開催偏りに注意）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
