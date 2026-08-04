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

    # 判定ヒント
    ex = coverage(feat["horse_id"], peds_id)
    ucov = ex["exact"]["unique_coverage"]
    ncov = ex["numeric_normalized"]["unique_coverage"]
    print("\n=== 判定ヒント ===")
    if ucov < 0.01 and ncov > 0.5:
        print("  → 正規化で大幅改善＝KEY_MISMATCH(leading zero/dtype)。規則を設計書へ固定後に materialize。")
    elif max(ucov, ncov) < 0.02:
        print("  → 正規化後も交差ほぼ0＝別キー(血統登録番号 vs horse_id)か SOURCE_PARTIAL。全期間再取得が要る。")
    else:
        print(f"  → 交差 unique_coverage(exact={ucov:.3f}, norm={ncov:.3f})。世代偏りは年別 coverage を参照。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
