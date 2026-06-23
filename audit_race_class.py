#!/usr/bin/env python
"""レースクラス分類の網羅性を取得データに対して監査する診断スクリプト。

データ取得後に実行し、`classify_race_class` が「判定不能(None)」に落とした生テキストを
出現回数つきで洗い出す。出てきた値を `src/constants/_master.py` の分類規則
（_RACE_CONDITION_RULES / グレード正規表現 / (L) 等）へ追加していくことで、
分類網羅性を実データ駆動で高める。

入力源（永続化されている生テキスト）:
  - horse_results の「レース名」: 全年代・グレード括弧込み。最良の診断ソース
  - race_info: 取得時に分類済みの race_class が NaN の行を、保存済み race_condition で診断
    （レース名=タイトルは race_info には保存していないため race_condition で代替）

使い方:
  python audit_race_class.py                 # 既定の pkl を読み両ソースを監査
  python audit_race_class.py --top 50        # 判定不能の上位 50 件を表示
  python audit_race_class.py --horse-results data/raw/horse_results.pkl
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO_ROOT = str(Path(__file__).resolve().parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import pandas as pd  # noqa: E402

from src.constants._horse_results_cols import HorseResultsCols  # noqa: E402
from src.constants._local_paths import LocalPaths  # noqa: E402
from src.preprocessing._race_class_audit import coverage_summary  # noqa: E402
from src.preprocessing._race_class_audit import unclassified_counts  # noqa: E402


def _load(path: str) -> pd.DataFrame:
    """pkl → DB フォールバックで読み込む。無ければ空 DataFrame。"""
    from src.pipeline._ingestion import load_raw

    return load_raw(path)


def _print_audit(title: str, values, top: int) -> None:
    summ = coverage_summary(values)
    print(f"\n=== {title} ===")
    print(
        f"  総数={summ['total']:,} / 評価対象(非空)={summ['evaluated']:,} / "
        f"分類済み={summ['classified']:,} / 判定不能={summ['unclassified']:,} / "
        f"網羅率={summ['coverage'] * 100:.2f}%  (欠損={summ['blank']:,})"
    )
    counts = unclassified_counts(values)
    if not counts:
        print("  判定不能: なし ✅")
        return
    print(f"  判定不能の実値（上位 {top}・出現回数降順）:")
    for text, n in counts.most_common(top):
        print(f"    {n:>7,}  {text!r}")
    if len(counts) > top:
        print(f"    … 他 {len(counts) - top:,} 種類")


def main() -> None:
    ap = argparse.ArgumentParser(description="レースクラス分類の網羅性監査")
    ap.add_argument("--top", type=int, default=30, help="判定不能の表示件数（既定 30）")
    ap.add_argument("--horse-results", default=LocalPaths.RAW_HORSE_RESULTS_PATH)
    ap.add_argument("--race-info", default=LocalPaths.RAW_RACE_INFO_PATH)
    args = ap.parse_args()

    # 1) horse_results の「レース名」（最良ソース）
    hr = _load(args.horse_results)
    if not hr.empty and HorseResultsCols.RACE_NAME in hr.columns:
        _print_audit(
            f"horse_results.{HorseResultsCols.RACE_NAME}",
            hr[HorseResultsCols.RACE_NAME].tolist(),
            args.top,
        )
    else:
        print(f"\n[skip] horse_results が空、または '{HorseResultsCols.RACE_NAME}' 列なし: {args.horse_results}")

    # 2) race_info: race_class が NaN の行を race_condition で診断
    ri = _load(args.race_info)
    if not ri.empty and "race_class" in ri.columns:
        na_mask = ri["race_class"].isna() | (ri["race_class"].astype(str).str.strip() == "")
        n_na = int(na_mask.sum())
        print("\n=== race_info.race_class（取得時に分類済み）===")
        print(f"  総数={len(ri):,} / race_class 未確定(NaN/空)={n_na:,}")
        if n_na and "race_condition" in ri.columns:
            from collections import Counter

            cond = Counter(ri.loc[na_mask, "race_condition"].astype(str).str.strip())
            cond.pop("", None)
            print(f"  未確定行の race_condition（上位 {args.top}）:")
            for text, n in cond.most_common(args.top):
                print(f"    {n:>7,}  {text!r}")
        elif n_na:
            print("  （race_condition 列が無いため内訳を表示できません）")
    else:
        print(f"\n[skip] race_info が空、または 'race_class' 列なし: {args.race_info}")

    print("\n対応方法: 上記の判定不能テキストを src/constants/_master.py の分類規則"
          "（_RACE_CONDITION_RULES / グレード正規表現 / (L)）に追加し、"
          "tests/constants/test_race_class.py に回帰ケースを足してください。")


if __name__ == "__main__":
    main()
