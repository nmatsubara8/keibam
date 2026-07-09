"""系統(父系)分類のカバレッジを、取得済み featured 上で計測する診断スクリプト。

種牡馬名列を自動検出し、シード(＋あればJRDB由来)マップで大系統に分類できた割合と、
未分類で件数の多い種牡馬 上位を出す。→ シードを拡張すべき種牡馬が一目で分かる。

実行: python scripts/keito_coverage.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.features._sire_line import coverage  # noqa: E402
from src.policies._manji_factors import _SIRE_COLS  # noqa: E402


def main() -> int:
    from app._model_eval import load_featured_data

    featured = load_featured_data()
    if featured is None or featured.empty:
        print("featured_data がありません")
        return 1
    col = next((c for c in _SIRE_COLS if c in featured.columns), None)
    if col is None:
        print(f"種牡馬名の列が featured にありません（探索: {_SIRE_COLS}）。")
        print("→ 血統の系統分類には種牡馬名列が必要。build_seed_featured で保持する必要があります。")
        print(f"  現在の列（先頭50）: {list(featured.columns)[:50]}")
        return 1

    cov = coverage(featured[col].tolist())
    print(f"種牡馬名列: '{col}'  行数: {cov['n']:,}")
    print(f"系統分類カバレッジ: {cov['classified']:,} / {cov['n']:,} = {cov['rate']:.1%}")
    print("\n未分類で件数の多い種牡馬（シード拡張候補・上位30）:")
    for name, cnt in cov["unmapped_top"]:
        print(f"  {cnt:>7,}  {name}")
    print("\n※ カバレッジが低ければ sire_to_keito_seed.tsv を拡張、"
          "または JRDB データ本体の系統コードで sire_to_keito.tsv を生成してください。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
