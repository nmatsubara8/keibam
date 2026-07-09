"""卍因子が「あなたのfeaturedで実際に発火しているか」を可視化する診断。

各因子の非NA率（＝その因子がデータから値を取れている割合）と、代表バケットを出す。
非NA率が0%の因子＝列名不一致で死んでいる → 配線 or データ側の対応が必要。
全列名もダンプするので、芝ダ/性別/距離変更/前走着順などの実列名を特定できる。

実行: python scripts/manji_factor_coverage.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.policies._manji_factors import NA, FACTORS, buckets  # noqa: E402


def main() -> int:
    from app._model_eval import load_featured_data

    featured = load_featured_data()
    if featured is None or featured.empty:
        print("featured_data がありません")
        return 1

    n = len(featured)
    print(f"featured: {n:,} 行 / {len(featured.columns)} 列\n")
    bk = buckets(featured)
    print(f"{'因子':<18}{'非NA率':>8}{'種類':>6}  代表バケット(件数上位)")
    print("-" * 78)
    dead = []
    for f in FACTORS:
        col = bk[f]
        nonna = (col != NA).mean()
        nuniq = col[col != NA].nunique()
        top = col[col != NA].value_counts().head(4)
        tops = " ".join(f"{k}:{v/n:.0%}" for k, v in top.items())
        mark = "  ← 死(列名不一致?)" if nonna == 0 else ""
        print(f"{f:<18}{nonna:>8.1%}{nuniq:>6}  {tops}{mark}")
        if nonna == 0:
            dead.append(f)

    print("\n" + "=" * 78)
    if dead:
        print(f"発火していない因子（{len(dead)}）: {dead}")
        print("→ 実列名に配線するか、その列を featured に保持する必要があります。")
    print("\n[全列名]")
    cols = list(featured.columns)
    for i in range(0, len(cols), 6):
        print("  " + " | ".join(cols[i:i + 6]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
