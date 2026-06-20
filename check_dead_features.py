"""死んでいる特徴量（全NaN/定数）の検出 — 重要度0%の根本原因を特定する。

重要度診断で 脚質/ペース・血統/種牡馬 が 0.00% だった。これらが全NaN/定数なら
特徴量として機能していない。featured の該当列の NaN 率・ユニーク数を出し、さらに
元データ（peds の peds_0 / horse_results の ペース）が埋まっているかを確認して、
「データ欠損」か「計算バグ」かを切り分ける。

実行: python check_dead_features.py
"""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)

# §2d 脚質/ペース と §2j 血統/種牡馬 の特徴量列
_PACE = ["pace_median", "leg_type_binary", "pace_at_distance"]
_SIRE = ["sire_win_rate", "sire_avg_rank", "sire_recent_win_rate"]
_COURSE = ["win_rate_at_distance", "avg_rank_at_course_type"]


def _report_cols(df: pd.DataFrame, cols: list[str]) -> None:
    for c in cols:
        if c not in df.columns:
            print(f"    {c:<26} 列なし")
            continue
        s = df[c]
        nan_rate = s.isna().mean()
        nuniq = s.nunique(dropna=True)
        print(f"    {c:<26} NaN率={nan_rate:6.1%}  ユニーク数={nuniq}"
              f"{'  ← 死んでいる' if (nan_rate > 0.99 or nuniq <= 1) else ''}")


def main() -> None:
    from src.constants._logging_config import setup_logging

    setup_logging()
    print("=" * 70)
    print("死んでいる特徴量の検出（脚質/ペース・血統/種牡馬）")
    print("=" * 70)

    from app._model_eval import load_featured_data

    featured = load_featured_data()
    if featured is None or featured.empty:
        logger.error("featured_data が読み込めません")
        return

    print(f"\n■ featured（{len(featured)} 行）の特徴量 NaN 率")
    print("  [§2d 脚質/ペース]")
    _report_cols(featured, _PACE)
    print("  [§2j 血統/種牡馬]")
    _report_cols(featured, _SIRE)
    print("  [§2e 距離/コース（比較用・効いている領域）]")
    _report_cols(featured, _COURSE)

    # 元データの確認: peds の peds_0、horse_results の ペース が埋まっているか
    print("\n■ 元データの充足（NaN/欠損だと特徴量が死ぬ）")
    import os

    from src.constants._local_paths import LocalPaths

    for label, path, col in [
        ("peds.pkl の peds_0(父)", LocalPaths.RAW_PEDS_PATH, "peds_0"),
        ("horse_results.pkl の ペース", LocalPaths.RAW_HORSE_RESULTS_PATH, "ペース"),
    ]:
        if not os.path.exists(path):
            print(f"  {label:<28} ファイルなし（{path}）← 未生成の可能性")
            continue
        try:
            d = pd.read_pickle(path)
        except Exception as e:  # noqa: BLE001
            print(f"  {label:<28} 読込失敗: {e}")
            continue
        if col in d.columns:
            print(f"  {label:<28} 行={len(d)} NaN率={d[col].isna().mean():.1%} "
                  f"ユニーク={d[col].nunique(dropna=True)}")
        else:
            print(f"  {label:<28} 行={len(d)} だが列 '{col}' なし ← 列名違い/パース問題")

    print("\n判定:")
    print(" - 特徴量が全NaN かつ 元データの該当列も空/欠損 → データ未生成（peds/horse_results 再生成で解決）")
    print(" - 元データは埋まっているのに特徴量が全NaN → 計算バグ（merger の該当処理を調査）")
    print("=" * 70)


if __name__ == "__main__":
    main()
