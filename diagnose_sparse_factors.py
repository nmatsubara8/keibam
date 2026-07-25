"""スパース因子(race_class/leg_type/pace_pressure)の欠損を年度別に可視化する。

仮説: 1986-2022=アーカイブ取込(horse_results無し・格パース無し)／2023-2026=netkeiba生
（horse_results・格あり）の二層構造で、netkeiba固有列が最近しか埋まらない。

featured の各ソース列の非null率を年度(race_id先頭4桁)別に出す。二層なら最近年だけ100%近い。

実行: python diagnose_sparse_factors.py
"""
from __future__ import annotations

import pandas as pd

from app._model_eval import load_featured_data
from src.constants._local_paths import LocalPaths


def _year(idx) -> pd.Series:
    return pd.Series(idx.astype(str)).str[:4]


def _nonnull_by_year(df: pd.DataFrame, col_mask: pd.Series, year: pd.Series, label: str) -> None:
    t = pd.DataFrame({"year": year.to_numpy(), "ok": col_mask.to_numpy().astype(int)})
    t = t[t["year"].str.fullmatch(r"\d{4}").fillna(False)]
    g = t.groupby("year")["ok"].agg(["mean", "size"])
    print(f"\n### {label}  （非null率 by 年度）")
    for y, r in g.iterrows():
        bar = "#" * int(r["mean"] * 30)
        print(f"  {y}  {r['mean'] * 100:5.1f}%  ({int(r['size']):>6,})  {bar}")


def main() -> None:
    f = load_featured_data()
    if f is None or f.empty:
        print("featured がありません")
        return
    year = _year(f.index)
    print(f"featured {len(f):,} 行 / 年度 {year.min()}–{year.max()}")

    # race_class: one-hot race_class__* のいずれかが 1
    rc_cols = [c for c in f.columns if str(c).startswith("race_class__")]
    if rc_cols:
        rc_ok = (f[rc_cols].fillna(0).to_numpy().sum(axis=1) > 0)
        _nonnull_by_year(f, pd.Series(rc_ok, index=f.index), year, f"race_class（one-hot {len(rc_cols)}列）")
    else:
        # 生 race_class 列 or race_class_level
        for c in ("race_class", "race_class_level"):
            if c in f.columns:
                _nonnull_by_year(f, f[c].notna(), year, f"{c}")
                break
        else:
            print("\n### race_class: 列が見つからない")

    # leg_type_binary
    if "leg_type_binary" in f.columns:
        _nonnull_by_year(f, f["leg_type_binary"].notna(), year, "leg_type_binary")
    else:
        print("\n### leg_type_binary: 列が見つからない")

    # 対照: 単勝(常に有る)、horse_id 桁数
    from src.constants._results_cols import ResultsCols
    if ResultsCols.TANSHO_ODDS in f.columns:
        _nonnull_by_year(f, f[ResultsCols.TANSHO_ODDS].notna(), year, "単勝(対照・常時)")
    if "horse_id" in f.columns:
        hid = f["horse_id"].astype(str)
        print("\n### horse_id の桁数分布（8桁=archive想定 / 10桁=netkeiba想定）")
        print(hid.str.len().value_counts().sort_index().to_string())

    # 生 horse_results の年度カバレッジ（通過の有無＝leg_type の源）
    import os
    hrp = LocalPaths.RAW_HORSE_RESULTS_PATH
    if os.path.exists(hrp):
        hr = pd.read_pickle(hrp)
        print(f"\n### horse_results 生データ: {len(hr):,} 行")
        if "date" in hr.columns:
            hy = pd.to_datetime(hr["date"], errors="coerce").dt.year
            print("  年度別 行数（末尾10年）:")
            print(hy.value_counts().sort_index().tail(10).to_string())
        col_tsuka = next((c for c in ("通過", "corner", "CORNER") if c in hr.columns), None)
        if col_tsuka:
            print(f"  通過({col_tsuka}) 非null率: {hr[col_tsuka].notna().mean() * 100:.1f}%")
    else:
        print(f"\n### horse_results: ファイルなし（{hrp}）＝leg_type の源が空")


if __name__ == "__main__":
    main()
