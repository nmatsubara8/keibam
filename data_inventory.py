"""中央(JRA)/地方(NAR) × データ種別 × 年度 の取得状況を棚卸しする。

各 raw データについて race_id から年度(先頭4桁)・開催者(5-6桁の場コード: 01-10=中央/他=地方)を
判定し、ユニークレース数を 年度×中央/地方 で集計して表示する。馬キーのデータ(horse_results/peds)は
総行数・ユニーク馬数のみ。ファイル不在はスキップ。

実行: python data_inventory.py
"""
from __future__ import annotations

import os

import pandas as pd

from src.constants._local_paths import LocalPaths

_CENTRAL = {f"{i:02d}" for i in range(1, 11)}  # 01札幌〜10小倉


def _rid(df: pd.DataFrame) -> pd.Series:
    if "race_id" in df.columns:
        return df["race_id"].astype(str)
    return df.index.to_series().astype(str)


def _org(code: str) -> str:
    if code in _CENTRAL:
        return "中央"
    return "地方" if code.isdigit() else "他"


def _load(path: str):
    if not os.path.exists(path):
        return None
    try:
        return pd.read_pickle(path)
    except Exception as e:  # noqa: BLE001
        print(f"  読込失敗: {path} ({e})")
        return None


def race_keyed(name: str, path: str) -> None:
    df = _load(path)
    print(f"\n### {name}  ({os.path.basename(path)})")
    if df is None:
        print("  ― ファイルなし")
        return
    rid = _rid(df).drop_duplicates()
    year = rid.str[:4]
    org = rid.str[4:6].map(_org)
    t = pd.DataFrame({"year": year, "org": org})
    t = t[t["year"].str.fullmatch(r"\d{4}").fillna(False)]
    piv = t.pivot_table(index="year", columns="org", aggfunc="size", fill_value=0)
    for c in ("中央", "地方", "他"):
        if c not in piv.columns:
            piv[c] = 0
    piv = piv[["中央", "地方", "他"]].sort_index()
    piv["計"] = piv.sum(axis=1)
    total_rows = len(df)
    print(f"  ユニークレース {len(rid):,} / 総行数 {total_rows:,}")
    print(f"  {'年度':<6}{'中央':>10}{'地方':>10}{'他':>8}{'計':>10}")
    for y, r in piv.iterrows():
        print(f"  {y:<6}{int(r['中央']):>10,}{int(r['地方']):>10,}{int(r['他']):>8,}{int(r['計']):>10,}")
    s = piv.sum()
    print(f"  {'合計':<6}{int(s['中央']):>10,}{int(s['地方']):>10,}{int(s['他']):>8,}{int(s['計']):>10,}")


def horse_keyed(name: str, path: str) -> None:
    df = _load(path)
    print(f"\n### {name}  ({os.path.basename(path)})")
    if df is None:
        print("  ― ファイルなし")
        return
    n = len(df)
    hid = None
    if "horse_id" in df.columns:
        hid = df["horse_id"].astype(str).nunique()
    elif df.index.name == "horse_id":
        hid = df.index.astype(str).nunique()
    print(f"  総行数 {n:,}" + (f" / ユニーク馬 {hid:,}" if hid is not None else ""))
    if "date" in df.columns:
        d = pd.to_datetime(df["date"], errors="coerce")
        print(f"  日付範囲 {d.min()} 〜 {d.max()}")


def main() -> None:
    print("=" * 60)
    print("中央(JRA)/地方(NAR) データ取得状況 棚卸し")
    print("=" * 60)

    # レースキー（year×中央/地方 で集計）
    race_keyed("結果(出走・着順)", LocalPaths.RAW_RESULTS_PATH)
    race_keyed("レース情報", LocalPaths.RAW_RACE_INFO_PATH)
    race_keyed("払戻(return_tables)", LocalPaths.RAW_RETURN_TABLES_PATH)
    payoffs_path = os.path.join(LocalPaths.RAW_DIR, "payoffs.pkl")
    if os.path.exists(payoffs_path):
        race_keyed("払戻アーカイブ(payoffs)", payoffs_path)
    race_keyed("オッズスナップショット", LocalPaths.RAW_ODDS_SNAPSHOT_PATH)
    for label, attr in (
        ("調教", "RAW_TRAINING_PATH"),
        ("パドック", "RAW_PADDOCK_PATH"),
        ("厩舎コメント", "RAW_COMMENT_PATH"),
        ("予想印", "RAW_YOSO_MARKS_PATH"),
    ):
        p = getattr(LocalPaths, attr, None)
        if p:
            race_keyed(label, p)

    # 馬キー
    horse_keyed("馬の過去成績(horse_results)", LocalPaths.RAW_HORSE_RESULTS_PATH)
    horse_keyed("馬基本情報(horse_info)", LocalPaths.RAW_HORSE_INFO_PATH)
    horse_keyed("血統(peds)", LocalPaths.RAW_PEDS_PATH)

    # featured（実際に学習で使える最終形）
    fp = LocalPaths.FEATURED_DATA_PATH
    if os.path.exists(fp):
        race_keyed("featured(学習最終形)", fp)

    print("\n" + "=" * 60)
    print("凡例: 中央=場コード01-10 / 地方=それ以外の数字コード(門別30,大井44…) / 他=非数字")
    print("=" * 60)


if __name__ == "__main__":
    main()
