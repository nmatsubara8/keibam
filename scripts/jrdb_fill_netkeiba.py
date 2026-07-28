"""JRDB で netkeiba の欠損年（2021-2022 中央）を補完する fill スクリプト（dry-run 既定）。

raw_jrdb_sed → adapters → netkeiba raw スキーマへ整形し、既存 netkeiba raw pickle
（results/race_info/horse_results）に **新規 race_id / (horse_id,日付) だけ**を union する。
NAR は JRDB に無いので触らず、既存年も上書きしない。

使い方:
  # まず dry-run（何行増えるか・列整合を確認。書き込まない）
  python scripts/jrdb_fill_netkeiba.py --year 2021 --year 2022
  # 問題なければ実 union（元 pickle は .bak にバックアップ）
  python scripts/jrdb_fill_netkeiba.py --year 2021 --year 2022 --apply
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.constants._local_paths import LocalPaths  # noqa: E402
from src.jrdb._crosswalk import read_crosswalk  # noqa: E402
from src.jrdb._fill import (  # noqa: E402
    build_fill_tables,
    filter_years,
    new_by_race_id,
    new_horse_results,
)
from src.storage._db import get_engine  # noqa: E402

_PATHS = {
    "results": LocalPaths.RAW_RESULTS_PATH,
    "race_info": LocalPaths.RAW_RACE_INFO_PATH,
    "horse_results": LocalPaths.RAW_HORSE_RESULTS_PATH,
}


def _parse_args(argv):
    ap = argparse.ArgumentParser(description="JRDB で netkeiba 欠損年を補完（dry-run 既定）")
    ap.add_argument("--year", action="append", default=None, metavar="YYYY",
                    help="補完対象年（results/race_info）。複数可。例 --year 2021 --year 2022")
    ap.add_argument("--db", default=None, help="SQLite パス（既定 LocalPaths.DB_PATH）")
    ap.add_argument("--apply", action="store_true",
                    help="実際に pickle へ union（既定は dry-run で書き込まない）")
    return ap.parse_args(argv)


def _load_existing(name):
    """既存 netkeiba raw pickle を読む（無ければ None）。"""
    p = Path(_PATHS[name])
    if not p.exists():
        return None
    return pd.read_pickle(p)


def _existing_race_ids(df):
    if df is None:
        return set()
    if df.index.name == "race_id":
        return set(df.index.astype(str))
    if "race_id" in df.columns:
        return set(df["race_id"].astype(str))
    return set()


def _existing_hr_keys(df):
    if df is None or "日付" not in df.columns:
        return set()
    hid = df.index.astype(str) if df.index.name == "horse_id" else df.get("horse_id")
    if hid is None:
        return set()
    return set(zip(pd.Series(hid).astype(str), df["日付"].astype(str), strict=False))


def main(argv=None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    years = args.year or ["2021", "2022"]
    engine = get_engine(args.db)

    print(f"[fill] 対象年 {years}（{'APPLY' if args.apply else 'DRY-RUN'}）")
    sed = pd.read_sql(text("SELECT * FROM raw_jrdb_sed"), engine)
    print(f"[fill] raw_jrdb_sed 読込 {len(sed):,} 行")
    built = build_fill_tables(sed, jockey_xwalk=read_crosswalk("jockey", db_path=args.db),
                              trainer_xwalk=read_crosswalk("trainer", db_path=args.db))

    existing = {n: _load_existing(n) for n in _PATHS}
    ex_res_ids = _existing_race_ids(existing["results"])
    ex_ri_ids = _existing_race_ids(existing["race_info"])
    ex_hr_keys = _existing_hr_keys(existing["horse_results"])

    # results / race_info: 対象年 かつ 既存に無い race_id
    res_new = new_by_race_id(filter_years(built["results"], years), ex_res_ids)
    ri_new = new_by_race_id(filter_years(built["race_info"], years), ex_ri_ids)
    # horse_results: 全年から 既存に無い (horse_id,日付)
    hr_new = new_horse_results(built["horse_results"], ex_hr_keys)

    print("\n[fill] 追加候補（新規のみ）:")
    print(f"  results:       {len(res_new):>8,} 行（対象年・既存 {len(ex_res_ids):,} と重複除外）")
    print(f"  race_info:     {len(ri_new):>8,} 行")
    print(f"  horse_results: {len(hr_new):>8,} 行（全年・既存 {len(ex_hr_keys):,} と重複除外）")

    # 列整合チェック（既存 netkeiba と生成側の列差）
    for name, new_df in (("results", res_new), ("race_info", ri_new),
                         ("horse_results", hr_new)):
        ex = existing[name]
        if ex is not None and not new_df.empty:
            excols = set(ex.columns) | ({ex.index.name} if ex.index.name else set())
            newcols = set(new_df.columns) | ({new_df.index.name} if new_df.index.name else set())
            only_nk = sorted(excols - newcols)
            only_jr = sorted(newcols - excols)
            print(f"\n  [{name}] 列差: netkeiba のみ={only_nk[:12]}"
                  + (" …" if len(only_nk) > 12 else ""))
            if only_jr:
                print(f"            JRDB のみ={only_jr}")

    if not args.apply:
        print("\n[fill] DRY-RUN 完了（書き込みなし）。問題なければ --apply で union。")
        return 0

    for name, new_df in (("results", res_new), ("race_info", ri_new),
                         ("horse_results", hr_new)):
        if new_df.empty:
            continue
        p = Path(_PATHS[name])
        ex = existing[name]
        if p.exists():
            shutil.copy2(p, str(p) + ".bak")   # 元をバックアップ
        merged = pd.concat([ex, new_df]) if ex is not None else new_df
        p.parent.mkdir(parents=True, exist_ok=True)
        merged.to_pickle(p)
        print(f"[fill] {name}: +{len(new_df):,} → {len(merged):,} 行 保存（{p.name}, 元は .bak）")
    print("[fill] APPLY 完了。featured を再生成すると対象年が含まれます。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
