"""raw pickle の主キー重複を keep-last で解消する（doctor の dup ERROR 修復）。

doctor が `dup.raw_results` 等で主キー重複を検出したとき、その pickle を
TABLE_SPECS の主キーで keep-last dedup して修復する。netkeiba 日次取込の再スクレイプ
（同一 race を後日また取得）で (race_id, 馬番) が二重化することがあり、後着＝訂正版なので
keep-last が正しい（JrdbStore.upsert / ingest と同じ方針）。

元の on-disk 構造（index_col が index か通常列か）を保って書き戻す。既定は DRY-RUN、
`--apply` で .bak バックアップを取ってから上書きする。

使い方:
    python scripts/dedup_raw_pickle.py                          # raw_results を点検（DRY-RUN）
    python scripts/dedup_raw_pickle.py --alias raw_results --apply
    python scripts/dedup_raw_pickle.py --alias raw_results --alias raw_horse_results --apply
"""
from __future__ import annotations

import argparse
import logging
import shutil
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.pipeline._duplicate_audit import (  # noqa: E402
    _normalize_index_col,
    count_pk_extras,
    resolve_pk_cols,
)
from src.storage._db import TABLE_SPECS, alias_to_pickle_path  # noqa: E402

logger = logging.getLogger(__name__)


def dedup_frame(df: pd.DataFrame, alias: str):
    """(deduped_df, n_removed, pk) を返す。元の index 構造を保つ。keep-last。

    pk が解決できない（キー列不足）場合は None を返して呼び出し側でスキップさせる。
    """
    spec = TABLE_SPECS[alias]
    orig_index_name = df.index.name
    pk = resolve_pk_cols(df, spec)
    if pk is None:
        return None
    work = _normalize_index_col(df, spec.index_col)   # PK を通常列へ起こす
    n_extra, _ = count_pk_extras(work, pk)
    if n_extra <= 0:
        return df, 0, pk
    deduped = work.drop_duplicates(subset=list(pk), keep="last")
    # 元構造へ戻す: index_col が元々 index だったなら index に戻す。
    if orig_index_name is not None and spec.index_col == orig_index_name:
        deduped = deduped.set_index(spec.index_col)
    else:
        deduped = deduped.reset_index(drop=True)
    return deduped, n_extra, pk


def process(alias: str, *, apply: bool) -> int:
    if alias not in TABLE_SPECS:
        print(f"[dedup] 未知の alias: {alias}（対応: {sorted(TABLE_SPECS)}）", file=sys.stderr)
        return 1
    path = alias_to_pickle_path(alias)
    if not path or not Path(path).exists():
        print(f"[dedup] {alias}: pickle が見つかりません（{path}）。ローカルで実行してください。")
        return 0
    df = pd.read_pickle(path)
    if not isinstance(df, pd.DataFrame) or df.empty:
        print(f"[dedup] {alias}: DataFrame でない/空のためスキップ。")
        return 0

    res = dedup_frame(df, alias)
    if res is None:
        spec = TABLE_SPECS[alias]
        print(f"[dedup] {alias}: 主キー列 {list(spec.primary_key)} が揃わずスキップ。")
        return 0
    deduped, n_removed, pk = res
    print(f"[dedup] {alias}: {len(df):,} 行 / 主キー{list(pk)} 重複 {n_removed:,} 行")
    if n_removed == 0:
        print(f"[dedup] {alias}: 重複なし。修復不要。")
        return 0
    if not apply:
        print(f"[dedup] {alias}: DRY-RUN（--apply で {len(df):,}→{len(deduped):,} 行に修復）。")
        return 0
    shutil.copy2(path, str(path) + ".bak")
    deduped.to_pickle(path)
    print(f"[dedup] {alias}: 修復済み {len(deduped):,} 行を保存（元は {Path(path).name}.bak）。")
    return 0


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    ap = argparse.ArgumentParser(description="raw pickle の主キー重複を keep-last で解消")
    ap.add_argument("--alias", action="append", default=None,
                    help="対象 alias（省略時 raw_results）。複数指定可")
    ap.add_argument("--apply", action="store_true", help="実際に修復して保存（既定は DRY-RUN）")
    args = ap.parse_args(argv if argv is not None else sys.argv[1:])
    aliases = args.alias or ["raw_results"]
    rc = 0
    for a in aliases:
        rc |= process(a, apply=args.apply)
    if not args.apply:
        print("\n[dedup] DRY-RUN 完了。--apply で修復後、doctor を再実行して OK を確認してください。")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
