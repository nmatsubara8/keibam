"""帯広ばんえい（場コード65）のレースを raw データから除去する（データ清浄化）。

ばんえい（輓馬＝そり引き）は馬場水分%・タイム等が平地と別体系で、平地レース用モデルには
不適。誤って収集された分（--organizer both 等）を results/race_info/return_tables の
各 pkl と HTML bin から除去する。race_id が列でも index でも対応。

既定は dry-run（件数表示のみ・保存しない）。--apply で実際に除去する。

実行:
  python purge_banei.py           # 影響件数の確認（保存しない）
  python purge_banei.py --apply   # 実際に除去（pkl 上書き＋bin 削除）
"""

from __future__ import annotations

import argparse
import glob
import logging
import os

logger = logging.getLogger(__name__)

_BANEI_CODE = "65"  # 帯広ばんえい


def _rid_series(df):
    """race_id を Series で返す（列 race_id 優先、無ければ index）。"""
    if "race_id" in df.columns:
        return df["race_id"].astype(str)
    return df.index.to_series().astype(str)


def main() -> None:
    from src.constants._logging_config import setup_logging

    setup_logging()
    ap = argparse.ArgumentParser(description="帯広ばんえい(場65)を raw から除去")
    ap.add_argument("--apply", action="store_true", help="実際に除去する（既定は dry-run）")
    args = ap.parse_args()

    from src.constants._local_paths import LocalPaths
    from src.pipeline._ingestion import load_raw, save_raw

    targets = [
        ("results", LocalPaths.RAW_RESULTS_PATH),
        ("race_info", LocalPaths.RAW_RACE_INFO_PATH),
        ("return_tables", LocalPaths.RAW_RETURN_TABLES_PATH),
    ]
    total_rows = 0
    for name, path in targets:
        df = load_raw(path)
        if df is None or df.empty:
            print(f"  {name}: 空/無し")
            continue
        rid = _rid_series(df)
        mask = rid.str[4:6] == _BANEI_CODE
        n = int(mask.sum())
        total_rows += n
        n_races = rid[mask].nunique()
        print(f"  {name}: ばんえい {n} 行 / {n_races} レース"
              + ("（除去して保存）" if args.apply and n else "（除去対象）" if n else "（なし）"))
        if args.apply and n:
            save_raw(df[~mask.to_numpy()], path)

    # HTML bin（data/html/race/<race_id>.bin）で場65のものを削除
    banei_bins = [
        f for f in glob.glob(os.path.join("data", "html", "race", "*.bin"))
        if len(os.path.basename(f)) >= 6 and os.path.basename(f)[4:6] == _BANEI_CODE
    ]
    print(f"  bin: ばんえい {len(banei_bins)} 件" + ("（削除）" if args.apply else "（削除対象）"))
    if args.apply:
        for f in banei_bins:
            os.remove(f)

    if args.apply:
        print(f"\n除去完了: raw {total_rows} 行 + bin {len(banei_bins)} 件を削除しました。")
    else:
        print(f"\n[dry-run] 除去対象: raw {total_rows} 行 + bin {len(banei_bins)} 件。"
              "実行するには --apply を付けてください。")


if __name__ == "__main__":
    main()
