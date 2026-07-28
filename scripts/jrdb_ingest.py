"""JRDB ファイル群を SQLite に冪等取込する（重複ロード防止つき）。

`src/jrdb/_store.py` の `JrdbStore` を使い、2 軸の重複防止で raw JRDB を永続化する:
  ① 既存データ内の重複排除 = 主キー (race_id, umaban) + keep-last upsert
  ② 新規ロード時の重複チェック = 処理済みファイル台帳（sha1）で未取込分だけ処理

netkeiba の raw テーブルとは別テーブル（raw_jrdb_*）なので同種データでも衝突しない。
`jrdb_build_features.py`（featured 付与）とは役割が別（こちらは raw の恒久蓄積）。

使い方:
  python scripts/jrdb_ingest.py --jrdb-dir /mnt/c/Users/.../jrdb
  python scripts/jrdb_ingest.py --jrdb-dir DIR --force        # 台帳無視で全再取込
  python scripts/jrdb_ingest.py --jrdb-dir DIR --db data/keibam.db
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.jrdb import JrdbStore  # noqa: E402


def _parse_args(argv):
    ap = argparse.ArgumentParser(description="JRDB ファイルを SQLite に冪等取込（重複防止つき）")
    ap.add_argument("--jrdb-dir", required=True,
                    help="KYI/SED/SKB/TYB の .txt/.zip/.lzh を置いたフォルダ")
    ap.add_argument("--extract-to", default="data/jrdb_txt",
                    help="アーカイブ展開先（.lzh/.zip → .txt）")
    ap.add_argument("--db", default=None, help="SQLite パス（既定 LocalPaths.DB_PATH）")
    ap.add_argument("--force", action="store_true",
                    help="処理済みファイル台帳を無視して全ファイルを再取込（訂正の強制反映）")
    ap.add_argument("--allow-length-mismatch", action="store_true",
                    help="レコード長が仕様と乖離するファイル（フォーマット版差の疑い）も取り込む")
    return ap.parse_args(argv)


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = _parse_args(argv if argv is not None else sys.argv[1:])

    store = JrdbStore(db_path=args.db)
    summary = store.ingest_dir(args.jrdb_dir, extract_to=args.extract_to, force=args.force,
                               allow_length_mismatch=args.allow_length_mismatch)

    print("\n[JRDB 取込サマリ]（file=新規取込 / skip=台帳一致 / badlen=版差スキップ / rows=書込行数）")
    total_files = total_skip = total_bad = total_rows = 0
    for rt in ("KYI", "SED", "SKB", "TYB", "CYB", "CHA", "HJC", "KKA", "UKC", "SRB",
               "KSA", "CSA", "KTA"):
        s = summary.get(rt, {"files": 0, "skipped": 0, "badlen": 0, "rows": 0})
        print(f"  {rt}: file={s['files']:>4}  skip={s['skipped']:>4}  "
              f"badlen={s.get('badlen', 0):>3}  rows={s['rows']:>8,}")
        total_files += s["files"]
        total_skip += s["skipped"]
        total_bad += s.get("badlen", 0)
        total_rows += s["rows"]
    print(f"  ---\n  計: file={total_files}  skip={total_skip}  badlen={total_bad}  rows={total_rows:,}")
    if total_bad:
        print(f"  ⚠️ {total_bad} ファイルがレコード長不一致でスキップされました"
              "（古い年度パック等。取り込むなら --allow-length-mismatch）。")
    if total_files == 0 and total_skip == 0 and total_bad == 0:
        print("  JRDB ファイルが見つかりません（KYI/SED/SKB/TYB の .txt/.zip/.lzh）。")
        return 1
    print("\n蓄積先テーブル: raw_jrdb_{kyi,sed,skb,tyb,cyb,cha,hjc,kka,ukc,srb,ksa,csa,kta}"
          "（台帳: jrdb_ingested_files）。再実行しても同一ファイルは skip され重複しません。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
