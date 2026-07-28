"""JRDB 会員エリアから設定(JSON)に従ってデータを取得し、冪等に取り込む（毎週起動用）。

認証は環境変数 **JRDB_USER / JRDB_PASS**（HTTP Basic 認証）。資格情報はコミット禁止。
`/member/datazip/<型>/index.html` をパースして年度パック/単体 zip を列挙し、未取得分だけ
DL（取得台帳 jrdb_fetched_files）→ `JrdbStore.ingest_dir` で冪等取込（sha1 台帳 + keep-last）。

「取得済み vs 新規」の重複は download 層（url 台帳）と ingest 層（sha1 台帳）の二重で排除する。

使い方（ローカルで実行）:
  export JRDB_USER=xxxx JRDB_PASS=yyyy
  python scripts/jrdb_fetch.py --config configs/jrdb_fetch.example.json
  python scripts/jrdb_fetch.py --config C.json --dry-run                 # DL せず対象一覧のみ
  python scripts/jrdb_fetch.py --config C.json --type TYB --latest 2     # 日次: 最新2件だけ
  python scripts/jrdb_fetch.py --config C.json --refresh                 # 既取得も再取得

毎週 cron 例（土 20:00）:
  0 20 * * 6  cd /path/keibam && JRDB_USER=.. JRDB_PASS=.. \
      python scripts/jrdb_fetch.py --config configs/jrdb_fetch.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.jrdb import JrdbStore  # noqa: E402
from src.jrdb._fetch import JrdbFetcher  # noqa: E402
from src.jrdb._fetch import select_files  # noqa: E402


def _parse_args(argv):
    ap = argparse.ArgumentParser(description="JRDB 会員エリアから取得→冪等取込（設定JSON）")
    ap.add_argument("--config", required=True, help="取得設定 JSON（configs/jrdb_fetch.example.json 参照）")
    ap.add_argument("--type", default=None, help="この型だけ処理（設定の name と一致）")
    ap.add_argument("--since-year", type=int, default=None, help="設定を上書きして年下限を指定")
    ap.add_argument("--latest", type=int, default=None, help="single を新しい順 N 件（日次用）")
    ap.add_argument("--db", default=None, help="SQLite パス（既定 LocalPaths.DB_PATH）")
    ap.add_argument("--dry-run", action="store_true", help="DL せず対象一覧のみ表示")
    ap.add_argument("--refresh", action="store_true", help="取得台帳を無視して再取得")
    ap.add_argument("--allow-length-mismatch", action="store_true",
                    help="レコード長が仕様と乖離するファイル（版差の疑い）も取り込む")
    return ap.parse_args(argv)


def _build_session():
    """Basic 認証済み requests.Session を作る（PHPSESSID Cookie は自動保持）。"""
    import requests  # noqa: PLC0415

    user, pw = os.environ.get("JRDB_USER"), os.environ.get("JRDB_PASS")
    if not user or not pw:
        print("環境変数 JRDB_USER / JRDB_PASS が未設定です（会員ID/パスワード）。", file=sys.stderr)
        raise SystemExit(2)
    s = requests.Session()
    s.auth = (user, pw)
    s.headers.update({"User-Agent": "keibam-jrdb-fetch/1.0"})
    return s


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = _parse_args(argv if argv is not None else sys.argv[1:])

    cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))
    types = [t for t in cfg.get("data_types", []) if t.get("enabled", True)]
    if args.type:
        types = [t for t in types if t.get("name") == args.type]
    if not types:
        print("対象データ型がありません（enabled / --type を確認）。", file=sys.stderr)
        return 1

    prefer = cfg.get("prefer", "zip")

    # dry-run は session だけで index を読む（DL しない）
    session = _build_session()
    store = None if args.dry_run else JrdbStore(db_path=args.db)
    fetcher = JrdbFetcher(
        session, base_url=cfg.get("base_url", "https://jrdb.com/member/datazip"),
        cache_dir=cfg.get("cache_dir", "data/jrdb_dl"), db_path=args.db,
    )

    print(f"[jrdb-fetch] {len(types)} 型を処理" + ("（dry-run）" if args.dry_run else ""))
    grand = {"downloaded": 0, "skipped": 0, "ingested": 0}
    for t in types:
        name, tdir = t["name"], t.get("dir", t["name"].capitalize())
        since = args.since_year if args.since_year is not None else t.get("since_year")
        latest = args.latest if args.latest is not None else t.get("latest")
        kinds = tuple(t.get("kinds", ["year", "single"]))

        if args.dry_run:
            files = select_files(fetcher.list_type(tdir), prefer=prefer,
                                 since_year=since, kinds=kinds, latest=latest)
            print(f"  {name}（{tdir}）: 対象 {len(files)} 件")
            for f in files[:12]:
                print(f"    - {f.name}")
            if len(files) > 12:
                print(f"    …ほか {len(files) - 12} 件")
            continue

        r = fetcher.fetch_and_ingest(tdir, store=store, since_year=since, kinds=kinds,
                                     latest=latest, refresh=args.refresh,
                                     allow_length_mismatch=args.allow_length_mismatch)
        ing_rows = sum(v["rows"] for v in r["ingest"].values())
        ing_bad = sum(v.get("badlen", 0) for v in r["ingest"].values())
        badmsg = f" / 版差skip{ing_bad}" if ing_bad else ""
        print(f"  {name}: 一覧{r['listed']} / DL{r['downloaded']} / skip{r['skipped_download']} "
              f"/ 取込{ing_rows}行{badmsg}")
        grand["downloaded"] += r["downloaded"]
        grand["skipped"] += r["skipped_download"]
        grand["ingested"] += ing_rows
        grand["badlen"] = grand.get("badlen", 0) + ing_bad

    if not args.dry_run:
        bad = grand.get("badlen", 0)
        badmsg = f" / 版差skip {bad}" if bad else ""
        print(f"[jrdb-fetch] 計: DL {grand['downloaded']} / skip {grand['skipped']} "
              f"/ 取込 {grand['ingested']} 行{badmsg}。再実行しても取得済みは skip され重複しません。")
        if bad:
            print("  ⚠️ レコード長不一致で取込を見送ったファイルがあります"
                  "（古い年度パック等。取り込むなら --allow-length-mismatch）。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
