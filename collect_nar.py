"""地方競馬(NAR)の履歴データ（結果・レース情報・払戻）をバルク収集する。

検証済みの経路を組み合わせた専用コレクタ:
  1. nar.netkeiba.com のレース一覧から日付ごとに NAR race_id を発見（organizer="local"）。
  2. db.netkeiba.com＋既存パーサで results/race_info/return を取得し raw pkl に増分マージ。
馬ページ・血統・featured 再生成には触らない（＝147k 馬バックフィルや重い featured 再構築を
毎回走らせない）。バルクで raw を貯めたあとに、別途 1 回だけ馬 backfill＋featured 再生成する運用。

ポライトネス（既定 4〜6 秒間隔＋1時間上限 1000）は全取得に自動適用。NAR に無い場合が多い
当日ノート/予想印は既定でスキップ（--with-notes で有効化）。

実行:
  # まず件数だけ確認（発見のみ・取得しない）
  python collect_nar.py --from 20260701 --to 20260721 --discover-only
  # 実収集（結果/情報/払戻のみ・馬/featured 非対象）
  python collect_nar.py --from 20260701 --to 20260721
  # 場を絞る（大井44・門別30 等の場コード）／安全上限
  python collect_nar.py --from 20250101 --to 20251231 --tracks 44 30 --limit 500

収集後（raw が貯まったら 1 回だけ）:
  KEIBA_SKIP_PEDS=1 python -m src.pipeline.run_pipeline ingest --race-id <代表的な1件>  # 馬backfill+featured
  もしくは backfill-horses → backfill-peds → rebuild-featured を順に実行。
"""

from __future__ import annotations

import argparse
import datetime
import logging
import os

logger = logging.getLogger(__name__)


def _date_range(frm: str, to: str) -> list[str]:
    """YYYYMMDD 文字列 frm..to（両端含む）の日付リストを返す。"""
    d0 = datetime.datetime.strptime(frm, "%Y%m%d").date()
    d1 = datetime.datetime.strptime(to, "%Y%m%d").date()
    if d1 < d0:
        d0, d1 = d1, d0
    out = []
    d = d0
    while d <= d1:
        out.append(d.strftime("%Y%m%d"))
        d += datetime.timedelta(days=1)
    return out


def main() -> None:
    from src.constants._logging_config import setup_logging

    setup_logging()
    ap = argparse.ArgumentParser(description="NAR 履歴データ（結果/情報/払戻）のバルク収集")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--from", dest="frm", metavar="YYYYMMDD", help="収集開始日")
    g.add_argument("--date", nargs="+", metavar="YYYYMMDD", help="個別日付を列挙")
    ap.add_argument("--to", metavar="YYYYMMDD", help="収集終了日（--from と併用）")
    ap.add_argument("--tracks", nargs="+", metavar="CODE",
                    help="場コードで絞る（例 44=大井 30=門別 45=川崎 …。未指定=全 NAR）")
    ap.add_argument("--limit", type=int, default=None, help="取得レース数の安全上限")
    ap.add_argument("--discover-only", action="store_true", help="発見のみ（取得しない・件数確認用）")
    ap.add_argument("--with-notes", action="store_true", help="当日ノート/予想印も取得（既定はスキップ）")
    ap.add_argument(
        "--include-banei", action="store_true",
        help="帯広ばんえい(場コード65)も収集する。既定は除外（輓馬＝そり引きで馬場水分%%・"
             "タイム等が平地と別体系のため、平地モデルには不適）",
    )
    args = ap.parse_args()

    if args.frm and not args.to:
        ap.error("--from を使う場合は --to も指定してください")

    # NAR に無いことが多い当日ノート/予想印は既定でスキップ（無駄な取得を避ける）
    if not args.with_notes:
        os.environ.setdefault("KEIBA_SKIP_RACE_DAY_NOTES", "1")
        os.environ.setdefault("KEIBA_SKIP_YOSO_MARKS", "1")

    from src.constants._local_paths import LocalPaths
    from src.pipeline._ingestion import load_raw
    from src.preparing._data_source import create_data_source
    from src.preparing._scrape_shutuba import scrape_race_id_race_time_list

    def _existing_race_ids() -> set:
        """results.pkl の既存 race_id 集合を返す。

        results.pkl は RangeIndex＋race_id 列（index は race_id ではない）なので、
        index を見る existing_race_ids は使えない。race_id 列から集合を作る。
        """
        df = load_raw(LocalPaths.RAW_RESULTS_PATH)
        if df is None or df.empty or "race_id" not in df.columns:
            return set()
        return set(df["race_id"].astype(str))

    dates = args.date if args.date else _date_range(args.frm, args.to)
    tracks = set(args.tracks) if args.tracks else None
    banei = "65"  # 帯広ばんえい（別体系）。既定で除外
    logger.info("[collect-nar] 対象 %d 日 / 場フィルタ=%s / ばんえい=%s",
                len(dates), sorted(tracks) if tracks else "全NAR",
                "含む" if args.include_banei else "除外")

    # 1. 日付ごとに NAR race_id を発見
    discovered: list[str] = []
    for d in dates:
        ids, _ = scrape_race_id_race_time_list(d, "local")
        ids = [str(r) for r in ids]
        if not args.include_banei:  # ばんえい(帯広65)を除外
            ids = [r for r in ids if r[4:6] != banei]
        if tracks:
            ids = [r for r in ids if r[4:6] in tracks]
        discovered.extend(ids)
        logger.info("[collect-nar] %s: %d レース発見（累計 %d）", d, len(ids), len(discovered))

    discovered = list(dict.fromkeys(discovered))  # 順序保持で重複除去
    # 2. 既存 results.pkl（race_id 列）と照合して新規のみに絞る
    existing = _existing_race_ids()
    new_ids = [r for r in discovered if r not in existing]  # 順序保持で新規のみ
    n_dup = len(discovered) - len(new_ids)
    logger.info("[collect-nar] 発見 %d / 既存 %d / 新規 %d", len(discovered), n_dup, len(new_ids))

    if args.limit is not None and len(new_ids) > args.limit:
        logger.info("[collect-nar] --limit %d 件に制限（残りは次回に）", args.limit)
        new_ids = new_ids[: args.limit]

    if args.discover_only:
        print(f"\n発見 {len(discovered)} レース / 新規 {len(new_ids)} レース（--discover-only のため取得せず）")
        print(f"取得見積り: 約 {len(new_ids)} リクエスト × ~5秒 = ~{len(new_ids) * 5 / 60:.0f} 分")
        return
    if not new_ids:
        print("新規レースがありません（全て取得済み or 発見0）。")
        return

    # 3. results/info/return を取得して raw pkl に増分マージ（馬/featured 非対象）
    source = create_data_source("netkeiba")
    logger.info("[collect-nar] %d レースの結果/情報/払戻を取得します（馬・featured は非対象）", len(new_ids))
    source.acquire_races(new_ids)

    after = _existing_race_ids()
    print(f"\n収集完了: results.pkl の総レース数 {len(existing)} → {len(after)}（+{len(after) - len(existing)}）")
    print("次段（raw が十分貯まったら 1 回だけ）: 馬 backfill → 血統 backfill → rebuild-featured。")


if __name__ == "__main__":
    main()
