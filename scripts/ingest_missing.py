"""欠落している開催日だけを冪等に取込む差分インジェスト。

アドホックな日付ループ（誤って過去年起点で暴走しがち）の安全な代替。
DB(raw_race_info)に未取込の発走日だけを 1 日ずつ `run_pipeline ingest --post-date`
に渡す。既定はドライラン（取込まず、欠落日を表示するだけ）。暴走防止として
欠落日が --max-days を超える場合は --force が無いと実行を拒否する。

使い方:
    # 明示日（ネット不要の差分判定が最速）
    python scripts/ingest_missing.py 20260627 20260628             # 欠落確認（dry-run）
    python scripts/ingest_missing.py 20260627 20260628 --execute   # 欠落分のみ取込

    # 期間レンジ（開催カレンダーをスクレイプして差分）
    python scripts/ingest_missing.py --from 2026-01 --to 2026-06
    python scripts/ingest_missing.py --from 2026-01 --to 2026-06 --execute
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ingest_missing")

_DATE8_RE = re.compile(r"\d{8}")
DEFAULT_DB = os.path.join("data", "keibam.db")


def normalize_days(values) -> set[str]:
    """雑多な日付表現（'2026年06月14日' / '2026-06-14' / '20260614'）→ {YYYYMMDD} 集合。"""
    out: set[str] = set()
    for v in values:
        if not v:
            continue
        s = str(v).replace("年", "").replace("月", "").replace("日", "").replace("-", "").replace("/", "")
        m = _DATE8_RE.search(s)
        if m:
            out.add(m.group(0))
    return out


def compute_missing(candidates, ingested) -> list[str]:
    """取込候補のうち DB 未取込の日を昇順で返す（ingested=None は『判定不能』で全件扱い）。"""
    cand = sorted(normalize_days(candidates))
    if ingested is None:
        return cand
    have = normalize_days(ingested)
    return [d for d in cand if d not in have]


def ingested_days(db_path: str) -> set[str] | None:
    """DB の取込済み発走日 {YYYYMMDD} を返す。読めない/スキーマ差異時は None（判定不能）。"""
    if not os.path.exists(db_path):
        logger.warning("DB が見つかりません: %s（取込済み判定不能）", db_path)
        return None
    import sqlite3

    con = sqlite3.connect(db_path)
    try:
        rows = [r[0] for r in con.execute("select distinct date from raw_race_info")]
    except Exception as e:  # noqa: BLE001 — date 列が無い等のスキーマ差異
        logger.warning("raw_race_info.date を読めません（%s）。取込済み判定不能", e)
        return None
    finally:
        con.close()
    return normalize_days(rows)


def _candidates_from_range(from_ym: str, to_ym: str) -> list[str]:
    """開催カレンダーをスクレイプして [from, to] 月内の開催日(YYYYMMDD)を返す（要ネット）。"""
    from src.preparing._scrape_kaisai_date import scrape_kaisai_date

    from_8 = from_ym.replace("-", "") + ("01" if len(from_ym.replace("-", "")) == 6 else "")
    # to は当月末日まで含めたいので翌月1日を終端に渡す
    ty, tm = (int(x) for x in to_ym.split("-")[:2])
    ny, nm = (ty + 1, 1) if tm == 12 else (ty, tm + 1)
    from_date = f"{from_8[:4]}-{from_8[4:6]}-01"
    to_date = f"{ny:04d}-{nm:02d}-01"
    logger.info("開催カレンダー取得: %s 〜 %s", from_date, to_date)
    df = scrape_kaisai_date(from_date=from_date, to_date=to_date)
    vals = df.to_numpy().ravel().tolist() if hasattr(df, "to_numpy") else list(df)
    days = sorted(normalize_days(vals))
    f8, t8 = from_date.replace("-", ""), to_date.replace("-", "")
    return [d for d in days if f8 <= d < t8]


def _ingest_one(day: str) -> int:
    """1 日分を run_pipeline ingest --post-date で取込む。終了コードを返す。"""
    cmd = [sys.executable, "-m", "src.pipeline.run_pipeline", "ingest", "--post-date", day]
    logger.info("▶ ingest %s", day)
    return subprocess.call(cmd)


def main() -> None:
    ap = argparse.ArgumentParser(description="欠落開催日だけを冪等に取込む差分インジェスト")
    ap.add_argument("days", nargs="*", help="発走日 YYYYMMDD（明示指定。複数可）")
    ap.add_argument("--from", dest="from_ym", help='レンジ開始 "YYYY-MM"（カレンダーをスクレイプ）')
    ap.add_argument("--to", dest="to_ym", help='レンジ終了 "YYYY-MM"（この月を含む）')
    ap.add_argument("--db", default=DEFAULT_DB, help=f"DB パス（既定 {DEFAULT_DB}）")
    ap.add_argument("--execute", action="store_true", help="実際に取込む（既定はドライラン）")
    ap.add_argument("--max-days", type=int, default=40,
                    help="一度に取込む欠落日の上限（暴走防止。既定 40）")
    ap.add_argument("--force", action="store_true", help="上限超過/判定不能でも実行する")
    ap.add_argument("--keep-going", action="store_true", help="途中失敗しても残りを続行する")
    args = ap.parse_args()

    today = dt.date.today().strftime("%Y%m%d")

    # 取込候補日を決める（明示日 優先 → レンジ）。
    if args.days:
        candidates = sorted(normalize_days(args.days))
    elif args.from_ym and args.to_ym:
        candidates = _candidates_from_range(args.from_ym, args.to_ym)
    else:
        ap.error("発走日 YYYYMMDD を列挙するか、--from/--to（YYYY-MM）を指定してください")

    # 未来日は結果が未確定なので除外する。
    future = [d for d in candidates if d > today]
    candidates = [d for d in candidates if d <= today]
    if future:
        logger.info("未来日 %d 件は対象外（結果未確定）: %s", len(future), future[:5])

    have = ingested_days(args.db)
    missing = compute_missing(candidates, have)

    n_have = "?" if have is None else len(have)
    print(f"■ 候補 {len(candidates)} 日 / 取込済み {n_have} 日 / 欠落 {len(missing)} 日")
    print(f"  欠落日: {missing if missing else '（なし）'}")
    if have is None:
        print("  ⚠ DB から取込済みを判定できませんでした（スキーマ差異/DB なし）。"
              "明示日指定＋--force での実行を推奨。")

    if not missing:
        print("✅ 欠落なし。取込は不要です。")
        return

    if not args.execute:
        print("（ドライラン）取込むには --execute を付けてください。")
        return

    # 暴走防止: 欠落が多すぎる、または判定不能のときは --force を要求。
    if (len(missing) > args.max_days or have is None) and not args.force:
        print(f"✗ 欠落 {len(missing)} 日 > 上限 {args.max_days}（または判定不能）。"
              "意図的な大量取込なら --force を付けてください。")
        sys.exit(2)

    failed = []
    for d in missing:
        code = _ingest_one(d)
        if code != 0:
            failed.append(d)
            logger.error("✗ ingest 失敗 %s (exit=%d)", d, code)
            if not args.keep_going:
                logger.error("中断します（残り %d 日。--keep-going で続行可）",
                             len(missing) - missing.index(d) - 1)
                break

    done = len(missing) - len(failed)
    print(f"■ 完了: 取込 {done} 日 / 失敗 {len(failed)} 日" + (f" {failed}" if failed else ""))
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
