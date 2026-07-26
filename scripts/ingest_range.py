"""日付範囲を1日ずつ取込む catch-up スクリプト（レジューム対応）。

`daily_ingest.sh` は「1日分」を取込むが、数日分をまとめて追いつきたい
（例: 20260721 以降が未取込）場合はこのスクリプトを使う。

特徴:
    * 開催カレンダー（scrape_kaisai_date）から範囲内の「実際にレースがある日」
      だけを対象にするため、開催の無い日を無駄に叩かない。
      （race_id 先頭8桁は「年+場+回+日」でありカレンダー日付ではない。
        日付は必ず開催カレンダー＝date 列を使うのがポイント。）
    * 取込完了日を resume ファイルに1行1日で記録し、再実行時はスキップする。
      途中でネットワーク断や中断があっても、完了済みの日を再取得しない。
    * 各日は既存の `run_pipeline ingest --post-date` をそのまま呼ぶため、
      増分マージ・DB upsert・featured 再生成の挙動は日次ジョブと完全に一致する。

使い方:
    # 20260721 から今日まで取込む
    python scripts/ingest_range.py --from 20260721

    # 範囲を明示（両端含む）
    python scripts/ingest_range.py --from 20260721 --to 20260726

    # --from 省略時: resume ファイルの最終完了日の翌日から（無ければ7日前から）
    python scripts/ingest_range.py

    # 対象日の確認だけ（取込は行わない）
    python scripts/ingest_range.py --from 20260721 --list-only
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import subprocess
import sys
from pathlib import Path

# プロジェクトルートを sys.path に追加
PROJECT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_DIR))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

DEFAULT_RESUME_FILE = PROJECT_DIR / "logs" / "ingest_resume.txt"
DEFAULT_LOOKBACK_DAYS = 7


# ---------------------------------------------------------------------------
# 日付ユーティリティ
# ---------------------------------------------------------------------------

def _to_ymd8(s: str) -> str:
    """"2026-07-21" / "20260721" を8桁 "20260721" に正規化する。"""
    digits = str(s).replace("-", "").replace("/", "").strip()[:8]
    if len(digits) != 8 or not digits.isdigit():
        raise ValueError(f"日付は YYYYMMDD もしくは YYYY-MM-DD で指定してください: {s!r}")
    # 妥当性検証（存在しない日付を弾く）
    dt.datetime.strptime(digits, "%Y%m%d")
    return digits


def _plus_one_day(ymd8: str) -> str:
    d = dt.datetime.strptime(ymd8, "%Y%m%d") + dt.timedelta(days=1)
    return d.strftime("%Y%m%d")


def _today_ymd8() -> str:
    return dt.date.today().strftime("%Y%m%d")


# ---------------------------------------------------------------------------
# resume ファイル
# ---------------------------------------------------------------------------

def _load_done(resume_file: Path) -> set[str]:
    """resume ファイルから取込完了日の集合を読む（無ければ空集合）。"""
    if not resume_file.exists():
        return set()
    done: set[str] = set()
    for line in resume_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and line.isdigit() and len(line) == 8:
            done.add(line)
    return done


def _mark_done(resume_file: Path, ymd8: str) -> None:
    """取込完了日を resume ファイルへ追記する（完了ごとに即 flush）。"""
    resume_file.parent.mkdir(parents=True, exist_ok=True)
    with resume_file.open("a", encoding="utf-8") as f:
        f.write(ymd8 + "\n")


# ---------------------------------------------------------------------------
# 開催日（実際にレースがある日）の列挙
# ---------------------------------------------------------------------------

def _race_days_in_range(from_ymd8: str, to_ymd8: str) -> list[str]:
    """開催カレンダーから [from, to]（両端含む）の開催日を昇順で返す。

    scrape_kaisai_date は from<=d<to（上限排他）なので、to を含めるため
    to+1日を渡す。返り値の最終列（=開催日）から8桁日付を取り出す。
    """
    from src.preparing._scrape_kaisai_date import scrape_kaisai_date

    from_date = f"{from_ymd8[:4]}-{from_ymd8[4:6]}-{from_ymd8[6:8]}"
    to_excl = _plus_one_day(to_ymd8)
    to_date = f"{to_excl[:4]}-{to_excl[4:6]}-{to_excl[6:8]}"

    df = scrape_kaisai_date(from_date=from_date, to_date=to_date)
    if df is None or len(df) == 0:
        return []

    # 開催日は最終列（規約）。8桁日付のみ採用し、範囲内に厳密に絞る。
    date_col = df.columns[-1]
    dates = (
        df[date_col].dropna().astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    )
    days = sorted(
        {d for d in dates if d.isdigit() and len(d) == 8 and from_ymd8 <= d <= to_ymd8}
    )
    return days


# ---------------------------------------------------------------------------
# 1日分の取込（既存 run_pipeline を subprocess で呼ぶ）
# ---------------------------------------------------------------------------

def _ingest_one_day(ymd8: str) -> bool:
    """`run_pipeline ingest --post-date` を1日分実行し成否を返す。"""
    cmd = [
        sys.executable, "-m", "src.pipeline.run_pipeline",
        "ingest", "--post-date", ymd8,
    ]
    logger.info("▶ %s 取込開始: %s", ymd8, " ".join(cmd))
    proc = subprocess.run(cmd, cwd=str(PROJECT_DIR))
    if proc.returncode == 0:
        logger.info("✅ %s 取込完了", ymd8)
        return True
    logger.warning(
        "⚠️ %s 取込が非0終了 (exit=%d)。resume には記録しないため次回再試行されます。",
        ymd8, proc.returncode,
    )
    return False


# ---------------------------------------------------------------------------
# エントリポイント
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="日付範囲を1日ずつ取込む（レジューム対応）")
    parser.add_argument(
        "--from", dest="from_", metavar="YYYYMMDD",
        help="取込開始日（両端含む）。省略時は resume の最終完了日の翌日、"
             f"無ければ {DEFAULT_LOOKBACK_DAYS} 日前",
    )
    parser.add_argument(
        "--to", dest="to", metavar="YYYYMMDD", default=None,
        help="取込終了日（両端含む）。省略時は今日",
    )
    parser.add_argument(
        "--resume-file", default=str(DEFAULT_RESUME_FILE),
        help=f"取込完了日を記録するファイル（既定: {DEFAULT_RESUME_FILE}）",
    )
    parser.add_argument(
        "--list-only", action="store_true",
        help="対象開催日を表示するだけで取込は行わない",
    )
    args = parser.parse_args(argv)

    resume_file = Path(args.resume_file)
    done = _load_done(resume_file)

    # 終了日: 既定は今日
    to_ymd8 = _to_ymd8(args.to) if args.to else _today_ymd8()

    # 開始日: 明示指定 > resume 最終完了日の翌日 > 既定ルックバック
    if args.from_:
        from_ymd8 = _to_ymd8(args.from_)
    elif done:
        from_ymd8 = _plus_one_day(max(done))
        logger.info("--from 省略: resume の最終完了日 %s の翌日 %s から", max(done), from_ymd8)
    else:
        from_ymd8 = (dt.date.today() - dt.timedelta(days=DEFAULT_LOOKBACK_DAYS)).strftime("%Y%m%d")
        logger.info("--from 省略かつ resume 空: 既定で %s から", from_ymd8)

    # ② 両端を含める（>= from かつ <= to）。from > to は何もしない。
    if from_ymd8 > to_ymd8:
        logger.info("開始日 %s が終了日 %s より後です。取込対象なし。", from_ymd8, to_ymd8)
        return 0

    logger.info("取込対象範囲: %s 〜 %s（両端含む）", from_ymd8, to_ymd8)
    logger.info("開催カレンダーを取得中...")
    race_days = _race_days_in_range(from_ymd8, to_ymd8)
    if not race_days:
        logger.info("範囲内に開催日がありません。取込対象なし。")
        return 0

    # ③ resume ファイルに done がある日はスキップ
    todo = [d for d in race_days if d not in done]
    logger.info(
        "開催日 %d 日（うち未取込 %d 日、取込済 %d 日）: %s",
        len(race_days), len(todo), len(race_days) - len(todo), race_days,
    )

    if args.list_only:
        logger.info("--list-only: 取込は行いません。未取込 = %s", todo)
        return 0

    if not todo:
        logger.info("🎉 すべて取込済みです。")
        return 0

    failed: list[str] = []
    for ymd8 in todo:
        if _ingest_one_day(ymd8):
            _mark_done(resume_file, ymd8)
            done.add(ymd8)
        else:
            failed.append(ymd8)

    logger.info("=" * 60)
    if failed:
        logger.warning("失敗（未完了）した日: %s", failed)
        logger.info("再実行例: python scripts/ingest_range.py --from %s --to %s", failed[0], to_ymd8)
        return 1

    logger.info("🎉 %s 〜 %s の取込が完了しました（resume: %s）", from_ymd8, to_ymd8, resume_file)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
