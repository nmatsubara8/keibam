"""日付範囲を1日ずつ取込む catch-up スクリプト（レジューム対応）。

`daily_ingest.sh` は「1日分」を取込むが、数日分をまとめて追いつきたい
（例: 20260721 以降が未取込）場合はこのスクリプトを使う。

方式:
    * 範囲内のカレンダー日を1日ずつ見て、日次取込と同じ出馬表スクレイプ
      （scrape_race_id_race_time_list）で当日の race_id を確認する。
      （race_id 先頭8桁は「年+場+回+日」でカレンダー日付ではないため、
        開催有無の判定は必ず開催日=post_date を基準にする。）
    * 取得した race_id を `run_pipeline ingest --race-id ...` に直接渡す。
      `--post-date` を使うと ingest 側で出馬表を再取得してしまうため、
      ここで得た race_id を渡して二重スクレイプを避ける（サーバ負荷軽減）。
    * 既定では対象日の race_id をまとめて1回の ingest に渡す（--batch）。
      ingest は毎回 data/html/race 配下の全 bin をパースし直すため、日ごとに
      呼ぶと重いテーブル再生成が日数分くり返される。まとめて1回にすることで
      再生成は1回で済む。日単位のレジューム粒度が欲しい場合は --per-day。
    * 取込に成功した日を resume ファイルに1行1日で記録し、再実行時は
      記録済みの日をスキップする。中断・ネットワーク断からも安全に再開できる。
    * 開催の無い日／出馬表を取得できなかった日は resume に記録しないため、
      次回実行時に再確認される（取りこぼしを防ぐ）。

ポライトネス（netkeiba への自主規制）:
    実際のスクレイプは PlaywrightScraper 経由で行われ、リクエスト間隔
    （最低1秒＋揺らぎ＝合計1〜3秒）と 1時間あたり上限（既定1000件）が
    常に適用される。強度は環境変数で調整できる（このスクリプトから起動する
    ingest サブプロセスにもそのまま継承される）:
        KEIBA_SCRAPE_DELAY           基準待機秒（既定 1.0、最低 1.0）
        KEIBA_SCRAPE_JITTER_MAX      揺らぎ上限秒（既定 2.0 → 合計 1〜3 秒）
        KEIBA_MAX_REQUESTS_PER_HOUR  1時間あたり上限（既定 1000、0 で無効）
    さらに日（バッチ）間に --sleep-between で追加の待機を挟める（既定 2 秒）。

使い方:
    # 20260721 から今日まで取込む（まとめて1回の ingest）
    python scripts/ingest_range.py --from 20260721

    # 範囲を明示（両端含む）
    python scripts/ingest_range.py --from 20260721 --to 20260726

    # 日単位で取込む（レジューム粒度は細かいがテーブル再生成が日数分走る）
    python scripts/ingest_range.py --from 20260721 --per-day

    # よりサーバに優しく（間隔を伸ばし、日間の待機も長めに）
    KEIBA_SCRAPE_DELAY=2 python scripts/ingest_range.py --from 20260721 --sleep-between 5

    # --from 省略時: resume ファイルの最終完了日の翌日から（無ければ7日前から）
    python scripts/ingest_range.py

    # 各日の開催有無を確認するだけ（取込は行わない）
    python scripts/ingest_range.py --from 20260721 --list-only
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import subprocess
import sys
import time
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
    dt.datetime.strptime(digits, "%Y%m%d")  # 存在しない日付を弾く
    return digits


def _plus_one_day(ymd8: str) -> str:
    d = dt.datetime.strptime(ymd8, "%Y%m%d") + dt.timedelta(days=1)
    return d.strftime("%Y%m%d")


def _today_ymd8() -> str:
    return dt.date.today().strftime("%Y%m%d")


def _calendar_days(from_ymd8: str, to_ymd8: str) -> list[str]:
    """[from, to]（両端含む）のカレンダー日を昇順の8桁文字列リストで返す。"""
    start = dt.datetime.strptime(from_ymd8, "%Y%m%d").date()
    end = dt.datetime.strptime(to_ymd8, "%Y%m%d").date()
    days: list[str] = []
    d = start
    while d <= end:  # ② 両端を含む（>= from かつ <= to）
        days.append(d.strftime("%Y%m%d"))
        d += dt.timedelta(days=1)
    return days


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
# 開催有無のプローブ / 1日分の取込
# ---------------------------------------------------------------------------

def _probe_race_ids(ymd8: str) -> list[str]:
    """当日の出馬表から race_id 一覧を取得する（日次取込と同じ経路）。

    開催が無い日・出馬表を取得できなかった日は空リストを返す
    （scrape_race_id_race_time_list はエラー時も ([], []) を返す）。
    """
    from src.preparing._scrape_shutuba import scrape_race_id_race_time_list

    race_ids, _ = scrape_race_id_race_time_list(ymd8)
    return list(race_ids or [])


def _ingest_race_ids(race_ids: list[str], label: str) -> bool:
    """`run_pipeline ingest --race-id ...` を実行し成否を返す。

    出馬表で取得済みの race_id を直接渡すことで、ingest 側での出馬表
    再取得（--post-date 経路）を避け、netkeiba への二重アクセスを防ぐ。
    """
    if not race_ids:
        return True
    cmd = [
        sys.executable, "-m", "src.pipeline.run_pipeline",
        "ingest", "--race-id", *[str(r) for r in race_ids],
    ]
    logger.info("▶ %s 取込開始: %d レース (run_pipeline ingest --race-id ...)", label, len(race_ids))
    proc = subprocess.run(cmd, cwd=str(PROJECT_DIR))
    if proc.returncode == 0:
        logger.info("✅ %s 取込完了", label)
        return True
    logger.warning(
        "⚠️ %s 取込が非0終了 (exit=%d)。resume には記録しないため次回再試行されます。",
        label, proc.returncode,
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
        help="各日の開催有無を確認するだけで取込は行わない",
    )
    parser.add_argument(
        "--per-day", action="store_true",
        help="日ごとに ingest を実行する（レジューム粒度は細かいがテーブル再生成が"
             "日数分走る）。既定は全対象日をまとめて1回の ingest に渡す",
    )
    parser.add_argument(
        "--sleep-between", type=float, default=2.0, metavar="SEC",
        help="出馬表プローブ／日単位 ingest の間に挟む追加待機秒（既定 2.0、"
             "リクエスト間隔の自主規制とは別の上乗せ）",
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

    if from_ymd8 > to_ymd8:
        logger.info("開始日 %s が終了日 %s より後です。取込対象なし。", from_ymd8, to_ymd8)
        return 0

    logger.info("取込対象範囲: %s 〜 %s（両端含む）", from_ymd8, to_ymd8)

    cal_days = _calendar_days(from_ymd8, to_ymd8)
    # ③ resume に done がある日はスキップ
    todo_days = [d for d in cal_days if d not in done]
    logger.info(
        "カレンダー %d 日（うち未処理 %d 日、取込済 %d 日）",
        len(cal_days), len(todo_days), len(cal_days) - len(todo_days),
    )

    # 各日をプローブして開催のある日と race_id を集める
    logger.info("各日の開催有無を確認中...（リクエスト間隔は自主規制で 1〜3 秒/件）")
    race_day_ids: dict[str, list[str]] = {}
    for i, d in enumerate(todo_days):
        if i > 0 and args.sleep_between > 0:
            time.sleep(args.sleep_between)  # プローブ間の上乗せ待機（ポライトネス）
        rids = _probe_race_ids(d)
        if rids:
            race_day_ids[d] = rids
            logger.info("▶ %s: %d レース（取込対象）", d, len(rids))
        else:
            logger.info("· %s: 開催なし/取得できず（resume には記録せず次回再確認）", d)

    race_days = sorted(race_day_ids)
    if args.list_only:
        total = sum(len(v) for v in race_day_ids.values())
        logger.info("--list-only: 開催あり %d 日 / 計 %d レース = %s", len(race_days), total, race_days)
        return 0

    if not race_days:
        logger.info("取込対象（開催あり）の日がありません。")
        return 0

    ingested: list[str] = []
    failed: list[str] = []

    if args.per_day:
        # 日単位: レジューム粒度は細かいが、テーブル再生成が日数分走る
        for i, d in enumerate(race_days):
            if i > 0 and args.sleep_between > 0:
                time.sleep(args.sleep_between)
            if _ingest_race_ids(race_day_ids[d], label=d):
                _mark_done(resume_file, d)
                done.add(d)
                ingested.append(d)
            else:
                failed.append(d)
    else:
        # 既定（バッチ）: 全対象日の race_id をまとめて1回の ingest に渡し、
        # 重いテーブル再生成を1回で済ませる。成功時に全対象日を done 記録。
        all_ids = sorted({r for d in race_days for r in race_day_ids[d]})
        label = f"{race_days[0]}〜{race_days[-1]}（{len(race_days)}日）"
        if _ingest_race_ids(all_ids, label=label):
            for d in race_days:
                _mark_done(resume_file, d)
                done.add(d)
            ingested = list(race_days)
        else:
            failed = list(race_days)

    logger.info("=" * 60)
    logger.info("取込完了: %s", ingested or "なし")
    if failed:
        logger.warning("失敗（未完了）した日: %s", failed)
        logger.info("再実行例: python scripts/ingest_range.py --from %s --to %s", failed[0], to_ymd8)
        return 1

    logger.info("🎉 %s 〜 %s の取込が完了しました（resume: %s）", from_ymd8, to_ymd8, resume_file)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
