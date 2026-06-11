"""過去データの取得スクリプト（年・月単位）。

指定期間を「月単位」でスクレイピングし、各ステップで検証を行う。
失敗した月はそのランで追加した bin を削除してリトライを促す。

使い方:
    # 月単位（素早いテスト向き）
    python scripts/fetch_history.py --from 2008-01 --to 2008-03
    python scripts/fetch_history.py --from 2008-01 --to 2008-01   # 1か月だけ
    python scripts/fetch_history.py --from 2008-01 --to 2008-01 --no-tables  # 取得のみ（テーブル生成省略・最速）

    # 年単位（後方互換）
    python scripts/fetch_history.py --from-year 2000 --to-year 2025
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# プロジェクトルートを sys.path に追加
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.constants._local_paths import LocalPaths
from src.preparing._scrape_kaisai_date import scrape_kaisai_date
from src.preparing._scrape_race_id_list import scrape_race_id_list
from src.preparing._scrape_html_race import scrape_html_race
from src.preparing._get_rawdata import get_rawdata_results, get_rawdata_info, get_rawdata_return

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

LP = LocalPaths()


# ---------------------------------------------------------------------------
# ロールバック
# ---------------------------------------------------------------------------

def _remove_period_race_bins(prefix: str) -> int:
    """指定プレフィックス（例 "200801"）の race bin を削除して件数を返す。"""
    race_dir = Path(LP.HTML_RACE_DIR)
    removed = 0
    for p in race_dir.glob(f"{prefix}*.bin"):
        p.unlink()
        removed += 1
    return removed


# ---------------------------------------------------------------------------
# 期間（月単位）リスト生成
# ---------------------------------------------------------------------------

def _month_periods(from_ym: str, to_ym: str) -> list[tuple[int, int]]:
    """"YYYY-MM" 〜 "YYYY-MM"（両端含む）の (year, month) リストを返す。"""
    fy, fm = (int(x) for x in from_ym.split("-")[:2])
    ty, tm = (int(x) for x in to_ym.split("-")[:2])
    periods: list[tuple[int, int]] = []
    y, m = fy, fm
    while (y, m) <= (ty, tm):
        periods.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return periods


# ---------------------------------------------------------------------------
# 1か月分の取得
# ---------------------------------------------------------------------------

def fetch_one_month(year: int, month: int) -> bool:
    """1か月分のデータを取得して True（成功）/ False（失敗）を返す。"""
    from_date = f"{year:04d}-{month:02d}-01"
    next_y, next_m = (year + 1, 1) if month == 12 else (year, month + 1)
    to_date = f"{next_y:04d}-{next_m:02d}-01"
    prefix = f"{year:04d}{month:02d}"  # bin ファイル名の年月プレフィックス
    label = f"{year:04d}-{month:02d}"

    logger.info("=" * 60)
    logger.info("▶ %s 取得開始 (%s 〜 %s)", label, from_date, to_date)

    try:
        # Step 1: 開催日リスト
        logger.info("[1/4] 開催日リスト取得")
        kaisai_dates = scrape_kaisai_date(from_date=from_date, to_date=to_date)
        n_dates = len(kaisai_dates)
        logger.info("  → %d 開催日", n_dates)
        if n_dates == 0:
            logger.warning("  開催日が 0 件。%s はスキップします", label)
            return True  # データなし月はエラーではない

        # Step 2: レース ID リスト
        logger.info("[2/4] レース ID リスト取得")
        race_id_list = scrape_race_id_list(kaisai_date_list=kaisai_dates)
        n_races = len(race_id_list)
        logger.info("  → %d レース ID", n_races)
        if n_races == 0:
            logger.warning("  レース ID が 0 件。%s はスキップします", label)
            return True

        # Step 3: レース HTML 取得（skip=True で差分のみ）
        logger.info("[3/4] レース HTML 取得 (skip=True で差分のみ)")
        scrape_html_race(race_id_list=race_id_list, skip=True)
        n_bins = len(list(Path(LP.HTML_RACE_DIR).glob(f"{prefix}*.bin")))
        logger.info("  → %d 件の bin ファイル（%s 分）", n_bins, label)

        # Step 4: bin 取得件数の簡易検証
        logger.info("[4/4] 取得件数検証")
        if n_bins == 0:
            raise ValueError(f"{label} の race bin が 0 件")

        logger.info("✅ %s 取得完了 (bin=%d 件)", label, n_bins)
        return True

    except Exception as e:
        logger.error("❌ %s 取得失敗: %s", label, e, exc_info=True)
        logger.info("ロールバック開始...")
        removed = _remove_period_race_bins(prefix)
        logger.info("  %d 件の race bin を削除", removed)
        logger.info("ロールバック完了。再試行: --from %s --to %s", label, label)
        return False


# ---------------------------------------------------------------------------
# エントリポイント
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="履歴データ取得スクリプト（年・月単位）")
    # 月単位（推奨・素早いテスト向き）
    parser.add_argument("--from", dest="from_ym", help='取得開始月 "YYYY-MM"（例: 2008-01）')
    parser.add_argument("--to", dest="to_ym", help='取得終了月 "YYYY-MM"（この月を含む）')
    # 年単位（後方互換）
    parser.add_argument("--from-year", type=int, help="取得開始年（年単位モード）")
    parser.add_argument("--to-year", type=int, help="取得終了年（この年を含む, 年単位モード）")
    parser.add_argument("--no-tables", action="store_true",
                        help="テーブル生成を省略し HTML 取得のみ行う（最速・テスト向き）")
    parser.add_argument("--stop-on-error", action="store_true", help="エラー発生時に即座に停止する")
    args = parser.parse_args()

    # 期間（月リスト）を決定する。月指定を優先し、無ければ年指定、どちらも無ければエラー。
    if args.from_ym and args.to_ym:
        periods = _month_periods(args.from_ym, args.to_ym)
        span = f"{args.from_ym} 〜 {args.to_ym}"
    elif args.from_year is not None and args.to_year is not None:
        periods = _month_periods(f"{args.from_year}-01", f"{args.to_year}-12")
        span = f"{args.from_year} 〜 {args.to_year} 年"
    else:
        parser.error("--from/--to（月単位）または --from-year/--to-year（年単位）を指定してください")

    logger.info("取得対象: %s (%d か月分)", span, len(periods))

    failed: list[str] = []
    for year, month in periods:
        ok = fetch_one_month(year, month)
        if not ok:
            failed.append(f"{year:04d}-{month:02d}")
            if args.stop_on_error:
                logger.error("--stop-on-error が指定されているため停止します")
                break

    logger.info("=" * 60)
    if failed:
        logger.warning("失敗した月: %s", failed)
        logger.info("再試行例: python scripts/fetch_history.py --from %s --to %s", failed[0], failed[-1])
        sys.exit(1)

    if args.no_tables:
        logger.info("🎉 HTML 取得完了（--no-tables のためテーブル生成は省略）")
        return

    # 全期間の HTML 取得が完了してから一括テーブル生成
    # （bin ファイルを全部揃えてから1回だけパースすることで二重取込を防ぐ）
    logger.info("=" * 60)
    logger.info("▶ テーブル生成（全 bin を一括パース・重複は自動除外）")
    results = get_rawdata_results(skip=False)
    get_rawdata_info(skip=False)
    get_rawdata_return(skip=False)
    n_total = len(results)
    logger.info("  results 総行数: %d 行", n_total)
    logger.info("🎉 取得・テーブル生成完了")


if __name__ == "__main__":
    main()
