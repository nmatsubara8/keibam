"""過去データの年次取得スクリプト。

1年単位でスクレイピングし、各ステップで検証を行う。
失敗した場合はそのランで追加されたデータを削除してリトライを促す。

使い方:
    python scripts/fetch_history.py --from-year 2000 --to-year 2025
    python scripts/fetch_history.py --from-year 2020 --to-year 2021  # 1年だけ
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys
from pathlib import Path

import pandas as pd

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

# 取得対象の pkl パスと検証に使う列名
RAW_PKL_PATHS = [
    LP.RAW_RESULTS_PATH,
    LP.RAW_RACE_INFO_PATH,
    LP.RAW_RETURN_TABLES_PATH,
]


# ---------------------------------------------------------------------------
# バックアップ / ロールバック
# ---------------------------------------------------------------------------

def _backup_pkls() -> dict[str, pd.DataFrame | None]:
    """主要 pkl のスナップショットを取る（メモリ上に保持）。"""
    snaps: dict[str, pd.DataFrame | None] = {}
    for path in RAW_PKL_PATHS:
        if os.path.exists(path):
            try:
                snaps[path] = pd.read_pickle(path)
            except Exception:
                snaps[path] = None
        else:
            snaps[path] = None
    return snaps


def _restore_pkls(snaps: dict[str, pd.DataFrame | None]) -> None:
    """スナップショットに pkl を巻き戻す。"""
    for path, df in snaps.items():
        if df is None:
            if os.path.exists(path):
                os.remove(path)
                logger.info("ロールバック: 削除 %s", path)
        else:
            df.to_pickle(path)
            logger.info("ロールバック: 復元 %s (%d 行)", path, len(df))


def _remove_year_race_bins(year: int) -> int:
    """該当年の race bin ファイルを削除して件数を返す。"""
    race_dir = Path(LP.HTML_RACE_DIR)
    removed = 0
    for p in race_dir.glob(f"{year}*.bin"):
        p.unlink()
        removed += 1
    return removed


# ---------------------------------------------------------------------------
# 1年分の取得
# ---------------------------------------------------------------------------

def fetch_one_year(year: int) -> bool:
    """1年分のデータを取得して True（成功）/ False（失敗）を返す。"""
    from_date = f"{year}-01-01"
    to_date = f"{year + 1}-01-01"
    logger.info("=" * 60)
    logger.info("▶ %d 年 取得開始 (%s 〜 %s)", year, from_date, to_date)

    # --- スナップショット ---
    snaps = _backup_pkls()
    race_bins_before = {p.name for p in Path(LP.HTML_RACE_DIR).glob(f"{year}*.bin")} if Path(LP.HTML_RACE_DIR).exists() else set()

    try:
        # Step 1: 開催日リスト
        logger.info("[1/5] 開催日リスト取得")
        kaisai_dates = scrape_kaisai_date(from_date=from_date, to_date=to_date)
        n_dates = len(kaisai_dates)
        logger.info("  → %d 開催日", n_dates)
        if n_dates == 0:
            logger.warning("  開催日が 0 件。%d 年はスキップします", year)
            return True  # データなし年はエラーではない

        # Step 2: レース ID リスト
        logger.info("[2/5] レース ID リスト取得")
        race_id_list = scrape_race_id_list(kaisai_date_list=kaisai_dates)
        n_races = len(race_id_list)
        logger.info("  → %d レース ID", n_races)
        if n_races == 0:
            logger.warning("  レース ID が 0 件。%d 年はスキップします", year)
            return True

        # Step 3: レース HTML 取得（skip=True で差分のみ・既存 bin は再取得しない）
        logger.info("[3/4] レース HTML 取得 (skip=True で差分のみ)")
        scrape_html_race(race_id_list=race_id_list, skip=True)
        n_bins = len(list(Path(LP.HTML_RACE_DIR).glob(f"{year}*.bin")))
        logger.info("  → %d 件の bin ファイル（%d 年分）", n_bins, year)

        # Step 4: bin 取得件数の簡易検証のみ（テーブル生成は全年完了後に一括で行う）
        logger.info("[4/4] 取得件数検証")
        if n_bins == 0:
            raise ValueError(f"{year} 年の race bin が 0 件")

        logger.info("✅ %d 年 取得完了 (bin=%d 件)", year, n_bins)
        return True

    except Exception as e:
        logger.error("❌ %d 年 取得失敗: %s", year, e, exc_info=True)
        logger.info("ロールバック開始...")
        _restore_pkls(snaps)
        removed = _remove_year_race_bins(year)
        logger.info("  %d 件の race bin を削除", removed)
        logger.info("ロールバック完了。再試行してください: --from-year %d --to-year %d", year, year + 1)
        return False


# ---------------------------------------------------------------------------
# エントリポイント
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="年次履歴データ取得スクリプト")
    parser.add_argument("--from-year", type=int, default=2000, help="取得開始年 (デフォルト: 2000)")
    parser.add_argument("--to-year", type=int, default=2025, help="取得終了年（この年を含む, デフォルト: 2025）")
    parser.add_argument("--stop-on-error", action="store_true", help="エラー発生時に即座に停止する")
    args = parser.parse_args()

    years = list(range(args.from_year, args.to_year + 1))
    logger.info("取得対象: %d 〜 %d 年 (%d 年分)", args.from_year, args.to_year, len(years))

    failed_years: list[int] = []
    for year in years:
        ok = fetch_one_year(year)
        if not ok:
            failed_years.append(year)
            if args.stop_on_error:
                logger.error("--stop-on-error が指定されているため停止します")
                break

    logger.info("=" * 60)
    if failed_years:
        logger.warning("失敗した年: %s", failed_years)
        logger.info("再試行例: python scripts/fetch_history.py --from-year %d --to-year %d", failed_years[0], failed_years[-1])
        sys.exit(1)

    # 全年の HTML 取得が完了してから一括テーブル生成
    # （bin ファイルを全部揃えてから1回だけパースすることで二重取込を防ぐ）
    logger.info("=" * 60)
    logger.info("▶ テーブル生成（全 bin を一括パース・重複は自動除外）")
    results = get_rawdata_results(skip=False)
    get_rawdata_info(skip=False)
    get_rawdata_return(skip=False)
    n_total = len(results)
    logger.info("  results 総行数: %d 行", n_total)
    logger.info("🎉 全年取得・テーブル生成完了")


if __name__ == "__main__":
    main()
