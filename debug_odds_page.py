"""オッズ専用ページの取得・パースを診断する使い捨てスクリプト。

predict_upcoming.py の補完が空を返す原因（パーサ不一致 / JS未描画 / 馬番マージ失敗）を
切り分けるため、本番と同じ取得経路（OddsSnapshotScraper → 単勝 b1 ページ）を再現して
中身を可視化する。発走前の午後（単勝が JS 描画される時間帯）に実行すること。

実行:
  python debug_odds_page.py 202602010412
  python debug_odds_page.py 202605030611 202605030612   # 複数可
"""

from __future__ import annotations

import os
import re
import sys

from src.constants._bet_types import BetType
from src.constants._logging_config import setup_logging
from src.preparing._odds_snapshot import OddsSnapshotScraper
from src.preparing._odds_snapshot import build_odds_url
from src.preparing._odds_snapshot import parse_combo_odds_html
from src.preparing._odds_snapshot import parse_win_odds_html


def _diagnose(html: str) -> None:
    odds_id_tag = 'id="odds-'
    n_tansho_cells = len(re.findall(r'id="odds-1-\d+"', html))
    print(f"  html長          : {len(html)}")
    print(f"  odds- セル含有   : {odds_id_tag in html}")
    print(f"  odds-1-NN 件数   : {n_tansho_cells}")
    for marker in ("単勝", "複勝", "発売前", "発売", "確定", "締切", "終了", "オッズ"):
        if marker in html:
            print(f"  文言             : 「{marker}」あり")
    # 単勝らしき td クラスの存在を確認（DOM 構造の手掛かり）。
    for cls in ("Odds", "Popular", "Txt_R", "Umaban", "Waku"):
        n = len(re.findall(rf'class="[^"]*\b{cls}\b[^"]*"', html))
        if n:
            print(f"  class={cls:8s}   : {n} 箇所")


def main() -> None:
    setup_logging()
    race_ids = sys.argv[1:] or ["202602010412"]
    dump_dir = "data/tmp"
    os.makedirs(dump_dir, exist_ok=True)

    scraper = OddsSnapshotScraper()
    try:
        for rid in race_ids:
            url = build_odds_url(rid, BetType.TANSHO)
            print("=" * 70)
            print(f"race_id={rid}")
            print(f"  url             : {url}")
            try:
                html = scraper.fetch_html(rid, BetType.TANSHO)
            except Exception as e:  # noqa: BLE001
                print(f"  ✗ fetch_html 例外: {e}")
                continue
            _diagnose(html)

            combo_rows = parse_combo_odds_html(html, BetType.TANSHO)
            win_rows = parse_win_odds_html(html)
            print(f"  parse_combo_odds: {len(combo_rows)} 行  例: {combo_rows[:5]}")
            print(f"  parse_win_odds  : {len(win_rows)} 行  例: {win_rows[:5]}")

            path = os.path.join(dump_dir, f"odds_{rid}.html")
            with open(path, "w", encoding="utf-8") as f:
                f.write(html)
            print(f"  HTML をダンプ    : {path}")
    finally:
        scraper_inner = getattr(scraper, "_scraper", None)
        if scraper_inner is not None:
            try:
                scraper_inner.close_sync()
            except Exception:  # noqa: BLE001
                pass
    print("=" * 70)
    print("※ parse_* が 0 行ならパーサ/DOM 不一致（ダンプ HTML を共有してください）。")
    print("※ 行があるのに補完されないなら馬番マージ問題（馬番値の型/桁を確認）。")


if __name__ == "__main__":
    main()
