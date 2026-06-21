"""画面サンプルの実 HTML を取得し、構造を要約するスクリプト（パーサ設計の前処理）。

データ取得項目の再設計（docs/data_acquisition_redesign.md）で分析した**全56サンプルの
ページ種別**（レース結果/馬/血統/人物4種×{profile,result,race}/所有馬・生産馬一覧/出馬表/
馬柱/競馬新聞/調教 oikiri/パドック/厩舎コメント/ライブ結果/予想印/オッズ8券種/調子偏差値）の
**実 DOM 構造**を入手するための使い捨て診断スクリプト。コンテナからは netkeiba 不達なので
**netkeiba に到達できる環境（VPS/ローカル）で実行**する。

ID 形式の注意: 人物の profile は数値 id（owner/486800）、result/race はハッシュ id
（owner=`?id=o_*` / breeder=`?id=b_*`）。種別に応じ --person-id に適切な形式を渡すこと。

やること:
1. ページ種別ごとの URL を組み立て、PlaywrightScraper（JS 描画待ち・ポライトネス・
   TLS 検査プロキシ対応）で HTML を取得
2. 生 HTML を `data/tmp/html_samples/<type>_<id>.html` に保存
3. `src/preparing/_html_structure.py` で table 構造・id・プレミアム手掛かりを要約し、
   `data/tmp/html_samples/<type>_<id>.report.txt` とコンソールに出力

このレポート（および生 HTML）を見て / 共有して、新規ページのパーサを実 DOM に合わせて起こす。

実行例:
  # 個別（種別 と id を指定）
  python fetch_html_samples.py oikiri 202605030611
  python fetch_html_samples.py jockey_result 00666
  python fetch_html_samples.py horse 2021103272

  # サンプル一式（各種別を 1 件ずつ。id は --race-id/--horse-id/--person-id で上書き可）
  python fetch_html_samples.py --all --race-id 202605030611 --horse-id 2021103272

ポライトネス: PlaywrightScraper は 1 時間あたりのリクエスト上限（HourlyRateLimiter）を
適用するが、本スクリプトは複数ページを連続取得するため、§3-37 と同様に各取得の「間に」
`polite_interval`（最低 1 秒 + 揺らぎ、既定 1〜3 秒）を明示的に挟む。環境変数で調整可:
  KEIBA_SCRAPE_DELAY（既定 1.0、<=0 で間隔無効）/ KEIBA_SCRAPE_JITTER_MAX（既定 2.0）/
  KEIBA_MAX_REQUESTS_PER_HOUR（既定 1000）。

TLS 検査プロキシ配下では `KEIBAM_IGNORE_HTTPS_ERRORS=1` を付けて実行する。
"""

from __future__ import annotations

import argparse
import os
import time

from src.constants._logging_config import setup_logging
from src.preparing._html_structure import structure_report
from src.preparing._rate_limiter import polite_interval

DUMP_DIR = os.path.join("data", "tmp", "html_samples")

# ページ種別 → (URL テンプレート, JS 描画完了を待つ CSS セレクタ or None)
# {race_id}/{horse_id}/{person_id} を id で置換する。
# docs/data_acquisition_redesign.md の全56サンプル（A.ページ種別マップ）を網羅する。
# 注: 人物の result/race は ?id=ハッシュ（owner=o_*, breeder=b_*）、profile は数値 id。
#     用途に応じ --person-id に適切な形式を渡すこと（数値↔ハッシュ対応は再設計の論点）。
PAGE_TYPES: dict[str, tuple[str, str | None]] = {
    # ── DB: レース結果（results/race_info/return + コーナー通過/ラップ/走行データ別表）──
    "race_result": ("https://db.netkeiba.com/race/{race_id}/", "table"),
    # ── DB: 馬（プロフィール血統枠=父/母父、競走成績、血統）──
    "horse": ("https://db.netkeiba.com/horse/{horse_id}/", "table"),
    "horse_ped": ("https://db.netkeiba.com/horse/ped/{horse_id}/", "table"),
    # ── DB: 人物4種（profile / 年度別成績 result.html / レース毎 race.html）共通テンプレート ──
    "jockey": ("https://db.netkeiba.com/jockey/{person_id}/", "table"),
    "jockey_result": ("https://db.netkeiba.com/jockey/result.html?id={person_id}", "table"),
    "jockey_race": ("https://db.netkeiba.com/jockey/race.html?id={person_id}", "table"),
    "trainer": ("https://db.netkeiba.com/trainer/{person_id}/", "table"),
    "trainer_result": ("https://db.netkeiba.com/trainer/result.html?id={person_id}", "table"),
    "trainer_race": ("https://db.netkeiba.com/trainer/race.html?id={person_id}", "table"),
    "owner": ("https://db.netkeiba.com/owner/{person_id}/", "table"),
    "owner_result": ("https://db.netkeiba.com/owner/result.html?id={person_id}", "table"),
    "owner_race": ("https://db.netkeiba.com/owner/race.html?id={person_id}", "table"),
    "breeder": ("https://db.netkeiba.com/breeder/{person_id}/", "table"),
    "breeder_result": ("https://db.netkeiba.com/breeder/result.html?id={person_id}", "table"),
    "breeder_race": ("https://db.netkeiba.com/breeder/race.html?id={person_id}", "table"),
    # ── DB: 所有馬/生産馬一覧（父/母/母父の併記列＝安価な血統取得源）──
    "owner_horses": ("https://db.netkeiba.com/horse/list.html?owner_id={person_id}", "table"),
    "breeder_horses": ("https://db.netkeiba.com/horse/list.html?breeder_id={person_id}", "table"),
    # ── race.netkeiba: 当日エントリ / 馬柱 ──
    "shutuba": ("https://race.netkeiba.com/race/shutuba.html?race_id={race_id}", "table"),
    "shutuba_past": (
        "https://race.netkeiba.com/race/shutuba_past.html?race_id={race_id}",
        "table",
    ),  # 馬柱9走
    "newspaper": (
        "https://race.netkeiba.com/race/newspaper.html?race_id={race_id}",
        "table",
    ),  # 競馬新聞/馬柱5走
    # ── race.netkeiba: 🌟調教（追い切り）評価のみ / 最終追切(type=2) / 中間全て(type=1) ──
    "oikiri": ("https://race.netkeiba.com/race/oikiri.html?race_id={race_id}", "table"),
    "oikiri_final": (
        "https://race.netkeiba.com/race/oikiri.html?race_id={race_id}&type=2",
        "table",
    ),
    "oikiri_all": (
        "https://race.netkeiba.com/race/oikiri.html?race_id={race_id}&type=1",
        "table",
    ),
    # ── race.netkeiba: 🌟パドック / 厩舎コメント / ライブ結果 / 予想印 ──
    "paddock": ("https://race.netkeiba.com/race/paddock.html?race_id={race_id}", "table"),
    "comment": ("https://race.netkeiba.com/race/comment.html?race_id={race_id}", "table"),
    "result_live": ("https://race.netkeiba.com/race/result.html?race_id={race_id}", "table"),
    "yoso_marks": ("https://race.netkeiba.com/yoso/mark_list.html?race_id={race_id}", "table"),
    # ── yoso.netkeiba: 予想家プロフィール/実績（印のスキル加重の素・無料）──
    #   yid=予想家ID（印グリッドの各列に紐づく）。no<N> は区分（要サンプルで確定）。
    "yoso_profile": ("https://yoso.netkeiba.com/no1/?pid=profile&yid={person_id}", None),
    # ── race.netkeiba: オッズ8券種（odds_watch で取得済・構造参照用。type=b1〜b8）──
    #   b1=単勝/複勝, b2=枠連, b3=馬連, b4=ワイド, b5=馬単, b6=枠単?, b7=三連複, b8=三連単
    "odds_b1": ("https://race.netkeiba.com/odds/index.html?type=b1&race_id={race_id}", None),
    "odds_b3": ("https://race.netkeiba.com/odds/index.html?type=b3&race_id={race_id}", None),
    "odds_b4": ("https://race.netkeiba.com/odds/index.html?type=b4&race_id={race_id}", None),
    "odds_b7": ("https://race.netkeiba.com/odds/index.html?type=b7&race_id={race_id}", None),
    "odds_b8": ("https://race.netkeiba.com/odds/index.html?type=b8&race_id={race_id}", None),
    # ── sp ドメイン: 派生指標（調子偏差値・自前再現候補）──
    "barometer": (
        "https://race.sp.netkeiba.com/barometer/score.html?race_id={race_id}",
        None,
    ),
}

# --all で取得する代表セット（新規パーサが必要な種別を中心に網羅。id は CLI で上書き可）。
# 重複(result_live=race_resultのライブ版/ shutuba=predict_upcomingで取得済) と
# 取得済みオッズは個別指定で取れるため --all からは外す。
DEFAULT_ALL = (
    # DB レース・馬・人物・一覧
    "race_result",
    "horse",
    "jockey_result",
    "trainer_result",
    "owner_result",
    "breeder_result",
    "owner_horses",
    "breeder_horses",
    # race.netkeiba 当日情報（新規パーサ対象）
    "shutuba_past",
    "newspaper",
    "oikiri",
    "oikiri_final",
    "paddock",
    "comment",
    "yoso_marks",  # 専門紙陣の印グリッド（無料・div/img）。実績で重み付けする集約特徴の素
    # 派生指標
    "barometer",
)


def _build_url(page_type: str, race_id: str, horse_id: str, person_id: str) -> str:
    tmpl, _ = PAGE_TYPES[page_type]
    return tmpl.format(race_id=race_id, horse_id=horse_id, person_id=person_id)


def _id_for(page_type: str, race_id: str, horse_id: str, person_id: str) -> str:
    """ファイル名用に、その種別が使う主 id を返す。"""
    tmpl, _ = PAGE_TYPES[page_type]
    if "{race_id}" in tmpl:
        return race_id
    if "{horse_id}" in tmpl:
        return horse_id
    return person_id


def fetch_one(scraper, page_type: str, race_id: str, horse_id: str, person_id: str) -> None:
    url = _build_url(page_type, race_id, horse_id, person_id)
    _, wait_selector = PAGE_TYPES[page_type]
    rid = _id_for(page_type, race_id, horse_id, person_id)
    print("=" * 78)
    print(f"[{page_type}] {url}")
    try:
        html = scraper.fetch_sync(url, wait_selector=wait_selector)
    except Exception as e:  # noqa: BLE001
        print(f"  ✗ 取得失敗: {e}")
        return

    os.makedirs(DUMP_DIR, exist_ok=True)
    html_path = os.path.join(DUMP_DIR, f"{page_type}_{rid}.html")
    report_path = os.path.join(DUMP_DIR, f"{page_type}_{rid}.report.txt")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    report = structure_report(html, url=url, min_rows=2)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report)
    print(report)
    print(f"  → 生HTML : {html_path}")
    print(f"  → 構造   : {report_path}")


def main() -> None:
    setup_logging()
    parser = argparse.ArgumentParser(description="netkeiba 画面サンプルの実 HTML 構造を取得する")
    parser.add_argument("page_type", nargs="?", help=f"ページ種別（{', '.join(PAGE_TYPES)}）")
    parser.add_argument("page_id", nargs="?", help="その種別の主 id（race_id/horse_id/person_id）")
    parser.add_argument("--all", action="store_true", help="代表セットを一括取得")
    parser.add_argument("--race-id", default="202605030611")
    parser.add_argument("--horse-id", default="2021103272")
    parser.add_argument("--person-id", default="00666")
    args = parser.parse_args()

    from src.preparing._scraper import PlaywrightScraper

    race_id, horse_id, person_id = args.race_id, args.horse_id, args.person_id
    if args.page_type and args.page_id:
        # 単発: 主 id を該当種別へ流し込む
        tmpl, _ = PAGE_TYPES[args.page_type]
        if "{race_id}" in tmpl:
            race_id = args.page_id
        elif "{horse_id}" in tmpl:
            horse_id = args.page_id
        else:
            person_id = args.page_id

    if args.page_type and args.page_type not in PAGE_TYPES:
        raise SystemExit(f"未対応の種別: {args.page_type}\n対応: {', '.join(PAGE_TYPES)}")

    targets = DEFAULT_ALL if args.all or not args.page_type else [args.page_type]

    scraper = PlaywrightScraper()
    try:
        # ポライトネス: PlaywrightScraper.fetch は 1 時間上限のみ適用するため、
        # 複数ページを連続取得する本スクリプトでは §3-37 と同様に
        # リクエスト「間隔」を明示的に挟む（最低 1 秒 + 揺らぎ。delay<=0 で無効化）。
        for n, pt in enumerate(targets):
            if n > 0:
                interval = polite_interval()
                if interval > 0:
                    time.sleep(interval)
            fetch_one(scraper, pt, race_id, horse_id, person_id)
    finally:
        close = getattr(scraper, "close", None)
        if callable(close):
            try:
                close()
            except Exception:  # noqa: BLE001
                pass


if __name__ == "__main__":
    main()
