"""Phase 6-a: netkeiba 調教データ等が「非ログイン（無料）で取得可能か」を判定する調査スクリプト。

feature_expansion_plan.md の Phase 6 ゲート。ユーザーは netkeiba 非会員のため、調教タイム/
追い切り評価・出馬表の脚質マーク等が無料枠で取れるかを **実サイトで確認してから** 実装
（6-b）に進む。取れなければ item 11(調教) はスキップし、item 8(内製展開予測=Phase 4 実装済) を
代替とする。

設計: フェッチ(I/O)と解析(純関数)を分離する。解析関数は合成 HTML でユニットテスト可能。
実フェッチは PlaywrightScraper + レート制限(_rate_limiter)経由で、ユーザーが VPS 等の
netkeiba アクセス可能環境で実行する（この dev サンドボックスからは実行しない想定）。

使い方（netkeiba アクセス可能環境で）:
    python scripts/probe_netkeiba_free.py --race-id 202405021211 --horse-id 2019105219
    python scripts/probe_netkeiba_free.py --race-id 202405021211 --save-dir data/tmp/probe

出力: 各ページの検出エビデンスと総合判定（PROCEED / SKIP_TRAINING / INCONCLUSIVE）。
判定はヒューリスティックのため、保存 HTML を目視確認して最終決定すること。
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ── 判定マーカー（netkeiba のログイン壁 / 会員限定表示） ──────────────
LOGIN_WALL_MARKERS = [
    "ログインが必要", "ログインしてください", "会員限定", "有料会員",
    "プレミアムサービス", "この機能をご利用", "ログイン後にご覧",
]
PREMIUM_MARKERS = ["プレミアム", "有料会員", "Icon_Member", "member_only", "premium"]

# 調教（追い切り）ページに固有のマーカー
TRAINING_CONTEXT_MARKERS = ["追切", "追い切り", "坂路", "ウッド", "美浦", "栗東", "調教"]
TRAINING_EVAL_MARKERS = ["調教評価", "調教矢印", "調教偏差", "一週前", "最終追い切り"]
# 調教タイムらしき数値（例 "52.3" 秒台や "13.4-12.8" のラップ）
_TIME_CELL_RE = re.compile(r"\b\d{1,2}\.\d\b")
_LAP_RE = re.compile(r"\d{1,2}\.\d\s*[-ー]\s*\d{1,2}\.\d")

# 出馬表の無料枠（脚質印・展開・馬体重発表）
SHUTUBA_LEG_TYPE_MARKERS = ["脚質", "Runstyle", "run_style", "逃げ", "先行", "差し", "追い込み"]
SHUTUBA_PACE_MARKERS = ["展開予想", "ペース予想", "隊列", "PaceAnalysis"]
SHUTUBA_WEIGHT_MARKERS = ["馬体重", "Weight", "体重発表"]


def _found(html: str, markers: list) -> list:
    """html に含まれるマーカーの一覧を返す。"""
    return [m for m in markers if m in html]


def analyze_training_page(html: str) -> dict:
    """調教（追い切り）ページの HTML から無料取得可否のエビデンスを抽出する（純関数）。

    返り値のキー:
      login_wall(bool) / premium_hits(list) / training_context(bool) /
      eval_hits(list) / time_cell_count(int) / lap_hits(int) / looks_free(bool)
    """
    if not html:
        return {
            "login_wall": False, "premium_hits": [], "training_context": False,
            "eval_hits": [], "time_cell_count": 0, "lap_hits": 0, "looks_free": False,
        }
    login_hits = _found(html, LOGIN_WALL_MARKERS)
    premium_hits = _found(html, PREMIUM_MARKERS)
    training_context = bool(_found(html, TRAINING_CONTEXT_MARKERS))
    eval_hits = _found(html, TRAINING_EVAL_MARKERS)
    time_cells = len(_TIME_CELL_RE.findall(html))
    lap_hits = len(_LAP_RE.findall(html))

    # 無料で「使える」= ログイン壁なし かつ 調教文脈があり かつ タイム/評価の実データが見える
    has_data = (time_cells >= 3 or lap_hits >= 1 or len(eval_hits) >= 1)
    looks_free = (not login_hits) and training_context and has_data
    return {
        "login_wall": bool(login_hits),
        "login_hits": login_hits,
        "premium_hits": premium_hits,
        "training_context": training_context,
        "eval_hits": eval_hits,
        "time_cell_count": time_cells,
        "lap_hits": lap_hits,
        "looks_free": looks_free,
    }


def analyze_shutuba_free_extras(html: str) -> dict:
    """出馬表ページの無料枠（脚質印/展開/馬体重発表）のエビデンスを抽出する（純関数）。"""
    if not html:
        return {"leg_type_hits": [], "pace_hits": [], "weight_hits": [], "login_wall": False}
    return {
        "login_wall": bool(_found(html, LOGIN_WALL_MARKERS)),
        "leg_type_hits": _found(html, SHUTUBA_LEG_TYPE_MARKERS),
        "pace_hits": _found(html, SHUTUBA_PACE_MARKERS),
        "weight_hits": _found(html, SHUTUBA_WEIGHT_MARKERS),
    }


def training_verdict(analyses: list) -> tuple:
    """複数の調教ページ解析結果から総合判定（verdict, reason）を返す。

    - PROCEED: いずれかで無料の調教データが見えた → 6-b 実装へ
    - SKIP_TRAINING: すべてログイン壁 / データ無し → item 11 スキップ（item 8 が代替）
    - INCONCLUSIVE: 判断がつかない → 保存 HTML を目視確認
    """
    if not analyses:
        return "INCONCLUSIVE", "解析対象なし"
    if any(a.get("looks_free") for a in analyses):
        return "PROCEED", "無料で調教タイム/評価が確認できたページあり"
    if all(a.get("login_wall") or (not a.get("training_context")) for a in analyses):
        return "SKIP_TRAINING", "全ページがログイン壁 or 調教データ非表示（会員限定の可能性）"
    return "INCONCLUSIVE", "壁は無いが調教データを検出できず（保存 HTML を目視確認）"


def _fetch(url: str, wait_selector: str | None = None) -> str:
    """PlaywrightScraper + レート制限で 1 ページ取得する（実 I/O）。"""
    from src.preparing._rate_limiter import polite_interval  # noqa: PLC0415
    from src.preparing._scraper import PlaywrightScraper  # noqa: PLC0415

    polite_interval(1.0)
    driver = PlaywrightScraper()
    return driver.fetch_sync(url, wait_selector=wait_selector)


def main() -> None:
    parser = argparse.ArgumentParser(description="netkeiba 調教データの無料取得可否を調査")
    parser.add_argument("--race-id", help="レース別追い切りページ用 race_id（12桁）")
    parser.add_argument("--horse-id", help="馬別調教ページ用 horse_id")
    parser.add_argument("--save-dir", default="data/tmp/probe", help="取得 HTML の保存先")
    args = parser.parse_args()

    if not args.race_id and not args.horse_id:
        parser.error("--race-id か --horse-id のいずれかを指定してください")

    os.makedirs(args.save_dir, exist_ok=True)
    analyses = []

    targets = []
    if args.race_id:
        targets.append((
            "race_oikiri",
            f"https://race.netkeiba.com/race/oikiri.html?race_id={args.race_id}",
            ".Oikiri_Table, .RaceTableArea, table",
        ))
    if args.horse_id:
        targets.append((
            "horse_training",
            f"https://db.netkeiba.com/horse/training/{args.horse_id}/",
            "table",
        ))

    for name, url, sel in targets:
        logger.info("fetching %s: %s", name, url)
        try:
            html = _fetch(url, wait_selector=sel)
        except Exception as e:  # noqa: BLE001
            logger.warning("取得失敗 %s: %s", name, e)
            continue
        path = os.path.join(args.save_dir, f"{name}.html")
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        result = analyze_training_page(html)
        analyses.append(result)
        logger.info("[%s] 保存: %s", name, path)
        logger.info(
            "[%s] login_wall=%s training_context=%s time_cells=%d laps=%d eval=%s looks_free=%s",
            name, result["login_wall"], result["training_context"],
            result["time_cell_count"], result["lap_hits"], result["eval_hits"], result["looks_free"],
        )

    verdict, reason = training_verdict(analyses)
    logger.info("=" * 60)
    logger.info("調教データ 無料取得 判定: %s — %s", verdict, reason)
    logger.info("保存 HTML を目視確認して最終決定してください（%s）", args.save_dir)
    if verdict == "SKIP_TRAINING":
        logger.info("→ item 11(調教) はスキップ。item 8(内製展開予測=Phase 4 実装済) が代替。")
    elif verdict == "PROCEED":
        logger.info("→ Phase 6-b（_scrape_training.py 等）の実装に進めます。")


if __name__ == "__main__":
    main()
