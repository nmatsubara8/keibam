"""Phase 9: コース形状マスタを公式サイトからスクレイプして course_master.csv を生成する。

feature_expansion_plan.md の Phase 9。手入力せず、公式のコースデータ（直線長・高低差・
1コーナーまで距離・ゴール前坂）をラベル付きテキストから抽出して CSV 化する。

設計（Phase 6-a と同じ規律）: フェッチ(I/O)と解析(純関数 parse_course_page)を分離。
解析は合成 HTML でユニットテスト可能。実サイトの DOM/URL は環境により異なるため、
`--probe` で 1 ページの抽出結果を確認 → 問題なければ本取得、という段階運用にする。

使い方（公式サイトにアクセス可能な環境で）:
    # 1) まず 1 ページで抽出を確認
    python scripts/scrape_course_master.py --probe \
        --url "https://<official>/course/tokyo_shiba_1400" \
        --place 05 --race-type 芝 --course-len 1400
    # 2) 設定ファイル(JSON: [{url, place, race_type, course_len}, ...])で一括
    python scripts/scrape_course_master.py --config data/tmp/course_targets.json

course_len は m 指定（1400）。CSV には 100m バケット（14）で保存する。
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.constants._course_master import COURSE_MASTER_COLS  # noqa: E402
from src.constants._local_paths import LocalPaths  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# 直線距離 / 高低差 / 1コーナーまで距離 のラベル付き数値を抽出する正規表現。
# ラベルと数値の間に助詞・約・記号（は/が/の/：/約 等）が入りうるため、
# 非数字フィラーを最大 8 文字まで非貪欲に許容してから数値を取る。
_STRAIGHT_RE = re.compile(
    r"直線(?:距離|の長さ)?[^0-9]{0,8}?([\d,]+(?:\.\d+)?)\s*(?:m|ｍ|メートル)"
)
_ELEV_RE = re.compile(
    r"高低差[^0-9]{0,8}?([\d]+(?:\.\d+)?)\s*(?:m|ｍ|メートル)"
)
_FIRST_CORNER_RE = re.compile(
    r"(?:第?\s*[1１]\s*(?:コーナー|角))(?:まで)?(?:の距離)?[^0-9]{0,8}?([\d,]+(?:\.\d+)?)\s*(?:m|ｍ|メートル)"
)
# ゴール前の坂（上り）を示す語
_FINAL_HILL_MARKERS = ["ゴール前.{0,6}(?:上り|のぼり|急坂)", "最後の直線.{0,10}(?:上り|急坂)", "ゴール前の坂"]


def _to_float(m) -> float:
    if not m:
        return float("nan")
    return float(m.group(1).replace(",", ""))


def parse_course_page(html: str) -> dict:
    """コースページの HTML/テキストから数値属性を抽出する（純関数）。

    返り値: {straight_length, elevation_diff, has_final_hill, first_corner_dist}。
    見つからない項目は NaN（has_final_hill は 0/1、判定不能は NaN）。
    """
    if not html:
        return {
            "straight_length": float("nan"), "elevation_diff": float("nan"),
            "has_final_hill": float("nan"), "first_corner_dist": float("nan"),
        }
    # タグを除いてテキスト化（軽量: 属性内の数値誤検出を避ける）
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text)

    straight = _to_float(_STRAIGHT_RE.search(text))
    elev = _to_float(_ELEV_RE.search(text))
    first_corner = _to_float(_FIRST_CORNER_RE.search(text))

    hill = any(re.search(p, text) for p in _FINAL_HILL_MARKERS)
    # 坂の言及も高低差も無ければ判定不能(NaN)、言及あり→1、高低差ありで言及無し→0
    if hill:
        has_hill: float = 1.0
    elif elev == elev:  # 高低差が取れている（NaN でない）
        has_hill = 0.0
    else:
        has_hill = float("nan")

    return {
        "straight_length": straight,
        "elevation_diff": elev,
        "has_final_hill": has_hill,
        "first_corner_dist": first_corner,
    }


def build_row(html: str, place: str, race_type: str, course_len_m) -> dict:
    """1 コースの CSV 行（course_master スキーマ）を組み立てる。"""
    vals = parse_course_page(html)
    bucket = int(float(course_len_m)) // 100 if str(course_len_m).strip() else float("nan")
    row = {
        "place_code": str(place).zfill(2),
        "race_type": race_type,
        "course_len": bucket,
        **vals,
    }
    return {c: row.get(c) for c in COURSE_MASTER_COLS}


def _fetch(url: str) -> str:
    """PlaywrightScraper + レート制限で 1 ページ取得（実 I/O）。"""
    from src.preparing._rate_limiter import polite_interval  # noqa: PLC0415
    from src.preparing._scraper import PlaywrightScraper  # noqa: PLC0415

    polite_interval(1.0)
    return PlaywrightScraper().fetch_sync(url)


def main() -> None:
    import pandas as pd

    parser = argparse.ArgumentParser(description="コース形状マスタを公式サイトからスクレイプ")
    parser.add_argument("--config", help="対象一覧 JSON [{url,place,race_type,course_len(m)}...]")
    parser.add_argument("--url", help="単発取得の URL（--probe 用）")
    parser.add_argument("--place", help="開催コード(2桁)")
    parser.add_argument("--race-type", help="芝/ダート/障")
    parser.add_argument("--course-len", help="距離(m)")
    parser.add_argument("--probe", action="store_true", help="1ページ取得し抽出結果を表示（保存しない）")
    parser.add_argument("--out", default=LocalPaths.COURSE_MASTER_PATH, help="出力 CSV")
    args = parser.parse_args()

    if args.probe:
        if not args.url:
            parser.error("--probe には --url が必要です")
        html = _fetch(args.url)
        vals = parse_course_page(html)
        logger.info("抽出結果: %s", vals)
        logger.info("いずれも NaN の場合は正規表現/URL を実 DOM に合わせて調整してください")
        return

    if not args.config:
        parser.error("--config か --probe を指定してください")
    targets = json.loads(Path(args.config).read_text(encoding="utf-8"))
    rows = []
    for t in targets:
        logger.info("fetching %s (%s %s %sm)", t["url"], t["place"], t["race_type"], t["course_len"])
        try:
            html = _fetch(t["url"])
        except Exception as e:  # noqa: BLE001
            logger.warning("取得失敗 %s: %s", t["url"], e)
            continue
        rows.append(build_row(html, t["place"], t["race_type"], t["course_len"]))

    if not rows:
        logger.warning("取得できた行がありません")
        return
    df = pd.DataFrame(rows, columns=COURSE_MASTER_COLS)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
    logger.info("保存: %s (%d 行)", args.out, len(df))


if __name__ == "__main__":
    main()
