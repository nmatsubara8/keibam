"""Phase 9-rev: JRA 公式コースページからコース形状マスタを自動生成する。

feature_expansion_plan.md の Phase 9。手入力せず、JRA 10 場の
https://www.jra.go.jp/facilities/race/{slug}/course/index.html を巡回して
「コースデータ」表（幾何）と「コース紹介」プロセ（定性プロファイル）を抽出し
data/master/course_master.csv を生成する。

設計: フェッチ(I/O)と解析(純関数)を分離。ページは cp932(Shift-JIS)。幾何は A コース
代表値に集約する（区分 A/B/C の直線差は僅少）。プロセ由来のプロファイルは
シミュレーション環境パラメータ・出走馬×コース相性評価に用いる。

使い方（JRA にアクセス可能な環境で）:
    python scripts/scrape_course_master.py                 # 10 場を巡回して CSV 生成
    python scripts/scrape_course_master.py --tracks tokyo nakayama
    python scripts/scrape_course_master.py --probe --slug sapporo   # 1 場の抽出結果表示
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.constants._course_master import COURSE_MASTER_COLS  # noqa: E402
from src.constants._local_paths import LocalPaths  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# JRA 10 場の URL slug → 開催コード（Master.PLACE_DICT と整合）
TRACK_SLUG_TO_PLACE: dict = {
    "sapporo": "01", "hakodate": "02", "fukushima": "03", "niigata": "04",
    "tokyo": "05", "nakayama": "06", "chukyo": "07", "kyoto": "08",
    "hanshin": "09", "kokura": "10",
}
_URL = "https://www.jra.go.jp/facilities/race/{slug}/course/index.html"


# ── テキスト/数値ユーティリティ ─────────────────────────────────
def _strip(html: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))


def _num(s: str) -> float:
    m = re.search(r"([\d,]+(?:\.\d+)?)", s)
    return float(m.group(1).replace(",", "")) if m else float("nan")


def _width_range(s: str) -> tuple:
    ns = [float(x) for x in re.findall(r"(\d+(?:\.\d+)?)", s)]
    return (min(ns), max(ns)) if ns else (float("nan"), float("nan"))


def _table_rows(table_html: str) -> list:
    out = []
    for tr in re.findall(r"<tr.*?</tr>", table_html, re.S):
        cells = [_strip(c).strip() for c in re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", tr, re.S)]
        cells = [c for c in cells if c]
        if cells:
            out.append(cells)
    return out


# ── 幾何（表）の抽出 ────────────────────────────────────────────
def parse_geometry(block_html: str) -> dict:
    """1 サーフェスのブロック HTML から幾何（直線/高低差/一周/幅員）を抽出する。

    レイアウト差（芝=高低差表＋A/B/C区分表 / ダート=単一結合表）を、ヘッダ名→列で
    吸収する。区分表はコース区分 A の行を採用する。
    """
    geo: dict = {}
    for tb in re.findall(r"<table.*?</table>", block_html, re.S):
        rs = _table_rows(tb)
        if len(rs) < 2:
            continue
        header = rs[0]

        def col(name: str, _h=header):
            for i, h in enumerate(_h):
                if name in h:
                    return i
            return None

        ci = col("コース")
        data = None
        if ci is not None:
            for r in rs[1:]:
                if len(r) > ci and r[ci].strip() == "A":
                    data = r
                    break
        if data is None:
            data = rs[1]

        for name, key in (("一周距離", "lap_length"), ("直線距離", "straight_length"),
                          ("高低差", "elevation_diff")):
            i = col(name)
            if i is not None and i < len(data):
                geo.setdefault(key, _num(data[i]))
        wi = col("幅員")
        if wi is not None and wi < len(data):
            wmin, wmax = _width_range(data[wi])
            geo.setdefault("width_min", wmin)
            geo.setdefault("width_max", wmax)
    return geo


def parse_geometry_by_surface(html: str) -> dict:
    """コースデータ節を芝/ダートに分割して幾何を返す: {"芝": {...}, "ダート": {...}}。"""
    idx = html.find("コースデータ")
    seg = html[idx: idx + 12000] if idx >= 0 else html
    d = seg.find("ダートコース")
    if d < 0:
        return {"芝": parse_geometry(seg), "ダート": {}}
    return {"芝": parse_geometry(seg[:d]), "ダート": parse_geometry(seg[d:])}


# ── 定性プロファイル（プロセ）の抽出 ───────────────────────────
def parse_course_prose(text: str) -> dict:
    """コース紹介プロセから定性プロファイル（脚質バイアス・馬場傾向・芝種等）を抽出する。"""
    def has(*ks) -> bool:
        return any(k in text for k in ks)

    p: dict = {}
    p["turf_type_code"] = 1.0 if "洋芝" in text else (0.0 if "野芝" in text else float("nan"))

    if has("半径が大き", "緩やかで大きなカーブ", "大きなカーブ", "滑らかに回れ", "緩やかなカーブ"):
        p["corner_radius_large"] = 1.0
    elif has("急なカーブ", "きついカーブ", "半径が小さ"):
        p["corner_radius_large"] = 0.0
    else:
        p["corner_radius_large"] = float("nan")

    if "スパイラルカーブ" in text:
        seg = text[text.find("スパイラルカーブ"): text.find("スパイラルカーブ") + 40]
        neg = "採用" in seg and ("されておらず" in seg or "ない" in seg)
        p["has_spiral_curve"] = 0.0 if neg else 1.0
    else:
        p["has_spiral_curve"] = float("nan")

    front = len(re.findall(
        r"前残り|逃げ切り|先行(?:有利|勢)|逃げ・先行|前が(?:止まら|残)|"
        r"追い?込み[^。]{0,15}(?:苦戦|不利|届か)|後方[^。]{0,12}(?:苦戦|不利)", text))
    closer = len(re.findall(
        r"差し[^。]{0,8}(?:有利|決ま|台頭)|追い?込み[^。]{0,8}(?:有利|決ま|得意)|外差し", text))
    p["run_style_bias"] = float(front - closer)

    if has("時計がかかる", "時計を要", "タフな馬場", "時計のかかる", "遅くなりがち"):
        p["time_bias"] = -1.0
    elif has("高速", "時計が速", "スピード決着"):
        p["time_bias"] = 1.0
    else:
        p["time_bias"] = 0.0

    if has("水はけ") and has("抜群", "良", "よく", "いい") and not has("水はけが悪", "水はけは悪"):
        p["drainage_good"] = 1.0
    elif has("雨の影響を受けることが多い", "道悪", "水はけが悪", "時計を要するコンディション"):
        p["drainage_good"] = 0.0
    elif has("重になることは滅多", "雨の影響は受けにくい", "重馬場になりにくい"):
        p["drainage_good"] = 1.0
    else:
        p["drainage_good"] = float("nan")
    return p


def parse_turn_direction(html: str) -> float:
    """回り（0=右, 1=左, NaN=不明）を返す。"""
    if "右回り" in html:
        return 0.0
    if "左回り" in html:
        return 1.0
    return float("nan")


def _prose_text(html: str) -> str:
    ps = [_strip(p).strip() for p in re.findall(r"<p[^>]*>(.*?)</p>", html, re.S)]
    return " ".join(p for p in ps if len(p) > 60 and "Copyright" not in p)


def build_rows(html: str, place_code: str) -> list:
    """1 場の HTML から芝/ダートの course_master 行（幾何＋プロファイル＋回り）を作る。"""
    geo = parse_geometry_by_surface(html)
    turn = parse_turn_direction(html)
    profile = parse_course_prose(_prose_text(html))
    rows = []
    for surface in ("芝", "ダート"):
        g = geo.get(surface) or {}
        if not g:
            continue
        row = {"place_code": place_code, "race_type": surface, "turn_direction": turn}
        row.update(g)
        prof = dict(profile)
        if surface != "芝":
            prof["turf_type_code"] = float("nan")  # 芝種はダートに無関係
        row.update(prof)
        rows.append({c: row.get(c, float("nan")) for c in COURSE_MASTER_COLS})
    return rows


def _fetch(slug: str) -> str:
    """JRA コースページを取得して cp932 デコードした HTML を返す（実 I/O）。"""
    import urllib.request

    from src.preparing._rate_limiter import polite_interval  # noqa: PLC0415

    polite_interval(1.0)
    url = _URL.format(slug=slug)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 keibam-course-scraper"})
    with urllib.request.urlopen(req, timeout=40) as resp:  # noqa: S310
        return resp.read().decode("cp932", errors="replace")


def main() -> None:
    import pandas as pd

    parser = argparse.ArgumentParser(description="JRA コースページからコース形状マスタを生成")
    parser.add_argument("--tracks", nargs="*", help="対象 slug（省略時は 10 場すべて）")
    parser.add_argument("--probe", action="store_true", help="1 場の抽出結果を表示（保存しない）")
    parser.add_argument("--slug", help="--probe 用の slug")
    parser.add_argument("--out", default=LocalPaths.COURSE_MASTER_PATH, help="出力 CSV")
    args = parser.parse_args()

    if args.probe:
        slug = args.slug or "sapporo"
        html = _fetch(slug)
        place = TRACK_SLUG_TO_PLACE.get(slug, "??")
        logger.info("turn=%s", parse_turn_direction(html))
        logger.info("geometry=%s", parse_geometry_by_surface(html))
        logger.info("prose=%s", parse_course_prose(_prose_text(html)))
        logger.info("rows=%s", build_rows(html, place))
        return

    slugs = args.tracks or list(TRACK_SLUG_TO_PLACE.keys())
    rows = []
    for slug in slugs:
        place = TRACK_SLUG_TO_PLACE.get(slug)
        if place is None:
            logger.warning("未知の slug: %s", slug)
            continue
        logger.info("fetching %s (place=%s)", slug, place)
        try:
            html = _fetch(slug)
        except Exception as e:  # noqa: BLE001
            logger.warning("取得失敗 %s: %s", slug, e)
            continue
        rows.extend(build_rows(html, place))

    if not rows:
        logger.warning("取得できた行がありません")
        return
    df = pd.DataFrame(rows, columns=COURSE_MASTER_COLS)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
    logger.info("保存: %s (%d 行)", args.out, len(df))


if __name__ == "__main__":
    main()
