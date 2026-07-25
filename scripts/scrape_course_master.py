"""JRA 公式コースページからコース形状マスタを 2 段階で生成する。

手入力せず、JRA 10 場の https://www.jra.go.jp/facilities/race/{slug}/course/index.html を
巡回する。「コース紹介」欄は 2 段階で処理する:

  Stage1（取得・素テキスト保存）: フェッチして「コース紹介」プロセを **素テキストのまま**
    保存する（幾何の表・回りも facts として同時保存）。→ data/master/course_prose.json
  Stage2（分析・要点抽出）: 保存済み素テキストから定性プロファイル（脚質バイアス/馬場傾向/
    芝種など）を抽出し、幾何と結合して最終マスタを出力する。→ data/master/course_master.csv

この分離により、**再フェッチせずに Stage2 のマーカーを改善→再分析**できる（素テキストは
将来の NLP/LLM 分析にも再利用可能）。フェッチ(I/O)と解析(純関数)も分離。ページは cp932。
幾何は A コース代表値に集約する（区分 A/B/C の直線差は僅少）。

使い方（JRA にアクセス可能な環境で）:
    python scripts/scrape_course_master.py                    # Stage1→Stage2 を一括
    python scripts/scrape_course_master.py --stage fetch      # Stage1 のみ（素テキスト保存）
    python scripts/scrape_course_master.py --stage analyze    # Stage2 のみ（保存済みを再分析）
    python scripts/scrape_course_master.py --probe --slug sapporo   # 1 場の抽出結果表示
"""

from __future__ import annotations

import argparse
import json
import logging
import os
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

# 地方競馬（NAR）: 1 ページに全場（アンカー id="course_<slug>"）。UTF-8。
# アンカー slug → 開催コード（Master.PLACE_DICT と整合）。帯広(obi)=ばんえいは
# 直線 200m の障害競走で通常のコース幾何に載らないため除外する。
_NAR_URL = "https://www.keiba.go.jp/guide/course/"
NAR_ANCHOR_TO_PLACE: dict = {
    "monb": "30", "mori": "35", "mizu": "36", "uraw": "42", "funa": "43",
    "ooi": "44", "kawa": "45", "kana": "46", "kasa": "47", "nago": "48",
    "sono": "50", "hime": "51", "kout": "54", "saga": "55",
}


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

    if has("半径が大き", "緩やかで大きなカーブ", "大きなカーブ", "滑らかに回れ", "緩やかなカーブ",
           "半径がゆったり", "カーブの半径がゆったり", "ゆったりしたカーブ", "半径がゆったりし",
           "カーブが緩い", "カーブがゆるやか", "カーブが緩やか"):  # NAR 表現も
        p["corner_radius_large"] = 1.0
    elif has("急なカーブ", "きついカーブ", "半径が小さ", "タイト", "小回り", "急カーブ",
             "カーブがきつい"):
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
        r"前残り|逃げ切り|先行(?:有利|勢|有力)|逃げ・先行|前が(?:止まら|残)|前有利|"
        r"逃げ[^。]{0,8}(?:有利|優勢|残)|"
        r"追い?込み[^。]{0,15}(?:苦戦|不利|届か)|後方[^。]{0,12}(?:苦戦|不利)|"
        r"差し[^。]{0,15}(?:厳しい|届か|苦戦)|差し馬[^。]{0,15}不利", text))
    # 「前有利とはいい切れない/限らない」等の否定は前有利マッチを相殺する
    front -= len(re.findall(
        r"前有利[^。]{0,6}(?:いい切れな|言い切れな|いえな|とは限|になるとは限|とは違)", text))
    closer = len(re.findall(
        r"差し[^。]{0,10}(?:有利|決ま|台頭|水準以上|残|優勢|届く)|"
        r"追い?込み[^。]{0,10}(?:有利|決ま|得意|届|でも届)|外差し|末脚(?:比べ|勝負|自慢)|"
        r"上がり[^。]{0,10}(?:速|勝負|比べ)|直線一気|イン差し[^。]{0,8}(?:決ま|利|有利)", text))
    p["run_style_bias"] = float(front - closer)

    if has("時計がかかる", "時計を要", "タフな馬場", "時計のかかる", "遅くなりがち", "パワータイプ",
           "パワーを要", "時計を要するコンディション"):
        p["time_bias"] = -1.0
    elif has("高速", "時計が速", "スピード決着", "上がりタイムは速", "上がりが速",
             "上がりタイムが速", "時計は速"):
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


# ── 距離別コースガイド（書籍/ガイド由来プロセ）: 要点抽出 ──────────
def _upset_prone(text: str) -> float:
    """波乱度（1=荒れやすい / 0=堅い / NaN=記載なし）をガイド文から抽出する。

    「紛れも出にくい」等の否定表現を誤検出しないよう、肯定側は 紛れやす/荒れやす 等の
    明示トークンに限定し、堅さ側（展開要らず/紛れが少な/マグレも出にくい）を先に判定する。
    """
    # 堅い（波乱少）を先に判定：肯定側の部分文字列（紛れ・荒れ）との衝突を避ける
    if any(k in text for k in (
            "展開要らず", "紛れが少な", "紛れもマグレ", "マグレも出にくい", "波乱は少な",
            "堅い", "手堅", "順当", "堅く", "荒れにく", "紛れが出にく")):
        return 0.0
    # 「難しいコース」は「逃げ切りは難しい」等で誤検出するため肯定側に含めない
    if any(k in text for k in (
            "荒れやす", "荒れる", "波乱含", "紛れやす", "トリッキー", "難解")):
        return 1.0
    return float("nan")


def parse_guide_prose(text: str) -> dict:
    """ガイド文（距離固有の「コース紹介」）→ 距離別プロファイル dict。

    course_master と同じ脚質/時計/コーナー/水はけ抽出（parse_course_prose）に、
    ガイド文固有の波乱度（upset_prone）を加える。COURSE_GUIDE_VALUE_COLS のみ返す。
    """
    from src.constants._course_guide import COURSE_GUIDE_VALUE_COLS  # noqa: PLC0415

    base = parse_course_prose(text or "")
    base["upset_prone"] = _upset_prone(text or "")
    return {c: base.get(c, float("nan")) for c in COURSE_GUIDE_VALUE_COLS}


def build_guide_master(guide_in: str) -> list:
    """手入力ソース CSV（place_code, race_type, course_len_m, prose_guide）を要点抽出し、
    距離別プロファイル行（COURSE_GUIDE_MASTER_COLS）のリストを返す。

    src は生成物 CSV を読むだけにするため、プロセ解析はこのオフライン生成器に閉じる。
    """
    import csv

    from src.constants._course_guide import (  # noqa: PLC0415
        COURSE_GUIDE_MASTER_COLS,
        COURSE_GUIDE_SOURCE_COL,
    )

    if not guide_in or not os.path.exists(guide_in):
        return []
    rows = []
    with open(guide_in, encoding="utf-8") as f:
        for rec in csv.DictReader(f):
            prose = (rec.get(COURSE_GUIDE_SOURCE_COL) or "").strip()
            if not prose:
                continue
            place = str(rec.get("place_code", "")).strip().zfill(2)
            rtype = str(rec.get("race_type", "")).strip()
            try:
                clen = int(float(rec.get("course_len_m", "")))
            except (TypeError, ValueError):
                continue
            row = {"place_code": place, "race_type": rtype, "course_len_m": clen}
            row.update(parse_guide_prose(prose))
            rows.append({c: row.get(c, float("nan")) for c in COURSE_GUIDE_MASTER_COLS})
    return rows


# ── 地方競馬（NAR）: 1 ページ内の各場アンカーを解析 ─────────────
def _nar_section(html: str, anchor: str) -> str:
    """id="course_<anchor>" から次アンカーまでの区間 HTML を返す。"""
    i = html.find(f'id="course_{anchor}"')
    if i < 0:
        return ""
    nxt = html.find('id="course_', i + 10)
    return html[i: nxt] if nxt > 0 else html[i: i + 2500]


def _nar_kv(section: str) -> dict:
    """NAR の「データ名｜詳細」表を dict 化する。"""
    d: dict = {}
    for tr in re.findall(r"<tr.*?</tr>", section, re.S):
        cells = [_strip(c).strip() for c in re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", tr, re.S)]
        cells = [c for c in cells if c]
        if len(cells) >= 2:
            d[cells[0]] = cells[1]
    return d


def _nar_num_for(detail: str, surface: str) -> float:
    """詳細文字列から surface（芝/ダート）に対応する数値を取る。分割が無ければ先頭値。"""
    if not detail:
        return float("nan")
    if "平坦" in detail:
        return 0.0
    if surface and surface in detail:
        m = re.search(surface + r"[^0-9]{0,10}?([\d,]+(?:\.\d+)?)", detail)
        if m:
            return float(m.group(1).replace(",", ""))
    m = re.search(r"([\d,]+(?:\.\d+)?)", detail)
    return float(m.group(1).replace(",", "")) if m else float("nan")


def _nar_turn(detail: str) -> float:
    """回り（0=右, 1=左, 左右両回り/不明は NaN）。"""
    r, left = "右" in detail, "左" in detail
    return float("nan") if (r and left) else (0.0 if r else (1.0 if left else float("nan")))


def parse_nar_surfaces(section: str) -> list:
    """区間に存在するコース種別（ダート必ず、芝コース記載があれば芝も）。"""
    return ["ダート"] + (["芝"] if ("芝コース" in section or "外芝" in section) else [])


def parse_nar_geometry(section: str, surface: str) -> dict:
    """NAR 区間の幾何（直線/高低差/一周/幅員）を surface 別に返す。"""
    d = _nar_kv(section)
    width = _nar_num_for(d.get("幅員", ""), surface)
    return {
        "straight_length": _nar_num_for(d.get("直線距離", ""), surface),
        "elevation_diff": _nar_num_for(d.get("高低差", ""), surface),
        "lap_length": _nar_num_for(d.get("1周距離", d.get("全長", "")), surface),
        "width_min": width,
        "width_max": width,
    }


def _nar_prose(section: str) -> str:
    """NAR 区間の紹介文（表より前の説明文）を素テキストで返す。"""
    head = section.split("データ名", 1)[0]
    txt = _strip(re.sub(r'id="course_[a-z]+">', " ", head))
    return re.sub(r"\s+", " ", txt).strip()


def build_raw_records_nar(html: str) -> dict:
    """Stage1(NAR): 1 ページ HTML → {place_code: record}（JRA と同じ record 形状）。"""
    records: dict = {}
    for anchor, place in NAR_ANCHOR_TO_PLACE.items():
        seg = _nar_section(html, anchor)
        if not seg:
            continue
        d = _nar_kv(seg)
        geometry = {s: parse_nar_geometry(seg, s) for s in parse_nar_surfaces(seg)}
        records[place] = {
            "place_code": place,
            "slug": anchor,
            "turn_direction": _nar_turn(d.get("回り", "")),
            "geometry": geometry,
            "prose_raw": _nar_prose(seg),
        }
    return records


# ── Stage1: 素テキストとして取得 ───────────────────────────────
def build_raw_record(html: str, slug: str, place_code: str) -> dict:
    """Stage1: 1 場の HTML から「素テキスト（コース紹介）＋幾何 facts＋回り」を保存用に組む。

    プロセは要点抽出せず **素のまま** prose_raw に格納する（Stage2 で分析する）。
    幾何・回りは非可逆でない facts なので同時に保存し、Stage2 は HTML 不要にする。
    """
    return {
        "place_code": place_code,
        "slug": slug,
        "turn_direction": parse_turn_direction(html),
        "geometry": parse_geometry_by_surface(html),
        "prose_raw": _prose_text(html),
    }


def _json_safe(obj):
    """NaN を None に落として JSON 可搬にする（geometry の欠損セル対策）。"""
    if isinstance(obj, float):
        return None if obj != obj else obj
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_safe(v) for v in obj]
    return obj


def save_raw_prose(records: dict, path: str) -> None:
    """Stage1 の素テキスト集合（place_code→record）を JSON 保存する。"""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(
        json.dumps(_json_safe(records), ensure_ascii=False, indent=1), encoding="utf-8"
    )


def load_raw_prose(path: str) -> dict:
    """Stage1 が保存した素テキスト集合を読み込む（無ければ空）。"""
    if not path or not os.path.exists(path):
        return {}
    return json.loads(Path(path).read_text(encoding="utf-8"))


# ── Stage2: 素テキストから要点抽出して最終行に組む ─────────────
def analyze_record(record: dict) -> list:
    """Stage2: 1 場の素レコード → course_master 行（幾何＋プロセ要点抽出プロファイル）。"""
    geo = record.get("geometry") or {}
    turn = record.get("turn_direction")
    turn = float("nan") if turn is None else turn
    profile = parse_course_prose(record.get("prose_raw") or "")
    rows = []
    for surface in ("芝", "ダート"):
        g = geo.get(surface) or {}
        if not g:
            continue
        row = {"place_code": record["place_code"], "race_type": surface, "turn_direction": turn}
        row.update({k: (float("nan") if v is None else v) for k, v in g.items()})
        prof = dict(profile)
        if surface != "芝":
            prof["turf_type_code"] = float("nan")  # 芝種はダートに無関係
        row.update(prof)
        rows.append({c: row.get(c, float("nan")) for c in COURSE_MASTER_COLS})
    return rows


def build_rows(html: str, place_code: str, slug: str = "") -> list:
    """便宜: Stage1→Stage2 を 1 場ぶん通す（build_raw_record → analyze_record）。"""
    return analyze_record(build_raw_record(html, slug, place_code))


def _http_get(url: str, encoding: str) -> str:
    """URL を取得して指定エンコーディングでデコードする（レート制限付き・実 I/O）。"""
    import urllib.request

    from src.preparing._rate_limiter import polite_interval  # noqa: PLC0415

    polite_interval(1.0)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 keibam-course-scraper"})
    with urllib.request.urlopen(req, timeout=40) as resp:  # noqa: S310
        return resp.read().decode(encoding, errors="replace")


def _fetch(slug: str) -> str:
    """JRA コースページ（cp932）を取得する。"""
    return _http_get(_URL.format(slug=slug), "cp932")


def _fetch_nar() -> str:
    """NAR（地方競馬）コースページ（UTF-8・全場 1 ページ）を取得する。"""
    return _http_get(_NAR_URL, "utf-8")


def _stage_fetch(slugs: list, source: str = "both") -> dict:
    """Stage1: JRA 各場 / NAR 1 ページをフェッチして素テキスト record を集める。"""
    records: dict = {}
    if source in ("jra", "both"):
        for slug in slugs:
            place = TRACK_SLUG_TO_PLACE.get(slug)
            if place is None:
                logger.warning("未知の slug: %s", slug)
                continue
            logger.info("[stage1/jra] fetching %s (place=%s)", slug, place)
            try:
                html = _fetch(slug)
            except Exception as e:  # noqa: BLE001
                logger.warning("取得失敗 %s: %s", slug, e)
                continue
            records[place] = build_raw_record(html, slug, place)
    if source in ("nar", "both"):
        logger.info("[stage1/nar] fetching NAR 全場")
        try:
            nar = build_raw_records_nar(_fetch_nar())
            records.update(nar)
            logger.info("[stage1/nar] %d 場を取得", len(nar))
        except Exception as e:  # noqa: BLE001
            logger.warning("NAR 取得失敗: %s", e)
    return records


def _stage_analyze(records: dict) -> list:
    """Stage2: 素テキスト record 集合 → course_master 行リスト。"""
    rows = []
    for place in sorted(records):
        rows.extend(analyze_record(records[place]))
    return rows


def _generate_guide(guide_in: str, guide_out: str, pd) -> None:
    """距離別ガイド手入力 CSV → 要点抽出 → 距離別マスタ CSV を書き出す（入力なければスキップ）。"""
    from src.constants._course_guide import COURSE_GUIDE_MASTER_COLS  # noqa: PLC0415

    guide_rows = build_guide_master(guide_in)
    if not guide_rows:
        logger.info("[stage2/guide] ガイド入力なし（%s）— スキップ", guide_in)
        return
    gdf = pd.DataFrame(guide_rows, columns=COURSE_GUIDE_MASTER_COLS)
    Path(guide_out).parent.mkdir(parents=True, exist_ok=True)
    gdf.to_csv(guide_out, index=False)
    logger.info("[stage2/guide] 保存: %s (%d 行)", guide_out, len(gdf))


def main() -> None:
    import pandas as pd

    parser = argparse.ArgumentParser(
        description="JRA/NAR コースページからコース形状マスタを生成（2段階）")
    parser.add_argument("--tracks", nargs="*", help="対象 JRA slug（省略時は 10 場すべて）")
    parser.add_argument("--source", choices=["jra", "nar", "both"], default="both",
                        help="取得元（jra=中央 / nar=地方 / both=両方）")
    parser.add_argument("--stage", choices=["fetch", "analyze", "all"], default="all",
                        help="fetch=素テキスト取得のみ / analyze=保存済みを再分析のみ / all=両方")
    parser.add_argument("--probe", action="store_true", help="1 場の抽出結果を表示（保存しない）")
    parser.add_argument("--slug", help="--probe 用の JRA slug")
    parser.add_argument("--prose", default=LocalPaths.COURSE_PROSE_PATH, help="素テキスト JSON")
    parser.add_argument("--out", default=LocalPaths.COURSE_MASTER_PATH, help="出力 CSV")
    parser.add_argument("--guide-in", default=LocalPaths.COURSE_GUIDE_PATH,
                        help="距離別ガイド手入力ソース CSV")
    parser.add_argument("--guide-out", default=LocalPaths.COURSE_GUIDE_MASTER_PATH,
                        help="距離別ガイド生成マスタ CSV")
    args = parser.parse_args()

    if args.probe:
        slug = args.slug or "sapporo"
        html = _fetch(slug)
        rec = build_raw_record(html, slug, TRACK_SLUG_TO_PLACE.get(slug, "??"))
        logger.info("turn=%s geometry=%s", rec["turn_direction"], rec["geometry"])
        logger.info("prose_raw(先頭120字)=%s", rec["prose_raw"][:120])
        logger.info("profile=%s", parse_course_prose(rec["prose_raw"]))
        logger.info("rows=%s", analyze_record(rec))
        return

    slugs = args.tracks or list(TRACK_SLUG_TO_PLACE.keys())

    # Stage1: 取得して素テキストを保存
    if args.stage in ("fetch", "all"):
        records = _stage_fetch(slugs, args.source)
        if not records:
            logger.warning("取得できた場がありません")
            return
        save_raw_prose(records, args.prose)
        logger.info("[stage1] 素テキスト保存: %s (%d 場)", args.prose, len(records))
    else:
        records = load_raw_prose(args.prose)
        logger.info("[stage2] 素テキスト読込: %s (%d 場)", args.prose, len(records))

    if args.stage == "fetch":
        return

    # Stage2b: 距離別ガイド（書籍/ガイド由来・手入力）を要点抽出（フェッチ不要・独立）
    _generate_guide(args.guide_in, args.guide_out, pd)

    # Stage2: 素テキストを分析して最終マスタを出力
    rows = _stage_analyze(records)
    if not rows:
        logger.warning("コース紹介の分析対象がありません（ガイドは生成済み）")
        return
    df = pd.DataFrame(rows, columns=COURSE_MASTER_COLS)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
    logger.info("[stage2] 保存: %s (%d 行)", args.out, len(df))


if __name__ == "__main__":
    main()
