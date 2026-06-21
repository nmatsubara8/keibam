"""HTML 構造の要約 — スクレイプした生 HTML からパーサ設計の素材を抽出する前処理。

新規ページ（調教 oikiri / パドック / 厩舎コメント / 人物ページ / 馬一覧 等）の
パーサを起こすには、実 DOM の「どんな table があり、summary/class/id は何で、
ヘッダ列は何か」を把握する必要がある。本モジュールはその構造を機械的に要約する。

ネットワークに依存しない純粋関数群（テスト可能）。実取得は `fetch_html_samples.py`
が PlaywrightScraper で行い、その HTML を本モジュールで要約してレポート化する。

レイヤ: preparing。bs4 は遅延 import（CI のトップレベル import 失敗を避ける）。
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # 型注釈用（実行時 import しない）
    from bs4 import BeautifulSoup


def _soup(html: str) -> "BeautifulSoup":
    from bs4 import BeautifulSoup

    return BeautifulSoup(html or "", "html.parser")


def _clean(s: str | None) -> str:
    """セルテキストの空白を正規化する。"""
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()


def summarize_tables(html: str, max_rows_scan: int = 3) -> list[dict[str, Any]]:
    """HTML 内の各 <table> の構造を要約する。

    返す各 dict: index / summary / caption / class / id / n_rows / n_cols /
    headers（最初のヘッダ行のセル）/ sample_row（最初のデータ行のセル）。
    """
    soup = _soup(html)
    out: list[dict[str, Any]] = []
    for i, table in enumerate(soup.find_all("table")):
        rows = table.find_all("tr")
        caption = table.find("caption")
        # ヘッダ: <th> を持つ最初の行、無ければ最初の行
        headers: list[str] = []
        for tr in rows[: max_rows_scan + 1]:
            ths = tr.find_all("th")
            if ths:
                headers = [_clean(th.get_text()) for th in ths]
                break
        # サンプルデータ行: <td> を持つ最初の行
        sample: list[str] = []
        for tr in rows:
            tds = tr.find_all("td")
            if tds:
                sample = [_clean(td.get_text()) for td in tds]
                break
        n_cols = max(
            (len(tr.find_all(["td", "th"])) for tr in rows[: max_rows_scan + 1]),
            default=0,
        )
        out.append(
            {
                "index": i,
                "summary": _clean(table.get("summary")),
                "caption": _clean(caption.get_text()) if caption else "",
                "class": " ".join(table.get("class", []) or []),
                "id": _clean(table.get("id")),
                "n_rows": len(rows),
                "n_cols": n_cols,
                "headers": headers,
                "sample_row": sample,
            }
        )
    return out


def summarize_repeated_containers(
    html: str, min_repeat: int = 3, max_report: int = 12
) -> list[dict[str, Any]]:
    """繰り返し子要素を持つコンテナ（div/ul/ol カード or 行）を要約する。

    `<table>` でない一覧（予想印・カード型 UI 等）の構造を拾うための補助。
    親要素ごとに「同一 class を持つ直下子要素」を数え、`min_repeat` 回以上
    繰り返すものを「行/カードの集合」とみなして報告する。

    返す各 dict: parent_tag / parent_class / parent_id / child_tag /
    child_class / count / sample（最初の子のテキスト抜粋）。
    """
    soup = _soup(html)
    out: list[dict[str, Any]] = []
    for parent in soup.find_all(["div", "ul", "ol", "section", "tbody"]):
        # 直下の子要素を (tag, class) でグルーピングして繰り返し数を数える
        groups: dict[tuple[str, str], list[Any]] = {}
        for child in parent.find_all(recursive=False):
            if child.name not in ("div", "li", "tr", "a", "dl", "span"):
                continue
            key = (child.name, " ".join(child.get("class", []) or []))
            if not key[1]:
                continue  # class 無しの子は対象外（ノイズ）
            groups.setdefault(key, []).append(child)
        for (child_tag, child_class), children in groups.items():
            if len(children) < min_repeat:
                continue
            sample = _clean(children[0].get_text())[:120]
            out.append(
                {
                    "parent_tag": parent.name,
                    "parent_class": " ".join(parent.get("class", []) or []),
                    "parent_id": _clean(parent.get("id")),
                    "child_tag": child_tag,
                    "child_class": child_class,
                    "count": len(children),
                    "sample": sample,
                }
            )
    # 繰り返し数が多い順。重複（同じ child_class が複数親で出る）も件数で上位を優先
    out.sort(key=lambda d: d["count"], reverse=True)
    return out[:max_report]


def find_element_ids(html: str, pattern: str) -> list[str]:
    """正規表現にマッチする要素 id を重複なし・出現順で返す（odds-* 等の手掛かり）。"""
    seen: dict[str, None] = {}
    for m in re.finditer(r'id="([^"]+)"', html or ""):
        eid = m.group(1)
        if re.search(pattern, eid) and eid not in seen:
            seen[eid] = None
    return list(seen)


def find_premium_markers(html: str) -> dict[str, int]:
    """プレミアム/ログイン要求の手掛かり文言・マークの出現数を数える。"""
    markers = (
        "プレミアム",
        "登録して",
        "ログイン",
        "会員登録",
        "ico_premium",
        "続きは",
    )
    counts: dict[str, int] = {}
    for m in markers:
        n = (html or "").count(m)
        if n:
            counts[m] = n
    return counts


def structure_report(html: str, url: str = "", min_rows: int = 1) -> str:
    """HTML の構造要約をパーサ設計用の読みやすいテキストにして返す。

    `min_rows` 行未満の table（レイアウト用の小表）は省略してノイズを減らす。
    """
    lines: list[str] = []
    lines.append("=" * 78)
    lines.append(f"URL : {url}")
    lines.append(f"html長 : {len(html or '')}")

    prem = find_premium_markers(html)
    if prem:
        lines.append(f"プレミアム手掛かり : {prem}")

    odds_ids = find_element_ids(html, r"^odds-")
    if odds_ids:
        lines.append(f"odds-* id 例 : {odds_ids[:8]}{' …' if len(odds_ids) > 8 else ''}")

    tables = [t for t in summarize_tables(html) if t["n_rows"] >= min_rows]
    lines.append(f"table 数（{min_rows}行以上）: {len(tables)}")
    for t in tables:
        lines.append("-" * 60)
        meta = []
        if t["summary"]:
            meta.append(f'summary="{t["summary"]}"')
        if t["caption"]:
            meta.append(f'caption="{t["caption"]}"')
        if t["class"]:
            meta.append(f'class="{t["class"]}"')
        if t["id"]:
            meta.append(f'id="{t["id"]}"')
        lines.append(f"[table#{t['index']}] {' '.join(meta) or '(属性なし)'}")
        lines.append(f"  行x列 : {t['n_rows']} x {t['n_cols']}")
        if t["headers"]:
            lines.append(f"  ヘッダ : {t['headers']}")
        if t["sample_row"]:
            sample = t["sample_row"][:12]
            tail = " …" if len(t["sample_row"]) > 12 else ""
            lines.append(f"  サンプル行 : {sample}{tail}")

    # table が乏しいページ（予想印・カード型）向けに div/list の繰り返し構造も報告
    containers = summarize_repeated_containers(html)
    if containers:
        lines.append("-" * 60)
        lines.append(f"繰り返しコンテナ（div/list ベース）: {len(containers)} 種")
        for c in containers:
            parent = c["parent_class"] or c["parent_id"] or "(無属性)"
            lines.append(
                f"  <{c['parent_tag']} {parent}> 直下に "
                f"<{c['child_tag']} class=\"{c['child_class']}\"> × {c['count']}"
            )
            if c["sample"]:
                lines.append(f"    例 : {c['sample']}")
    return "\n".join(lines)
