"""レース当日ページ（調教評価 / パドック / 厩舎コメント）のパーサ。

netkeiba の当日情報ページから、**無料**かつ**事前確定（リーク無し）**の評価・コメントを
`(race_id, 馬番)` ロング形式で抽出する。いずれも馬番で results に結合できるため
horse_id 解決は不要だが、リンクがあれば horse_id も併せて拾う。

対象（実 DOM 確認済み）:
- 調教評価   `race.../race/oikiri.html?race_id=`（table.OikiriTable）
    枠 / 馬番(td.Umaban) / 印 / 馬名(a→horse_id) / 評価(td.Training_Critic) / 映像(td[class^=Rank_])
    ※ oikiri_final(type=2) の「調教タイム」はプレミアム → 取得しない。評価/映像は default ページから。
- パドック   `race.../race/paddock.html?race_id=`（table.Paddock_Table、注目馬のみ＝sparse）
    枠 / 馬番(td.Waku) / 馬名(td.Horse_Name) / 評価(td.Hyoka: A/B/穴) / コメント(td.Comment)
- 厩舎コメント `race.../race/comment.html?race_id=`（#All_Comment_Table）
    枠 / 馬番(td.Waku) / 馬名(td.Horse_Name) / コメント(td.txt_l) / 評価(td.Hyoka)

レイヤ: preparing。bs4 は遅延 import。
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)

_HORSE_ID_RE = re.compile(r"/horse/(\d+)")


def _soup(html: str) -> Any:
    from bs4 import BeautifulSoup

    return BeautifulSoup(html or "", "html.parser")


def _text(node: Any) -> str:
    if node is None:
        return ""
    return re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip()


def _to_int(s: str) -> Optional[int]:
    m = re.search(r"\d+", s or "")
    return int(m.group(0)) if m else None


def _horse_id_in(row: Any) -> str:
    """行内の最初の /horse/<id> リンクから horse_id を返す（無ければ ''）。"""
    for a in row.select("a[href]"):
        m = _HORSE_ID_RE.search(a.get("href", ""))
        if m:
            return m.group(1)
    return ""


def parse_training(html: str, race_id: str) -> pd.DataFrame:
    """調教評価ページ → DataFrame[race_id, 馬番, horse_id, 調教評価, 映像グレード]。

    評価（叩き良化 等）と映像グレード（A/B/C）はリーク無しの当日仕上がりシグナル。
    """
    cols = ["race_id", "馬番", "horse_id", "調教評価", "映像グレード"]
    soup = _soup(html)
    table = soup.select_one("table.OikiriTable")
    if table is None:
        return pd.DataFrame(columns=cols)

    rows: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        umaban_td = tr.find("td", class_="Umaban")
        if umaban_td is None:
            continue  # ヘッダ等
        umaban = _to_int(_text(umaban_td))
        if umaban is None:
            continue
        critic = tr.find("td", class_="Training_Critic")
        # 映像グレード: class が "Rank_..." のセル（テキストは A/B/C）。
        # bs4 の class_=callable は版差があるため td を手動走査して判定する。
        grade_td = None
        for td in tr.find_all("td"):
            classes = td.get("class") or []
            if any(x.startswith("Rank_") for x in classes):
                grade_td = td
                break
        rows.append(
            {
                "race_id": race_id,
                "馬番": umaban,
                "horse_id": _horse_id_in(tr),
                "調教評価": _text(critic) or None,
                "映像グレード": _text(grade_td) or None,
            }
        )
    return pd.DataFrame(rows, columns=cols)


def parse_paddock(html: str, race_id: str) -> pd.DataFrame:
    """パドックページ → DataFrame[race_id, 馬番, horse_id, パドック評価, パドックコメント]。

    注目馬のみ掲載（全頭は出ない＝sparse）。評価は A/B/穴 の順序グレード。
    """
    cols = ["race_id", "馬番", "horse_id", "パドック評価", "パドックコメント"]
    soup = _soup(html)
    rows: list[dict[str, Any]] = []
    for table in soup.select("table.Paddock_Table"):
        for tr in table.find_all("tr"):
            umaban_td = tr.find("td", class_="Waku")  # 馬番セル（枠は Waku<N>）
            if umaban_td is None:
                continue
            umaban = _to_int(_text(umaban_td))
            if umaban is None:
                continue
            rows.append(
                {
                    "race_id": race_id,
                    "馬番": umaban,
                    "horse_id": _horse_id_in(tr),
                    "パドック評価": _text(tr.find("td", class_="Hyoka")) or None,
                    "パドックコメント": _text(tr.find("td", class_="Comment")) or None,
                }
            )
    return pd.DataFrame(rows, columns=cols)


def parse_comments(html: str, race_id: str) -> pd.DataFrame:
    """厩舎コメントページ → DataFrame[race_id, 馬番, horse_id, 厩舎コメント, コメント評価]。"""
    cols = ["race_id", "馬番", "horse_id", "厩舎コメント", "コメント評価"]
    soup = _soup(html)
    table = soup.select_one("#All_Comment_Table") or soup.select_one("table.Stable_Comment")
    if table is None:
        return pd.DataFrame(columns=cols)

    rows: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        umaban_td = tr.find("td", class_="Waku")
        if umaban_td is None:
            continue
        umaban = _to_int(_text(umaban_td))
        if umaban is None:
            continue
        # コメントセルは小文字 txt_l（パドックの Txt_L と区別）
        comment_td = tr.find("td", class_="txt_l") or tr.find("td", class_="Txt_L")
        rows.append(
            {
                "race_id": race_id,
                "馬番": umaban,
                "horse_id": _horse_id_in(tr),
                "厩舎コメント": _text(comment_td) or None,
                "コメント評価": _text(tr.find("td", class_="Hyoka")) or None,
            }
        )
    return pd.DataFrame(rows, columns=cols)


# ---------------------------------------------------------------------------
# 取得層（スクレイパ + 永続化）— OddsSnapshotScraper を範に取る
# ---------------------------------------------------------------------------

# note_type → (URL テンプレート, 描画待ちセレクタ, パーサ)
_NOTE_SPECS: dict[str, tuple[str, str, Any]] = {
    "training": (
        "https://race.netkeiba.com/race/oikiri.html?race_id={race_id}",
        "table.OikiriTable",
        parse_training,
    ),
    "paddock": (
        "https://race.netkeiba.com/race/paddock.html?race_id={race_id}",
        "table.Paddock_Table",
        parse_paddock,
    ),
    "comment": (
        "https://race.netkeiba.com/race/comment.html?race_id={race_id}",
        "#All_Comment_Table",
        parse_comments,
    ),
}


class RaceDayNotesScraper:
    """調教評価 / パドック / 厩舎コメントページを取得して raw DataFrame を返す。

    OddsSnapshotScraper と同じく、PlaywrightScraper を DI 可能にして（テスト時は
    `scraper=` にダミーを渡す）、URL 構築・取得・パースを 1 メソッドに集約する。
    取得失敗・パース 0 件は空 DataFrame を返し、例外でバッチを止めない。
    """

    def __init__(self, scraper: Any = None) -> None:
        self._scraper = scraper

    def _ensure_scraper(self) -> Any:
        if self._scraper is None:
            from src.preparing._scraper import PlaywrightScraper

            self._scraper = PlaywrightScraper()
        return self._scraper

    def capture(self, race_id: str, note_type: str) -> pd.DataFrame:
        """1 ページを取得・パースして (race_id, 馬番) ロング DataFrame を返す。"""
        if note_type not in _NOTE_SPECS:
            raise ValueError(f"unknown note_type: {note_type}")
        url_tmpl, selector, parse_fn = _NOTE_SPECS[note_type]
        url = url_tmpl.format(race_id=race_id)
        try:
            html = self._ensure_scraper().fetch_sync(url, wait_selector=selector)
        except Exception as e:  # noqa: BLE001 — 取得失敗はスキップ（空を返す）
            logger.warning("race_day_notes 取得失敗 type=%s race_id=%s: %s", note_type, race_id, e)
            return parse_fn("", race_id)  # 空 HTML → 型付き空 DataFrame
        return parse_fn(html, race_id)


def persist_notes(df: pd.DataFrame, pickle_path: str) -> int:
    """(race_id, 馬番) ロング DataFrame を race_id index にして raw pickle(+DB) に反映する。

    `update_rawdata` は index で dedup するため、results と同様に race_id を index に置く。
    同一 race_id の旧行は総入替される（再取得で最新に更新）。空なら何もしない。
    """
    if df is None or df.empty:
        return 0
    from src.preparing._get_rawdata import update_rawdata

    indexed = df.set_index("race_id")
    update_rawdata(pickle_path, indexed)
    return len(indexed)

