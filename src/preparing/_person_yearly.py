"""人物（騎手/調教師/馬主/生産者）の年度別成績パーサ。

netkeiba db の ``/{jockey,trainer,owner,breeder}/result/<id>/`` ページ（**EUC-JP**）の
``table.race_table_01`` を年度別ロング DataFrame に変換する。as-of（前年まで）で結合して
人物スキル特徴に使う。

実 HTML（2026 行）で確認した列位置（td、21列）:
    [0]年度 [1]順位 [2]1着 [3]2着 [4]3着 [5]着外 [6]重賞出走 [7]重賞勝利 [8]特別出走
    [9]特別勝利 [10]平場出走 [11]平場勝利 [12]芝出走 [13]芝勝利 [14]ダート出走
    [15]ダート勝利 [16]勝率 [17]連対率 [18]複勝率 [19]収得賞金(万円) [20]代表馬

レイヤ: preparing。pandas/requests/bs4 のみ。取得失敗・構造差異は空 DataFrame（堅牢性優先）。
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# entity_type → URL テンプレート（いずれも EUC-JP・同じ race_table_01 構造を想定）
_ENTITY_URL: dict[str, str] = {
    "jockey": "https://db.netkeiba.com/jockey/result/{eid}/",
    "trainer": "https://db.netkeiba.com/trainer/result/{eid}/",
    "owner": "https://db.netkeiba.com/owner/result/{eid}/",
    "breeder": "https://db.netkeiba.com/breeder/result/{eid}/",
}

_LONG_COLUMNS = [
    "entity_type", "entity_id", "year",
    "出走回数", "勝利数", "勝率", "連対率", "複勝率",
    "芝勝率", "ダート勝率", "重賞勝利", "収得賞金",
]


def _num(text: Any) -> float:
    """カンマ・空白除去して数値化（'.225' や '5,145,314.6' も）。不可は NaN。"""
    if text is None:
        return float("nan")
    s = str(text).replace(",", "").strip()
    if s in ("", "-"):
        return float("nan")
    try:
        return float(s)
    except ValueError:
        return float("nan")


def parse_person_yearly(html: str, entity_type: str, entity_id: str) -> pd.DataFrame:
    """人物の result.html を年度別ロング DataFrame に変換する（呼び出し側で EUC-JP デコード）。

    年度が 4 桁の行のみ採用（『累計』『年度』等のヘッダ行は除外）。芝/ダート勝率は
    勝利数÷出走数で算出。出走回数=1着+2着+3着+着外、勝利数=1着。
    """
    from bs4 import BeautifulSoup

    if not html:
        return pd.DataFrame(columns=_LONG_COLUMNS)
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="race_table_01")
    if table is None:
        return pd.DataFrame(columns=_LONG_COLUMNS)

    rows: list[dict] = []
    for tr in table.find_all("tr"):
        cells = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]
        if len(cells) < 20 or not re.fullmatch(r"\d{4}", cells[0] or ""):
            continue  # 累計・ヘッダ・空行をスキップ
        ichi, ni, san, gai = _num(cells[2]), _num(cells[3]), _num(cells[4]), _num(cells[5])
        shiba_run, shiba_win = _num(cells[12]), _num(cells[13])
        dirt_run, dirt_win = _num(cells[14]), _num(cells[15])
        rows.append({
            "entity_type": entity_type,
            "entity_id": str(entity_id),
            "year": int(cells[0]),
            "出走回数": ichi + ni + san + gai,
            "勝利数": ichi,
            "勝率": _num(cells[16]),
            "連対率": _num(cells[17]),
            "複勝率": _num(cells[18]),
            "芝勝率": (shiba_win / shiba_run) if shiba_run else float("nan"),
            "ダート勝率": (dirt_win / dirt_run) if dirt_run else float("nan"),
            "重賞勝利": _num(cells[7]),
            "収得賞金": _num(cells[19]),
        })
    if not rows:
        return pd.DataFrame(columns=_LONG_COLUMNS)
    return pd.DataFrame(rows, columns=_LONG_COLUMNS)


def fetch_person_yearly(
    entity_type: str, entity_id: str, *, timeout: float = 10.0, session: Optional[Any] = None
) -> pd.DataFrame:
    """人物の年度別成績を取得する（EUC-JP・匿名 GET）。失敗時は空 DataFrame。"""
    import requests

    from src.preparing._scraper import _DEFAULT_USER_AGENT

    tmpl = _ENTITY_URL.get(entity_type)
    if tmpl is None:
        logger.warning("person_yearly: 未知の entity_type=%s", entity_type)
        return pd.DataFrame(columns=_LONG_COLUMNS)
    url = tmpl.format(eid=entity_id)
    sess = session or requests
    try:
        resp = sess.get(url, headers={"User-Agent": _DEFAULT_USER_AGENT}, timeout=timeout)
        resp.raise_for_status()
        resp.encoding = "euc-jp"  # netkeiba db は EUC-JP
        return parse_person_yearly(resp.text, entity_type, entity_id)
    except Exception as e:  # noqa: BLE001 — 取得失敗はスキップ（空）
        logger.warning("person_yearly 取得失敗 %s/%s: %s", entity_type, entity_id, e)
        return pd.DataFrame(columns=_LONG_COLUMNS)


def persist_person_yearly(df: pd.DataFrame, pickle_path: str) -> int:
    """年度別ロングを entity_id index にして raw pickle(+DB) に反映する。

    update_rawdata は index で dedup する。同一 entity_id の旧行を総入替（再取得で最新化）。
    """
    if df is None or df.empty:
        return 0
    from src.preparing._get_rawdata import update_rawdata

    indexed = df.set_index("entity_id")
    update_rawdata(pickle_path, indexed)
    return len(indexed)
