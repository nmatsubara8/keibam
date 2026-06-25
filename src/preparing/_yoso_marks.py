"""予想印（プロ予想家の印）パーサ — netkeiba の内部 JSON API を解析する。

netkeiba の予想印グリッド（race.../yoso/mark_list.html）は印を JavaScript で描画し、
内部 API から取得している。本モジュールはその API レスポンスを解析して
`raw_yoso_marks`（race_id × 馬番 × 予想家 のロング形式）に展開する。

API（raw HTML の JS 解析で特定）:
    GET https://race.netkeiba.com/api/api_get_pro_yoso_list_v2.html
        ?input=UTF-8&output=json&race_id=<race_id>&ref_type=2
レスポンス: json.data.ary_item[] の各要素が 1 予想家:
    - yosoka_id / yosoka_name（<br> 区切り）
    - goods_kbn: *_free=無料 / no1_premium=プレミアム指定 / umai_sell=有料(mark空)
    - mark: {"<馬番>": "<code>"} … code 1=◎ 2=○ 3=▲ 4=△ 5=☆
    - recovery_rate（回収率・スキル加重の素）

方針（ユーザー指定・更新）: **取得できる予想家は無料・プレミアム指定とも全て取得**する
（API が匿名で返す `no1_premium` も含める）。由来は `goods_kbn` 列で保持し、後段で
free/premium を区別・重み付けできるようにする。`umai_sell`（有料・未購入）は mark が空で
自然に行が出ない。**取得できなくてもエラーにせず空で返す**（堅牢性優先）。

レイヤ: preparing。pandas/requests のみに依存。
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Optional

import pandas as pd

# FREE_GOODS_KBN は constants へ、aggregate_consensus は preprocessing へ移設（レイヤ逆流の解消）。
# 既存の `from src.preparing._yoso_marks import FREE_GOODS_KBN / aggregate_consensus` を温存する再 export。
from src.constants._yoso import FREE_GOODS_KBN  # noqa: F401
from src.preprocessing._yoso_consensus import aggregate_consensus  # noqa: F401

logger = logging.getLogger(__name__)

# 印コード（API） → グリフ / スコア
MARK_CODE_TO_GLYPH: dict[int, str] = {1: "◎", 2: "○", 3: "▲", 4: "△", 5: "☆"}
MARK_CODE_TO_SCORE: dict[int, int] = {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}

API_URL = "https://race.netkeiba.com/api/api_get_pro_yoso_list_v2.html"

_LONG_COLUMNS = [
    "race_id",
    "馬番",
    "predictor_yid",
    "predictor_name",
    "goods_kbn",
    "mark",
    "mark_score",
]


def _strip_br(name: str | None) -> str:
    """yosoka_name の <br> 区切りを除去して連結する（'本<br>紙' → '本紙'）。"""
    if not name:
        return ""
    text = re.sub(r"<br\s*/?>", "", name, flags=re.IGNORECASE)
    return re.sub(r"\s+", "", text).strip()


def _coerce_payload(payload: Any) -> dict[str, Any]:
    """dict / JSON 文字列 / JSONP 文字列のいずれでも dict に正規化する。"""
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode("utf-8", errors="replace")
    if isinstance(payload, str):
        s = payload.strip()
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            # jsonp 形式 callback({...}) から中身の {...} を取り出す
            m = re.search(r"\{.*\}", s, flags=re.DOTALL)
            if m:
                return json.loads(m.group(0))
            raise
    raise TypeError(f"unsupported payload type: {type(payload)!r}")


def parse_umaban_map(html: str) -> dict[int, int]:
    """出馬表 HTML から uma_id→馬番 の対応表を作る。

    予想印 API の `mark` キーは **uma_id（出走登録ID）**で馬番ではない。出馬表の各行は
    ``<tr id="tr_<uma_id>">`` を持ち、行内 ``td.Umaban`` が馬番。両者を対応づける。
    """
    from bs4 import BeautifulSoup

    if not html:
        return {}
    soup = BeautifulSoup(html, "html.parser")
    out: dict[int, int] = {}
    for tr in soup.select("tr[id^=tr_]"):
        m = re.match(r"tr_(\d+)$", tr.get("id", ""))
        if not m:
            continue
        umaban_td = tr.find("td", class_=re.compile("Umaban"))
        if umaban_td is None:
            continue
        txt = umaban_td.get_text(strip=True)
        if not txt.isdigit():
            continue
        out[int(m.group(1))] = int(txt)
    return out


def fetch_umaban_map(
    race_id: str, *, timeout: float = 10.0, session: Optional[Any] = None
) -> dict[int, int]:
    """出馬表ページを取得して uma_id→馬番 を返す（取得失敗時は空 dict）。"""
    import requests

    from src.preparing._scraper import _DEFAULT_USER_AGENT

    url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
    sess = session or requests
    try:
        resp = sess.get(url, headers={"User-Agent": _DEFAULT_USER_AGENT}, timeout=timeout)
        resp.raise_for_status()
        return parse_umaban_map(resp.text)
    except Exception as e:  # noqa: BLE001 — 取得失敗は空（呼び出し側で印を捨てる）
        logger.warning("uma_id→馬番 取得失敗 race_id=%s: %s", race_id, e)
        return {}


def parse_pro_yoso_json(
    payload: Any, race_id: str, free_only: bool = False,
    umaban_map: Optional[dict[int, int]] = None,
) -> pd.DataFrame:
    """予想印 API レスポンスを `raw_yoso_marks` ロング形式 DataFrame に変換する。

    Parameters
    ----------
    payload : dict / JSON文字列 / JSONP文字列（api_get_pro_yoso_list_v2 の応答）
    race_id : 対象レースID（応答に含まれないため呼び出し側が付与）
    free_only : 既定 False（**無料＋プレミアム指定の両方を取得**）。True にすると
        無料予想家（goods_kbn ∈ FREE_GOODS_KBN）のみ。由来は `goods_kbn` 列に保持。
    umaban_map : uma_id→馬番 の対応表（出馬表由来）。``mark`` のキーは uma_id のため、
        これで馬番に変換する。**指定時、表に無い uma_id（取消馬等）の印は捨てる**。
        None（既定）のときはキーをそのまま馬番として扱う（後方互換・主にテスト用）。

    Returns
    -------
    DataFrame[race_id, 馬番, predictor_yid, predictor_name, goods_kbn, mark, mark_score]
        印が無い予想家（umai_sell 等）は行を生成しない。壊れた要素はスキップし例外を投げない。
    """
    try:
        data = _coerce_payload(payload)
    except (ValueError, TypeError, json.JSONDecodeError) as e:
        logger.warning("yoso marks: payload 解析失敗 race_id=%s: %s", race_id, e)
        return pd.DataFrame(columns=_LONG_COLUMNS)
    if not isinstance(data, dict) or data.get("status") == "NG":
        return pd.DataFrame(columns=_LONG_COLUMNS)
    items = (data.get("data") or {}).get("ary_item") or []

    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        goods_kbn = item.get("goods_kbn", "")
        if free_only and goods_kbn not in FREE_GOODS_KBN:
            continue
        yid = str(item.get("yosoka_id", "")).strip()
        name = _strip_br(item.get("yosoka_name"))
        marks = item.get("mark") or {}
        if not isinstance(marks, dict):
            continue  # 取得できない予想家は黙ってスキップ（エラーにしない）
        for uma_id_raw, code_raw in marks.items():
            try:
                uma_id = int(uma_id_raw)
                code = int(code_raw)
            except (TypeError, ValueError):
                continue
            # mark のキーは uma_id。対応表があれば馬番に変換（無い uma_id は捨てる）。
            if umaban_map is not None:
                umaban = umaban_map.get(uma_id)
                if umaban is None:
                    continue
            else:
                umaban = uma_id  # 後方互換: 表なしはキーをそのまま
            glyph = MARK_CODE_TO_GLYPH.get(code)
            if glyph is None:
                continue  # 未知コードは無視
            rows.append(
                {
                    "race_id": race_id,
                    "馬番": umaban,
                    "predictor_yid": yid,
                    "predictor_name": name,
                    "goods_kbn": goods_kbn,
                    "mark": glyph,
                    "mark_score": MARK_CODE_TO_SCORE[code],
                }
            )

    if not rows:
        return pd.DataFrame(columns=_LONG_COLUMNS)
    return pd.DataFrame(rows, columns=_LONG_COLUMNS)


def fetch_pro_yoso_marks(
    race_id: str,
    *,
    free_only: bool = False,
    timeout: float = 10.0,
    session: Optional[Any] = None,
) -> pd.DataFrame:
    """予想印 API を叩いて `raw_yoso_marks` を取得する（ネットワーク・ユーザー環境用）。

    匿名 GET。`output=json` を要求し JSON を解析する。**取得できなくても例外を投げず
    空 DataFrame を返す**（バッチ取得を止めない）。ポライトネスの間隔は呼び出し側で
    polite_interval を挟むこと。既定で無料＋プレミアム指定の両方を取得する。
    """
    import requests  # 遅延 import（解析・テストは requests 不要）

    from src.preparing._scraper import _DEFAULT_USER_AGENT

    params = {
        "input": "UTF-8",
        "output": "json",
        "race_id": race_id,
        "ref_type": "2",
    }
    # 2024 のクローラー対策で UA 未設定だと HTTP 400。ブラウザ UA を必ず付ける。
    headers = {
        "User-Agent": _DEFAULT_USER_AGENT,
        "Referer": f"https://race.netkeiba.com/yoso/mark_list.html?race_id={race_id}",
    }
    sess = session or requests
    # mark のキーは uma_id のため、先に出馬表から uma_id→馬番 を引いて変換する。
    # 取得できないと馬番を確定できないので印を捨てる（誤った馬番で結合しない）。
    umaban_map = fetch_umaban_map(race_id, timeout=timeout, session=session)
    if not umaban_map:
        logger.warning("yoso marks: uma_id→馬番 が空のため race_id=%s をスキップ", race_id)
        return pd.DataFrame(columns=_LONG_COLUMNS)
    try:
        resp = sess.get(API_URL, params=params, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return parse_pro_yoso_json(resp.text, race_id, free_only=free_only, umaban_map=umaban_map)
    except Exception as e:  # noqa: BLE001 — 取得失敗はスキップ（空を返す）
        logger.warning("yoso marks 取得失敗 race_id=%s: %s", race_id, e)
        return pd.DataFrame(columns=_LONG_COLUMNS)


def persist_yoso_marks(df: pd.DataFrame, pickle_path: str) -> int:
    """予想印ロング DataFrame を race_id index にして raw pickle(+DB) に反映する。

    `update_rawdata` は index で dedup するため race_id を index に置く。同一 race_id の
    旧行は総入替される（再取得で最新に更新）。空なら何もしない。
    """
    if df is None or df.empty:
        return 0
    from src.preparing._get_rawdata import update_rawdata

    indexed = df.set_index("race_id")
    update_rawdata(pickle_path, indexed)
    return len(indexed)
