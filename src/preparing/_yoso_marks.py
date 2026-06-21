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

logger = logging.getLogger(__name__)

# 印コード（API） → グリフ / スコア
MARK_CODE_TO_GLYPH: dict[int, str] = {1: "◎", 2: "○", 3: "▲", 4: "△", 5: "☆"}
MARK_CODE_TO_SCORE: dict[int, int] = {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}

# 無料を表す goods_kbn（由来判定用）。umai_buy=購入済み（ログイン時のみ出現）も無料扱い。
FREE_GOODS_KBN: frozenset[str] = frozenset({"no1_free", "umai_free", "umai_buy"})

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


def parse_pro_yoso_json(
    payload: Any, race_id: str, free_only: bool = False
) -> pd.DataFrame:
    """予想印 API レスポンスを `raw_yoso_marks` ロング形式 DataFrame に変換する。

    Parameters
    ----------
    payload : dict / JSON文字列 / JSONP文字列（api_get_pro_yoso_list_v2 の応答）
    race_id : 対象レースID（応答に含まれないため呼び出し側が付与）
    free_only : 既定 False（**無料＋プレミアム指定の両方を取得**）。True にすると
        無料予想家（goods_kbn ∈ FREE_GOODS_KBN）のみ。由来は `goods_kbn` 列に保持。

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
        for umaban_raw, code_raw in marks.items():
            try:
                umaban = int(umaban_raw)
                code = int(code_raw)
            except (TypeError, ValueError):
                continue
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


def aggregate_consensus(df_long: pd.DataFrame) -> pd.DataFrame:
    """印ロング形式を (race_id, 馬番) ごとのコンセンサス特徴に集約する（リーク無し）。

    予想家の顔ぶれはレースで変動するため、個別予想家列でなく集約量を作る:
    - yoso_n_marks    : 印を付けた予想家数（注目度）
    - yoso_n_honmei   : ◎の数
    - yoso_score_sum  : 印スコア合計（◎5..☆1）
    - yoso_score_mean : 印スコア平均（印を付けた予想家内での評価の高さ）
    """
    cols = ["race_id", "馬番", "yoso_n_marks", "yoso_n_honmei", "yoso_score_sum", "yoso_score_mean"]
    if df_long is None or df_long.empty:
        return pd.DataFrame(columns=cols)

    g = df_long.groupby(["race_id", "馬番"])
    out = g.agg(
        yoso_n_marks=("mark_score", "size"),
        yoso_score_sum=("mark_score", "sum"),
        yoso_score_mean=("mark_score", "mean"),
    )
    out["yoso_n_honmei"] = (
        df_long.assign(_h=(df_long["mark"] == "◎").astype(int))
        .groupby(["race_id", "馬番"])["_h"]
        .sum()
    )
    # 無料予想家のみの印数も併設（プレミアム除外の特徴を後で選べるように。最大スキーマの思想）
    if "goods_kbn" in df_long.columns:
        free = df_long[df_long["goods_kbn"].isin(FREE_GOODS_KBN)]
        out["yoso_n_marks_free"] = (
            free.groupby(["race_id", "馬番"]).size() if not free.empty else 0
        )
        out["yoso_n_marks_free"] = out["yoso_n_marks_free"].fillna(0).astype(int)
        cols = cols + ["yoso_n_marks_free"]
    return out.reset_index()[cols]


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

    params = {
        "input": "UTF-8",
        "output": "json",
        "race_id": race_id,
        "ref_type": "2",
    }
    headers = {"Referer": f"https://race.netkeiba.com/yoso/mark_list.html?race_id={race_id}"}
    sess = session or requests
    try:
        resp = sess.get(API_URL, params=params, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return parse_pro_yoso_json(resp.text, race_id, free_only=free_only)
    except Exception as e:  # noqa: BLE001 — 取得失敗はスキップ（空を返す）
        logger.warning("yoso marks 取得失敗 race_id=%s: %s", race_id, e)
        return pd.DataFrame(columns=_LONG_COLUMNS)
