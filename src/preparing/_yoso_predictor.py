"""予想家プロフィール（プロ予想家スキルの prior）パーサ。

netkeiba の予想家プロフィール ``yoso.netkeiba.com/no1/?pid=profile&yid=<yid>``（UTF-8・静的）
の **予想履歴テーブル**（日付/場名/レース/結果/的中配当/◎の成績、直近 ~24 件）から、予想家
ごとの **◎的中率/回収率の集計（prior）** を 1 行に集約する。

制約（実 HTML で確認）: 履歴はページネーション無しの直近 ~24 件のみで年の表記が無い。よって
true career ではなく『直近スナップショット』。予想家スキルは安定特性として prior に使う
（現時点値を過去レースに当てる軽微リークは許容＝方式 B1 のユーザー指定）。

レイヤ: preparing。pandas/requests/bs4 のみ。失敗・構造差異は空 DataFrame（堅牢性優先）。
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)

_PROFILE_URL = "https://yoso.netkeiba.com/no1/?pid=profile&yid={yid}"
_RANK_RE = re.compile(r"(\d+)\s*着")
_PAYOUT_RE = re.compile(r"([\d,]+)\s*円")

# 予想家×通算 prior の列（直近ログ集計）
_COLUMNS = [
    "predictor_yid", "profile_n", "profile_honmei_winrate",
    "profile_honmei_pkrate", "profile_hit_rate", "profile_avg_return",
]


def parse_yoso_predictor(html: str, predictor_yid: str) -> pd.DataFrame:
    """予想家プロフィールの履歴テーブルを集計し、予想家1行の prior DataFrame を返す。

    - ``profile_honmei_winrate`` : ◎が1着になった率（◎着順が取れた行内）
    - ``profile_honmei_pkrate``  : ◎が3着以内の率
    - ``profile_hit_rate``       : 馬券『的中』率（結果列）
    - ``profile_avg_return``     : 的中時の平均配当（円）。回収力の代理
    """
    from bs4 import BeautifulSoup

    if not html:
        return pd.DataFrame(columns=_COLUMNS)
    soup = BeautifulSoup(html, "html.parser")
    table = None
    for t in soup.find_all("table"):
        txt = t.get_text()
        if "◎の成績" in txt and "日付" in txt:
            table = t
            break
    if table is None:
        return pd.DataFrame(columns=_COLUMNS)

    n = hit = n_hon = hon1 = hon3 = 0
    payouts: list[float] = []
    for tr in table.find_all("tr")[1:]:
        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
        if len(cells) < 7:
            continue
        kekka, haito, seiseki = cells[4], cells[5], cells[6]
        n += 1
        if "的中" in kekka:
            hit += 1
            pm = _PAYOUT_RE.search(haito)
            if pm:
                payouts.append(float(pm.group(1).replace(",", "")))
        rm = _RANK_RE.search(seiseki)
        if rm:
            n_hon += 1
            chaku = int(rm.group(1))
            if chaku == 1:
                hon1 += 1
            if chaku <= 3:
                hon3 += 1
    if n == 0:
        return pd.DataFrame(columns=_COLUMNS)
    return pd.DataFrame([{
        "predictor_yid": str(predictor_yid),
        "profile_n": n,
        "profile_honmei_winrate": (hon1 / n_hon) if n_hon else float("nan"),
        "profile_honmei_pkrate": (hon3 / n_hon) if n_hon else float("nan"),
        "profile_hit_rate": hit / n,
        "profile_avg_return": (sum(payouts) / len(payouts)) if payouts else 0.0,
    }], columns=_COLUMNS)


def fetch_yoso_predictor(
    predictor_yid: str, *, timeout: float = 10.0, session: Optional[Any] = None
) -> pd.DataFrame:
    """予想家プロフィールを取得して prior を返す（UTF-8・匿名 GET）。失敗時は空。"""
    import requests

    from src.preparing._scraper import _DEFAULT_USER_AGENT

    url = _PROFILE_URL.format(yid=predictor_yid)
    sess = session or requests
    try:
        resp = sess.get(url, headers={"User-Agent": _DEFAULT_USER_AGENT}, timeout=timeout)
        resp.raise_for_status()
        return parse_yoso_predictor(resp.text, predictor_yid)
    except Exception as e:  # noqa: BLE001 — 取得失敗はスキップ（空）
        logger.warning("yoso_predictor 取得失敗 yid=%s: %s", predictor_yid, e)
        return pd.DataFrame(columns=_COLUMNS)


def persist_yoso_predictor(df: pd.DataFrame, pickle_path: str) -> int:
    """予想家 prior を predictor_yid index にして raw pickle(+DB) に反映する。"""
    if df is None or df.empty:
        return 0
    from src.preparing._get_rawdata import update_rawdata

    indexed = df.set_index("predictor_yid")
    update_rawdata(pickle_path, indexed)
    return len(indexed)
