"""JRDB 固定長ファイル（KYI/SED/SKB）を DataFrame にパースする。cp932・16進日対応。

race_key はレースキー→race_id に変換し、(race_id, umaban) を featured への結合キーにする。
数値フィールドは半角/全角空白を NaN として数値化。特記コードは 3桁×6 を列 tokki1..6 に展開。
"""
from __future__ import annotations

import logging
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

from src.jrdb import _layouts as L
from src.jrdb._keys import race_key_to_race_id

logger = logging.getLogger(__name__)

# レコード長の許容差（レコード末 CRLF/LF の有無で ±2 程度ぶれるため）。これを超える
# 乖離はフォーマット版差の疑い（オフセットがずれて値が壊れる）。
_LEN_TOLERANCE = 2


def _records(path: str) -> list[bytes]:
    raw = Path(path).read_bytes()
    sep = b"\r\n" if b"\r\n" in raw else b"\n"
    return [r for r in raw.split(sep) if r.strip()]


def dominant_record_length(records: list[bytes]) -> int:
    """最頻レコード長（バイト）。records が空なら 0。"""
    if not records:
        return 0
    return Counter(len(r) for r in records).most_common(1)[0][0]


def check_record_length(path: str, record_type: str, *, tolerance: int = _LEN_TOLERANCE):
    """レコード長が仕様（RECORD_LEN）と ±tolerance 内か検査する。

    Returns
    -------
    (ok, dominant, expected) : ok は許容内か（仕様未知/空ファイルは True 扱い）。
    """
    rt = record_type.upper()
    expected = L.RECORD_LEN.get(rt)
    dom = dominant_record_length(_records(path))
    if expected is None or dom == 0:
        return True, dom, expected
    return abs(dom - expected) <= tolerance, dom, expected


def _slice(rec: bytes, start1: int, length: int) -> str:
    """1始まりバイト位置で切り出して cp932 デコード。"""
    return rec[start1 - 1: start1 - 1 + length].decode("cp932", "replace")


def _num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.str.strip().replace("", np.nan), errors="coerce")


def parse(path: str, record_type: str) -> pd.DataFrame:
    """JRDBファイルを DataFrame にする。record_type ∈ {'KYI','SED','SKB'}。

    共通列: race_id（変換済）, umaban（int）, ketto。加えて record_type 別の項目。
    """
    rt = record_type.upper()
    layout = {"KYI": L.KYI, "SED": L.SED, "SKB": L.SKB, "TYB": L.TYB}[rt]
    recs = _records(path)
    # フォーマット版差の検知: レコード長が仕様と大きく乖離したら警告（オフセットずれで
    # 値が壊れ得る）。取込側（JrdbStore）は既定でこのファイルをスキップする。
    _ok, _dom, _exp = check_record_length(path, rt)
    if not _ok:
        logger.warning(
            "[jrdb-parse] %s(%s): レコード長 %d が仕様 %d と乖離。フォーマット版が異なる可能性→"
            "オフセットずれで値が壊れる恐れ（古い年度パック等）。", rt, Path(path).name, _dom, _exp,
        )
    if not recs:
        return pd.DataFrame()  # 空ファイル/有効レコード無し → 空（store.upsert は空を no-op 扱い）
    cols: dict[str, list] = {name: [] for name in layout}
    repeats = L.SKB_REPEAT if rt == "SKB" else {}
    rep_cols: dict[str, list] = {}
    for base, (_s, _u, cnt) in repeats.items():
        for i in range(1, cnt + 1):
            rep_cols[f"{base}{i}"] = []
    ashi = L.SKB_ASHIMOTO if rt == "SKB" else {}
    for name in ashi:
        rep_cols[name] = []

    for r in recs:
        for name, (s, ln) in layout.items():
            cols[name].append(_slice(r, s, ln))
        for base, (s, u, cnt) in repeats.items():
            for i in range(cnt):
                rep_cols[f"{base}{i + 1}"].append(_slice(r, s + i * u, u).strip())
        for name, (s, ln) in ashi.items():
            rep_cols[name].append(_slice(r, s, ln).strip())

    df = pd.DataFrame({**cols, **rep_cols})
    df["race_id"] = df["race_key"].map(race_key_to_race_id)
    df["umaban"] = _num(df["umaban"]).astype("Int64")
    if "ketto" in df.columns:  # TYB 等は血統登録番号を持たない
        df["ketto"] = df["ketto"].str.strip()

    # 数値項目（pace_yosou=H/M/S は文字なので除外・後段でコード化）
    numeric = {
        "KYI": ["idm", "kishu_idx", "joho_idx", "sougou_idx", "rotation",
                "kijun_odds", "kijun_ninki", "kijun_fukuodds", "ninki_idx",
                "chokyo_idx", "kyusha_idx", "chokyo_yajirushi", "kyusha_hyoka",
                "kishu_kitai_rentai", "gekiso_idx", "class_code",
                "ten_idx", "pace_idx", "agari_idx", "ichi_idx",
                "dochu_juni", "go3f_juni", "goal_juni", "kakutei_bataijuu",
                "kokyu_flag", "start_idx", "deokure_rate", "manken_idx",
                "kishu_tansho", "kishu_3nai", "nyukyu_days"],
        "SED": ["chakujun", "kakutei_tansho", "idm", "deokure", "ichidori",
                "furi", "mae_furi", "naka_furi", "ato_furi", "bataijuu",
                "kakutei_fukusho_shita", "odds_10_tansho", "odds_10_fukusho"],
        "SKB": [],
        "TYB": ["idm", "kishu_idx", "joho_idx", "odds_idx", "paddock_idx",
                "sougou_idx", "bagu_change", "ashimoto_info", "torikeshi",
                "tansho_odds", "fukusho_odds", "bataijuu"],
    }[rt]
    for c in numeric:
        df[c] = _num(df[c])
    if "bamei" in df.columns:
        df["bamei"] = df["bamei"].str.strip()
    if "ymd" in df.columns:
        df["ymd"] = df["ymd"].str.strip()
    return df
