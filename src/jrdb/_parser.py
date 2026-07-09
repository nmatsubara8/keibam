"""JRDB 固定長ファイル（KYI/SED/SKB）を DataFrame にパースする。cp932・16進日対応。

race_key はレースキー→race_id に変換し、(race_id, umaban) を featured への結合キーにする。
数値フィールドは半角/全角空白を NaN として数値化。特記コードは 3桁×6 を列 tokki1..6 に展開。
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from src.jrdb import _layouts as L
from src.jrdb._keys import race_key_to_race_id


def _records(path: str) -> list[bytes]:
    raw = Path(path).read_bytes()
    sep = b"\r\n" if b"\r\n" in raw else b"\n"
    return [r for r in raw.split(sep) if r.strip()]


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
    layout = {"KYI": L.KYI, "SED": L.SED, "SKB": L.SKB}[rt]
    recs = _records(path)
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
    df["ketto"] = df["ketto"].str.strip()

    # 数値項目
    numeric = {
        "KYI": ["idm", "kijun_odds", "kijun_ninki"],
        "SED": ["chakujun", "kakutei_tansho", "idm", "deokure", "ichidori",
                "furi", "mae_furi", "naka_furi", "ato_furi", "bataijuu"],
        "SKB": [],
    }[rt]
    for c in numeric:
        df[c] = _num(df[c])
    if "bamei" in df.columns:
        df["bamei"] = df["bamei"].str.strip()
    if "ymd" in df.columns:
        df["ymd"] = df["ymd"].str.strip()
    return df
