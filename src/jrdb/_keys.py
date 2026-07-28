"""JRDB のキー（レースキー・血統登録番号）を netkeiba の race_id・horse_id へ橋渡しする。

JRDB レースキー(8) = 場コード(2) + 年(2) + 回(1) + 日(1,16進) + R(2)
netkeiba race_id(12) = 西暦(4) + 場(2) + 回(2,0詰) + 日(2,0詰) + R(2)

JRDB 血統登録番号(8) = 生年(2) + 通し番号(6)
netkeiba horse_id(10) = 生年(4) + 通し番号(6)

年の世紀補完: 2桁年 yy が pivot(既定86)以上 → 1900+yy、未満 → 2000+yy。
（JRDBは1986年頃以降。1986-2085 を一意に解釈できる）
"""
from __future__ import annotations


def _century(yy: int, pivot: int = 86) -> int:
    return 1900 + yy if yy >= pivot else 2000 + yy


def race_key_to_race_id(key: str, *, pivot: int = 86) -> str | None:
    """JRDB レースキー(先頭8) → netkeiba race_id(12)。失敗時 None。"""
    if key is None:
        return None
    k = str(key).strip()
    if len(k) < 8:
        return None
    place, yy, kai, day_hex, r = k[0:2], k[2:4], k[4:5], k[5:6], k[6:8]
    if not (place.isdigit() and yy.isdigit() and kai.isalnum() and r.isdigit()):
        return None
    try:
        day = int(day_hex, 16)          # 日は16進1桁（10-15日は a-f）
        kai_i = int(kai, 16)
    except ValueError:
        return None
    year = _century(int(yy), pivot)
    return f"{year:04d}{place}{kai_i:02d}{day:02d}{r}"


def kaisai_key_to_kaisai_id(key: str, *, pivot: int = 86) -> str | None:
    """JRDB 開催キー(先頭6=場2+年2+回1+日1) → 開催ID(10=race_id の R 抜き先頭10桁)。

    KAB 開催データはレース単位でなく開催（競馬場×日）単位。race_id[:10] と一致する
    ID を作り、レースメタ（going/天候）を race_id の先頭10桁で突合できるようにする。
    """
    if key is None:
        return None
    k = str(key).strip()
    if len(k) < 6:
        return None
    rid = race_key_to_race_id(k[:6] + "00", pivot=pivot)  # ダミー R=00 を足して既存変換を再利用
    return rid[:10] if rid else None


def ketto_to_horse_id(ketto: str, *, pivot: int = 86) -> str | None:
    """JRDB 血統登録番号(8) → netkeiba horse_id(10)。失敗時 None。"""
    if ketto is None:
        return None
    k = str(ketto).strip()
    if len(k) != 8 or not k.isdigit():
        return None
    year = _century(int(k[0:2]), pivot)
    return f"{year:04d}{k[2:8]}"
