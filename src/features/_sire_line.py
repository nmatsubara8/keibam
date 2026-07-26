"""系統（父系 sire-line）分類レイヤ — JRDB 系統コード表に基づく血統の「洗い替え」。

我々の血統データは生の種牡馬名（父/母父）で疎になりがち。JRDB の系統コード表
（84 小系統 / 15 大系統）で種牡馬を**専門分類の系統に束ねる**ことで、回収率ファクターを
汎化し（例: 系統×芝ダ、系統×コース）、疎性を減らす。

権威ある割当は JRDB データ本体の「系統コード」フィールド。ここは:
  1. data/jrdb_codes/keito_code.tsv       … 系統タクソノミー（code→小系統→大系統, 固定）
  2. data/jrdb_codes/sire_to_keito_seed.tsv … 種牡馬名→系統コード の暫定シード（主要種牡馬）
を読み、`classify_sire(name)` で (code, 小系統, 大系統) を返す。JRDB 由来の
種牡馬→系統コード マップ（data/jrdb_codes/sire_to_keito.tsv）が存在すれば**そちらを優先**し、
シードを上書き/拡張する。取得済み netkeiba データだけで完結（データ本体不要）。
"""
from __future__ import annotations

import functools
import unicodedata
from pathlib import Path

_DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "jrdb_codes"
_TAXONOMY = _DATA_DIR / "keito_code.tsv"
_SEED = _DATA_DIR / "sire_to_keito_seed.tsv"
_JRDB = _DATA_DIR / "sire_to_keito.tsv"  # JRDB 由来（あれば優先）

UNKNOWN = ("", "不明", "不明")


def _norm(name) -> str:
    """種牡馬名の正規化: 全半角統一・空白除去。"""
    if name is None:
        return ""
    s = unicodedata.normalize("NFKC", str(name)).strip()
    return s.replace(" ", "").replace("　", "")


@functools.lru_cache(maxsize=1)
def _taxonomy() -> dict[str, tuple[str, str]]:
    """{code: (小系統, 大系統)}。"""
    out: dict[str, tuple[str, str]] = {}
    if not _TAXONOMY.exists():
        return out
    for i, line in enumerate(_TAXONOMY.read_text(encoding="utf-8").splitlines()):
        if i == 0 or not line.strip() or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) >= 3:
            out[parts[0].strip()] = (parts[1].strip(), parts[2].strip())
    return out


def _load_sire_map(path: Path) -> dict[str, str]:
    """{正規化種牡馬名: code} を TSV から読む（# 行・ヘッダはスキップ）。"""
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for i, line in enumerate(path.read_text(encoding="utf-8").splitlines()):
        if not line.strip() or line.startswith("#"):
            continue
        parts = line.split("\t")
        if i == 0 and parts[0].strip().lower() in ("sire", "種牡馬", "name"):
            continue  # ヘッダ
        if len(parts) >= 2 and parts[1].strip().isdigit():
            out[_norm(parts[0])] = parts[1].strip()
    return out


@functools.lru_cache(maxsize=1)
def _sire_map() -> dict[str, str]:
    """種牡馬名(正規化)→系統コード。シードを土台に、JRDB 由来があれば上書き。"""
    m = _load_sire_map(_SEED)
    m.update(_load_sire_map(_JRDB))  # JRDB 優先
    return m


def classify_sire(name) -> tuple[str, str, str]:
    """種牡馬名 → (系統コード, 小系統名, 大系統名)。未知は UNKNOWN。"""
    code = _sire_map().get(_norm(name))
    if code is None:
        return UNKNOWN
    small, big = _taxonomy().get(code, ("不明", "不明"))
    return (code, small, big)


def daikeito(name) -> str:
    """種牡馬名 → 大系統名（未知は '不明'）。ファクター用の主粒度。"""
    return classify_sire(name)[2]


def shoukeito(name) -> str:
    """種牡馬名 → 小系統名（未知は '不明'）。"""
    return classify_sire(name)[1]


def coverage(sire_names) -> dict:
    """種牡馬名の iterable に対する分類カバレッジを返す（診断用）。

    Returns: {n, classified, rate, unmapped: {名前: 件数} 上位}
    """
    from collections import Counter
    total = 0
    hit = 0
    unmapped: Counter = Counter()
    for nm in sire_names:
        total += 1
        if classify_sire(nm)[0]:
            hit += 1
        else:
            unmapped[_norm(nm)] += 1
    return {
        "n": total,
        "classified": hit,
        "rate": (hit / total) if total else 0.0,
        "unmapped_top": unmapped.most_common(30),
    }
