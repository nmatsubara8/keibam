"""名寄せ（エンティティ解決）— 学習前に表記ゆれを吸収する前処理レイヤ。

馬名・レース名・厩舎名・生産者名などは netkeiba 上で表記ゆれ（全角半角・空白・
所在地サフィックス・改称等）がある。結合・集計は**安定 ID**（horse_id / jockey_id /
trainer_id / owner_id / breeder_id / race_id）で行うのが正準で、名前は表示専用とする。

本モジュールは:
1. `normalize_text` / `normalize_breeder_name` / `normalize_race_name` — 正規化関数
2. `canonical_id` — ID の正準文字列化（整数値 float の ".0" を除去）
3. `EntityMaster` — id ↔ 正準名 + 別名→id 逆引きのマスタ
4. 名前しか無いフィールド（調教の併入相手・勝ち馬・近親馬 等）を ID へ解決

設計方針: 名前→ID 解決は**曖昧（同名複数 / 不明）なら None（欠損）を返す**。
誤結合は特徴を汚染しリークや偽相関を生むため、欠損の方が安全。

レイヤ: preprocessing（constants/storage に依存可）。
"""

from __future__ import annotations

import re
import unicodedata
from collections import defaultdict
from typing import Iterable, Optional

# 生産者・馬主名の末尾に付く所在地サフィックス（例「ノーザンファーム 勇払郡」）。
# 空白 + 任意語 + (郡|市|町|村|区) で終わるものを除去する。
_LOCATION_SUFFIX_RE = re.compile(r"[\s　]+\S*?[郡市町村区]$")

# レース名からグレード表記（(G1)/(GⅢ)/(Jpn1)/(L) 等）を抽出する。
_GRADE_RE = re.compile(r"[（(]\s*(G[ⅠⅡⅢI1-3]+|Jpn[ⅠⅡⅢI1-3]+|L|OP)\s*[)）]", re.IGNORECASE)

# グレード内のローマ数字 → アラビア数字の対応（表記ゆれ吸収）。
# NFKC 正規化で Unicode ローマ数字（Ⅰ/Ⅱ/Ⅲ）は ASCII の "I"/"II"/"III" に分解されるため、
# **長いものから順に**置換する（"I"→"1" を先にすると "II" が "11" に壊れる）。
_GRADE_NUM_ORDERED = (("III", "3"), ("II", "2"), ("I", "1"))


def _is_missing(v) -> bool:
    """None / NaN / 空文字を欠損とみなす。"""
    if v is None:
        return True
    if isinstance(v, float):
        return v != v  # NaN
    return False


def normalize_text(s) -> str:
    """汎用テキスト正規化: NFKC 統一 + 前後空白除去 + 連続空白の単一化。

    非文字列（NaN 等）は空文字を返す。
    """
    if _is_missing(s):
        return ""
    if not isinstance(s, str):
        s = str(s)
    s = unicodedata.normalize("NFKC", s)
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s


def normalize_breeder_name(s) -> str:
    """生産者・馬主名の正規化: 汎用正規化 + 末尾の所在地サフィックス除去。

    例: 「ノーザンファーム 勇払郡」→「ノーザンファーム」。
    所在地が付かない名前はそのまま返す。
    """
    s = normalize_text(s)
    if not s:
        return ""
    stripped = _LOCATION_SUFFIX_RE.sub("", s).strip()
    # サフィックス除去で空になる病的ケースは元の値を保持する
    return stripped or s


def normalize_grade(g: str) -> str:
    """グレード文字列を正準化（GⅢ→G3、Jpn1→Jpn1、大文字化）。"""
    g = normalize_text(g).upper()
    for k, v in _GRADE_NUM_ORDERED:
        g = g.replace(k, v)
    return g


def extract_race_grade(race_name) -> Optional[str]:
    """レース名からグレード（G1/G2/G3/Jpn1/L/OP）を抽出する。無ければ None。"""
    s = normalize_text(race_name)
    if not s:
        return None
    m = _GRADE_RE.search(s)
    if not m:
        return None
    return normalize_grade(m.group(1))


def normalize_race_name(s) -> str:
    """レース名の正準化: 汎用正規化 + グレード括弧の除去（系列名で揃える）。

    例「京王杯スプリングC(G2)」「京王杯スプリングＣ（GⅡ）」→「京王杯スプリングC」。
    回次・条件付記は呼び出し側で扱う（ここでは券種横断の系列キーに使える素を返す）。
    """
    s = normalize_text(s)
    if not s:
        return ""
    s = _GRADE_RE.sub("", s).strip()
    return s


def canonical_id(v) -> Optional[str]:
    """ID の正準文字列化。None/NaN は None、整数値 float は int 表記に揃える。

    netkeiba の ID は int64 由来と float64 由来で "1234" と "1234.0" に割れるため、
    結合キーを揃える（storage._repo._to_db_str と同方針）。
    """
    if _is_missing(v):
        return None
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    s = str(v).strip()
    return s or None


class EntityMaster:
    """id ↔ 正準名 + 別名→id 逆引きのマスタ。

    同一 id に複数の別名がぶら下がり得る（表記ゆれ）。逆引きでは、ある名前に対し
    一意な id が定まる場合のみ解決し、曖昧（複数 id）/不明は None を返す。
    """

    def __init__(self, normalizer=normalize_text) -> None:
        self._normalizer = normalizer
        self.id_to_name: dict[str, str] = {}
        self.name_to_ids: dict[str, set] = defaultdict(set)

    def add(self, entity_id, name) -> None:
        """(id, name) を 1 件登録する。id 欠損は無視、name 欠損は逆引きのみスキップ。"""
        eid = canonical_id(entity_id)
        if eid is None:
            return
        nm = self._normalizer(name)
        # 最初に観測した非空の名前を正準名とする
        if nm and eid not in self.id_to_name:
            self.id_to_name[eid] = nm
        if nm:
            self.name_to_ids[nm].add(eid)

    def build(self, ids: Iterable, names: Iterable) -> "EntityMaster":
        """2 つの反復可能（pandas.Series 等）から一括構築する。"""
        for i, n in zip(ids, names, strict=False):
            self.add(i, n)
        return self

    def resolve(self, name) -> Optional[str]:
        """名前 → id。一意に定まるときのみ id を返し、曖昧/不明は None。"""
        nm = self._normalizer(name)
        if not nm:
            return None
        ids = self.name_to_ids.get(nm)
        if not ids or len(ids) != 1:
            return None
        return next(iter(ids))

    def name_of(self, entity_id) -> Optional[str]:
        """id → 正準名（未知は None）。"""
        eid = canonical_id(entity_id)
        if eid is None:
            return None
        return self.id_to_name.get(eid)

    def __len__(self) -> int:
        return len(self.id_to_name)
