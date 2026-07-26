"""レースクラス分類の網羅性監査（取得データに対する「判定不能」の洗い出し）。

`classify_race_class` がどの程度の生テキストを正準クラスへ写像できているか、また
None（判定不能）に落ちる実値は何かを集計する純粋関数群。データ取得後にこれを回し、
判定不能として残った生テキストを `Master._RACE_CONDITION_RULES` 等へ追加していく
（=分類規則を実データ駆動で育てる）ための土台。

入力源（永続化されている生テキスト）:
- horse_results の「レース名」（全年代・グレード括弧込み。最良の診断ソース）
- race_info の `race_class`（取得時に分類済み）と `race_condition`（タイトルは非永続）

Streamlit/IO に依存しない。CLI 配線は ルートの ``audit_race_class.py``。
"""

from __future__ import annotations

from collections import Counter
from typing import Iterable, Optional

from src.constants._master import classify_race_class
from src.constants._master import race_class_level


def _is_blank(v) -> bool:
    """None / NaN / 空白のみ を欠損（分類対象外）とみなす。"""
    if v is None:
        return True
    if isinstance(v, float) and v != v:  # NaN
        return True
    return not str(v).strip()


def coverage_summary(values: Iterable) -> dict:
    """生テキスト群の分類網羅率を集計する。

    欠損（空白/NaN）は分母から除外する（取得欠落であり分類失敗ではないため）。

    Returns
    -------
    {"total", "blank", "evaluated", "classified", "unclassified", "coverage"}
    """
    total = blank = classified = 0
    for v in values:
        total += 1
        if _is_blank(v):
            blank += 1
            continue
        if classify_race_class(v) is not None:
            classified += 1
    evaluated = total - blank
    return {
        "total": total,
        "blank": blank,
        "evaluated": evaluated,
        "classified": classified,
        "unclassified": evaluated - classified,
        "coverage": (classified / evaluated) if evaluated else 0.0,
    }


def unclassified_counts(values: Iterable) -> Counter:
    """classify_race_class が None を返す生テキストの出現回数（多い順に使える Counter）。"""
    c: Counter = Counter()
    for v in values:
        if _is_blank(v):
            continue
        if classify_race_class(v) is None:
            c[str(v).strip()] += 1
    return c


def class_distribution(values: Iterable) -> Counter:
    """分類結果（正準クラス名 / None=判定不能）の分布を返す。"""
    c: Counter = Counter()
    for v in values:
        if _is_blank(v):
            continue
        c[classify_race_class(v)] += 1
    return c


def classify_with_level(text) -> tuple[Optional[str], Optional[int]]:
    """単一テキストを (正準クラス, 順序値) に写像する（デバッグ・突合用）。"""
    cls = classify_race_class(text)
    return cls, race_class_level(cls)
