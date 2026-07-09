"""系統(父系)分類レイヤの単体テスト。"""
from __future__ import annotations

import pandas as pd

from src.features._sire_line import (
    UNKNOWN,
    classify_sire,
    coverage,
    daikeito,
    shoukeito,
)
from src.policies._manji_factors import NA, f_sire_line


def test_taxonomy_and_classification():
    # ディープインパクト → ヘイロー系(小) / ロイヤルチャージャー系(大)
    code, small, big = classify_sire("ディープインパクト")
    assert code == "1206"
    assert small == "ヘイロー系"
    assert big == "ロイヤルチャージャー系"


def test_daikeito_groups_sunday_line_together():
    # サンデー系の複数種牡馬が同じ大系統に束ねられる
    for s in ["ディープインパクト", "ハーツクライ", "キズナ", "ダイワメジャー", "オルフェーヴル"]:
        assert daikeito(s) == "ロイヤルチャージャー系", s


def test_mrprospector_and_roberto_lines():
    assert shoukeito("キングカメハメハ") == "ミスタープロスペクター系"
    assert daikeito("キングカメハメハ") == "ネイティヴダンサー系"
    assert shoukeito("エピファネイア") == "ロベルト系"
    assert daikeito("モーリス") == "ロイヤルチャージャー系"  # ロベルト系→ロイヤルチャージャー系


def test_name_normalization():
    # 全角空白・半角空白を含んでも一致
    assert classify_sire(" ディープ インパクト ") == classify_sire("ディープインパクト")


def test_unknown_sire_returns_unknown():
    assert classify_sire("存在しない種牡馬XYZ") == UNKNOWN
    assert daikeito("存在しない種牡馬XYZ") == "不明"
    assert classify_sire(None) == UNKNOWN


def test_coverage_report():
    names = ["ディープインパクト", "キングカメハメハ", "無名種牡馬A", "無名種牡馬A"]
    cov = coverage(names)
    assert cov["n"] == 4
    assert cov["classified"] == 2
    assert cov["rate"] == 0.5
    assert ("無名種牡馬A", 2) in cov["unmapped_top"]


def test_f_sire_line_factor_maps_and_is_tolerant():
    # 種牡馬名列 '父' があれば大系統に分類、未知は na
    df = pd.DataFrame(
        {"父": ["ディープインパクト", "キングカメハメハ", "無名XYZ"]},
        index=["R1", "R1", "R1"],
    )
    out = pd.Series(f_sire_line(df), index=df.index)
    assert out.iloc[0] == "ロイヤルチャージャー系"
    assert out.iloc[1] == "ネイティヴダンサー系"
    assert out.iloc[2] == NA
    # 種牡馬名列が無ければ全 na
    df2 = pd.DataFrame({"馬番": [1, 2]}, index=["R", "R"])
    assert (pd.Series(f_sire_line(df2), index=df2.index) == NA).all()
