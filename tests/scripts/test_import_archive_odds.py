"""横持ち払戻 CSV → 縦持ち payoffs 変換の単体テスト。

単勝/複勝/馬連/三連単の代表を仕込み、combo_key 正準化（順不同は昇順、三連単は順序保持）・
払戻円・欠損スキップを固定する。ユーザ提供の実カラム名に準拠。
"""
from __future__ import annotations

import pandas as pd

from scripts.import_archive_odds import csv_to_payoffs


def _row(**kw):
    base = {"レースID": "198601010101"}
    base.update(kw)
    return base


def _sample():
    # 1レース: 単勝=馬2(210円), 複勝=馬2(100)/馬3(230), 馬連=2-3(510),
    #          三連単=3→2→1(順序保持), 三連複は欠損(NaN)
    return pd.DataFrame([_row(**{
        "単勝1_馬番": "2", "単勝1_オッズ": "210", "単勝1_人気": "1",
        "単勝2_馬番": None, "単勝2_オッズ": None, "単勝2_人気": None,
        "複勝1_馬番": "2", "複勝1_オッズ": "100", "複勝1_人気": "1",
        "複勝2_馬番": "3", "複勝2_オッズ": "230", "複勝2_人気": "4",
        "馬連1_組合せ1": "3", "馬連1_組合せ2": "2", "馬連1_オッズ": "510", "馬連1_人気": "2",
        "三連単1_組合せ1": "3", "三連単1_組合せ2": "2", "三連単1_組合せ3": "1",
        "三連単1_オッズ": "9999", "三連単1_人気": "50",
        "三連複1_組合せ1": None, "三連複1_組合せ2": None, "三連複1_組合せ3": None,
        "三連複1_オッズ": None, "三連複1_人気": None,
    })])


def test_tansho_fukusho_extracted():
    p = csv_to_payoffs(_sample())
    t = p[p["bet_type"] == "tansho"]
    assert list(t["combo_key"]) == ["2"] and list(t["payoff_yen"]) == [210.0]
    f = p[p["bet_type"] == "fukusho"].sort_values("combo_key")
    assert list(f["combo_key"]) == ["2", "3"]
    assert set(f["payoff_yen"]) == {100.0, 230.0}


def test_umaren_sorted_ascending():
    p = csv_to_payoffs(_sample())
    u = p[p["bet_type"] == "umaren"]
    # 3-2 は順不同 → 昇順 "2-3"
    assert list(u["combo_key"]) == ["2-3"]
    assert list(u["payoff_yen"]) == [510.0]


def test_sanrentan_order_preserved():
    p = csv_to_payoffs(_sample())
    s = p[p["bet_type"] == "sanrentan"]
    # 3→2→1 は順序保持 → "3-2-1"（昇順化しない）
    assert list(s["combo_key"]) == ["3-2-1"]


def test_missing_combos_skipped():
    p = csv_to_payoffs(_sample())
    # 三連複は全 NaN → 行を作らない
    assert (p["bet_type"] == "sanrenpuku").sum() == 0
    # 単勝2（NaN）もスキップ
    assert (p["bet_type"] == "tansho").sum() == 1


def test_schema():
    p = csv_to_payoffs(_sample())
    assert list(p.columns) == ["race_id", "bet_type", "combo_key", "payoff_yen", "popularity"]
    assert (p["race_id"] == "198601010101").all()
