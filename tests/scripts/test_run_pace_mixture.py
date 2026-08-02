"""run_pace_mixture.build_race_records（純関数）の単体テスト。

featured(per-horse) → レース単位レコード(odds/styles/winner) への変換が、発走前情報のみで
組め、勝ち馬・脚質・オッズを正しく拾い、頭数不足/勝ち馬なしを除外することを確認する。
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd

_MOD = Path(__file__).resolve().parents[2] / "scripts" / "run_pace_mixture.py"
_spec = importlib.util.spec_from_file_location("run_pace_mixture", _MOD)
rpm = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rpm)


def _featured():
    # R1: 3頭（勝ち馬=馬番2）。R2: 2頭（頭数不足で除外）。R3: 勝ち馬なし（除外）。
    rows = [
        ("2023R1", 1, 3.0, 2, 0.1),
        ("2023R1", 2, 2.0, 1, 0.6),
        ("2023R1", 3, 9.0, 3, 0.9),
        ("2023R2", 1, 1.5, 1, 0.3),
        ("2023R2", 2, 4.0, 2, 0.7),
        ("2024R3", 1, 2.0, 5, 0.1),   # 着順5→勝ち馬なし
        ("2024R3", 2, 3.0, 4, 0.4),
        ("2024R3", 3, 5.0, 6, 0.8),
    ]
    return pd.DataFrame(
        [(u, o, r, pm) for _, u, o, r, pm in rows],
        columns=["馬番", "単勝", "着順", "pace_median"],
        index=pd.Index([rid for rid, *_ in rows], name="race_id"),
    )


def test_build_race_records_basic():
    recs = {r["race_id"]: r for r in rpm.build_race_records(_featured())}
    assert set(recs) == {"2023R1"}                 # R2=頭数不足, R3=勝ち馬なし で除外
    r1 = recs["2023R1"]
    assert r1["winner"] == 2 and r1["year"] == 2023
    assert r1["odds"] == {1: 3.0, 2: 2.0, 3: 9.0}
    assert r1["styles"][1] == "nige"               # pace_median 0.1 <0.2
    assert r1["styles"][2] == "sashi"              # 0.6 は 0.5<=r<0.8 → sashi
    assert r1["styles"][3] == "oikomi"             # 0.9 >=0.8


def test_build_race_records_skips_bad_odds():
    df = pd.DataFrame(
        {"馬番": [1, 2, 3], "単勝": [0.0, 2.0, 3.0], "着順": [2, 1, 3],
         "pace_median": [0.1, 0.5, 0.9]},
        index=pd.Index(["2023RX"] * 3, name="race_id"),
    )
    recs = rpm.build_race_records(df)
    # 単勝=0 の馬番1 は除外され 2頭に → 3頭未満で race ごと除外
    assert recs == []
