"""B prospective 確認ハーネスの純部（freeze日 split・trigger ゲート）のテスト。"""
from __future__ import annotations

import pandas as pd

from scripts.run_residual_head_prospective import FROZEN, _gate, _split_by_freeze


def _recs():
    # freeze_date=2026-08-02。前後に分かれるレコード
    return [
        {"race_id": "R1", "year": 2025, "winner": 1},
        {"race_id": "R2", "year": 2026, "winner": 2},   # freeze前
        {"race_id": "R3", "year": 2026, "winner": 3},   # freeze後
        {"race_id": "R4", "year": 2027, "winner": 1},   # freeze後
    ]


def _ymap():
    return {"R1": pd.Timestamp("2025-05-01"), "R2": pd.Timestamp("2026-07-01"),
            "R3": pd.Timestamp("2026-09-01"), "R4": pd.Timestamp("2027-03-01")}


def test_split_by_freeze():
    train, test, meta = _split_by_freeze(_recs(), _ymap())
    assert {r["race_id"] for r in train} == {"R1", "R2"}      # <= 2026-08-02
    assert {r["race_id"] for r in test} == {"R3", "R4"}        # > freeze
    assert meta["max_train_date"] == "2026-07-01"
    assert meta["min_test_date"] == "2026-09-01"
    assert meta["n_train"] == 2 and meta["n_test"] == 2


def test_split_drops_unmapped():
    train, test, _ = _split_by_freeze(_recs(), {"R1": pd.Timestamp("2025-05-01")})
    assert {r["race_id"] for r in train} == {"R1"}
    assert test == []                                          # ymd 不明は除外


def test_gate_blocks_below_trigger():
    train, test, meta = _split_by_freeze(_recs(), _ymap())
    feat = pd.DataFrame({c: [0.5] for c in FROZEN["features"]})
    checks, passed, blockers = _gate(train, test, meta, feat)
    assert passed is False                                     # n_test=2 << trigger 5000
    assert checks["trigger_reached"] is False
    assert any("trigger" in b for b in blockers)
    assert checks["time_order_ok"] is True                     # max_train < min_test


def test_frozen_prospective_spec():
    assert FROZEN["hypothesis_id"] == "B_RESIDUAL_HEAD_PROSPECTIVE_CONFIRM"
    assert FROZEN["freeze_date"] == "2026-08-02"
    assert FROZEN["min_test_races"] == 5000
    assert FROZEN["interim_looks"] == 0
    assert FROZEN["features"] == ["jrdb_idm", "jrdb_kishu_idx", "jrdb_joho_idx",
                                  "wet_rel_rank", "kinryo_per_weight"]
