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


def _featured_for(rids_train, rids_test, wet_train_frac, wet_test_frac):
    """train/test race_id 群を index に持つ featured 風フレーム（wet_rel_rank の coverage を制御）。"""
    import numpy as np
    rows, idx = [], []
    def add(rids, frac):
        for i, rid in enumerate(rids):
            for h in range(4):                      # 1レース4頭
                idx.append(rid)
                wet = 0.3 if ((i * 4 + h) / (len(rids) * 4)) < frac else np.nan
                rows.append({"jrdb_idm": 0.1, "jrdb_kishu_idx": 0.1, "jrdb_joho_idx": 0.1,
                             "wet_rel_rank": wet, "kinryo_per_weight": 0.1})
    add(rids_train, wet_train_frac); add(rids_test, wet_test_frac)
    return pd.DataFrame(rows, index=idx)


def test_gate_partial_feature_not_blocked():
    # wet_rel_rank が train/test とも ~50%（自然に部分的）→ 相対比較で断絶なし＝coverage は blocker にしない
    train = [{"race_id": f"T{i}", "winner": 1} for i in range(10)]
    test = [{"race_id": f"E{i}", "winner": 1} for i in range(10)]
    meta = {"max_train_date": "2026-07-01", "min_test_date": "2026-09-01",
            "n_train": 10, "n_test": 10}
    feat = _featured_for([r["race_id"] for r in train], [r["race_id"] for r in test], 0.5, 0.5)
    checks, _passed, blockers = _gate(train, test, meta, feat)
    assert not any("coverage" in b for b in blockers)         # 自然な部分特徴は罰しない


def test_gate_coverage_regression_blocks():
    # test 期で wet_rel_rank が train の半分未満に断絶 → blocker
    train = [{"race_id": f"T{i}", "winner": 1} for i in range(10)]
    test = [{"race_id": f"E{i}", "winner": 1} for i in range(10)]
    meta = {"max_train_date": "2026-07-01", "min_test_date": "2026-09-01",
            "n_train": 10, "n_test": 10}
    feat = _featured_for([r["race_id"] for r in train], [r["race_id"] for r in test], 0.8, 0.1)
    _checks, _passed, blockers = _gate(train, test, meta, feat)
    assert any("coverage" in b for b in blockers)             # 断絶は検出
