"""B-2027 confirmation ハーネスの純部（事前判定規則・feature hash）のテスト。"""
from __future__ import annotations

from scripts.run_residual_head_2027 import FROZEN, _feature_hash, verdict


def test_frozen_spec_locked():
    assert FROZEN["hypothesis_id"] == "B_RESIDUAL_HEAD_2027_CONFIRM"
    assert FROZEN["features"] == ["jrdb_idm", "jrdb_kishu_idx", "jrdb_joho_idx",
                                  "wet_rel_rank", "kinryo_per_weight"]
    assert FROZEN["l2"] == 1.0 and FROZEN["mes_dnll"] == 0.001
    assert FROZEN["test_year"] == 2027 and FROZEN["interim_looks"] == 0
    assert FROZEN["bootstrap_repetitions"] == 20000 and FROZEN["bootstrap_seed"] == 0


def test_verdict_confirmed():
    # ΔNLL<=-MES・CI上限<0・ΔECE非悪化 → 🟢
    assert verdict(-0.0015, -0.0005, 0.0).startswith("🟢")


def test_verdict_sub_mes():
    # CI上限<0 だが |ΔNLL|<MES → 🟡
    assert verdict(-0.0004, -0.0001, 0.0).startswith("🟡")


def test_verdict_not_confirmed_ci_crosses_zero():
    assert verdict(-0.0015, +0.0002, 0.0).startswith("❌")


def test_verdict_not_confirmed_positive():
    assert verdict(+0.0005, +0.001, 0.0).startswith("❌")


def test_verdict_not_confirmed_ece_worse():
    # 効果量十分でも ECE 悪化 → ❌
    assert verdict(-0.002, -0.001, 0.01).startswith("❌")


def test_verdict_ece_none_ok():
    assert verdict(-0.0015, -0.0005, None).startswith("🟢")


def test_feature_hash_deterministic():
    recs = [{"race_id": "202705010101", "feats": {
        1: {"jrdb_idm": 0.1, "jrdb_kishu_idx": 0.2, "jrdb_joho_idx": 0.0,
            "wet_rel_rank": -0.1, "kinryo_per_weight": 0.05}}}]
    assert _feature_hash(recs) == _feature_hash([dict(recs[0])])


def test_feature_hash_changes_on_value():
    a = [{"race_id": "R", "feats": {1: {k: 0.0 for k in FROZEN["features"]}}}]
    b = [{"race_id": "R", "feats": {1: {**{k: 0.0 for k in FROZEN["features"]},
                                        "jrdb_idm": 0.9}}}]
    assert _feature_hash(a) != _feature_hash(b)
