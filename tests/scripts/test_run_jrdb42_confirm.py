"""JRDB42 confirmation 事前登録ハーネスの純部テスト（性能でなく凍結仕様と統計規則）。"""
from __future__ import annotations

import scripts.run_jrdb42_confirm as J


def test_frozen_features_are_active_plus_history_no_context():
    from src.training._feature_materialization import (CONTEXT_JRDB, CURRENT_ACTIVE_JRDB,
                                                       HISTORY_JRDB)
    feats = set(J.FROZEN["features"])
    assert feats == set(CURRENT_ACTIVE_JRDB) | set(HISTORY_JRDB)     # 33＋8
    assert set(CONTEXT_JRDB).isdisjoint(feats)                       # race-context は除外
    assert "jrdb_pace_hms" not in feats
    assert len(J.FROZEN["features"]) == 41


def test_frozen_registration_invariants():
    assert J.FROZEN["test_year"] == 2027 and J.FROZEN["reserved_test_start"] == 2027
    assert J.FROZEN["interim_looks"] == 0
    assert J.FROZEN["multiplicity"] == "holm"
    assert "B_RESIDUAL_HEAD_2027_CONFIRM" in J.FROZEN["family"]      # B と joint（m=2）
    assert J.FROZEN["mes_dnll"] == 0.001                             # nats/race


def test_holm_two_hypothesis_family():
    # 小さい p は alpha/2、次は alpha/1。両方十分小さければ両採択。
    out = J.holm_reject({"B": 0.001, "JRDB42": 0.02}, alpha=0.05)
    assert out["B"]["reject"] is True and abs(out["B"]["threshold"] - 0.025) < 1e-12
    assert out["JRDB42"]["reject"] is True and abs(out["JRDB42"]["threshold"] - 0.05) < 1e-12


def test_holm_step_down_blocks_after_first_failure():
    # 最小 p が閾値 alpha/2=0.025 を超えると step-down で以降も不採択（両方 fail）。
    out = J.holm_reject({"B": 0.03, "JRDB42": 0.04}, alpha=0.05)
    assert out["B"]["reject"] is False          # 最小 0.03 > 0.025 → 不採択→stop
    assert out["JRDB42"]["reject"] is False     # step-down で不採択


def test_verdict_rules():
    # 🟢 は CI 上限<0 かつ mean<=-MES かつ ECE 許容内
    assert "Confirmed" in J.verdict(-0.002, -0.0005, 0.0)
    assert "sub-MES" in J.verdict(-0.0003, -0.00001, 0.0)
    assert "Not confirmed" in J.verdict(+0.001, +0.002, 0.0)
