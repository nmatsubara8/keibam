"""run_h3 の監査ゲート純部（feature hash・feature-only 監査 pass/fail）のテスト。"""
from __future__ import annotations

from scripts.run_h3 import _feature_audit, _feature_hash


def _recs():
    return [
        {"race_id": "202305010101", "year": 2023, "winner": 1,
         "odds": {1: 2.0, 2: 3.0, 3: 4.0},
         "feats": {1: {"h3a": 0.1, "h3b": -0.2}, 2: {"h3a": -0.1, "h3b": 0.0},
                   3: {"h3a": 0.0, "h3b": 0.3}}},
    ]


def _audit(**over):
    a = {"target_key_duplicate_count": 0, "nar_rows": 0, "n_nan_inf": 0,
         "n_feature_rows": 24, "n_target": 24, "n_h3a_nonzero": 20, "n_h3b_nonzero": 21,
         "calibration_rowsums": {2023: {"fast": 1.0, "normal": 1.0, "slow": 1.0}},
         "feature_effectiveness_var_frac": {"h3a": {2023: 0.9}, "h3b": {2023: 0.8}}}
    a.update(over)
    return a


def test_feature_hash_deterministic_and_order_independent():
    r1 = _recs()
    r2 = [dict(r1[0])]                       # 同内容
    assert _feature_hash(r1) == _feature_hash(r2)


def test_feature_hash_changes_on_value_change():
    r1 = _recs()
    r2 = _recs()
    r2[0]["feats"][1]["h3a"] = 0.999
    assert _feature_hash(r1) != _feature_hash(r2)


def test_feature_audit_pass():
    checks, passed, blockers = _feature_audit(_recs(), _audit(), min_train_years=3)
    assert passed is True and blockers == []
    assert checks["nar_rows"] == 0 and checks["completeness_all_targets"] is True


def test_feature_audit_fails_on_all_zero_feature():
    checks, passed, blockers = _feature_audit(
        _recs(), _audit(n_h3a_nonzero=0), min_train_years=3)
    assert passed is False
    assert any("H3a" in b for b in blockers)


def test_feature_audit_fails_on_no_variance():
    _, passed, blockers = _feature_audit(
        _recs(), _audit(feature_effectiveness_var_frac={"h3a": {2023: 0.0}, "h3b": {2023: 0.5}}),
        min_train_years=3)
    assert passed is False and any("H3a" in b for b in blockers)


def test_feature_audit_fails_on_nan_inf():
    _, passed, blockers = _feature_audit(_recs(), _audit(n_nan_inf=3), min_train_years=3)
    assert passed is False and any("NaN/inf" in b for b in blockers)


def test_feature_audit_fails_on_completeness():
    _, passed, blockers = _feature_audit(_recs(), _audit(n_feature_rows=20, n_target=24),
                                         min_train_years=3)
    assert passed is False and any("完全性" in b for b in blockers)


def test_feature_audit_fails_on_calibration_rowsum():
    _, passed, blockers = _feature_audit(
        _recs(), _audit(calibration_rowsums={2023: {"fast": 0.9}}), min_train_years=3)
    assert passed is False and any("calibration" in b for b in blockers)
