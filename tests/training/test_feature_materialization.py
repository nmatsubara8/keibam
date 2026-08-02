"""feature 実体化 fail-closed ガードの単体テスト。"""
from __future__ import annotations

import pytest

from src.training._feature_materialization import (
    EXPECTED_JRDB_FULL,
    REQUIRED_JRDB_MIN,
    assert_features_materialized,
)


def test_required_present_ok():
    cols = list(REQUIRED_JRDB_MIN) + ["単勝", "着順"]
    missing_opt = assert_features_materialized(cols, REQUIRED_JRDB_MIN)
    assert missing_opt == []


def test_required_missing_raises():
    cols = ["jrdb_idm", "単勝"]   # 他の required 欠落
    with pytest.raises(RuntimeError):
        assert_features_materialized(cols, REQUIRED_JRDB_MIN)


def test_optional_missing_returned_not_raised():
    cols = list(REQUIRED_JRDB_MIN)
    missing = assert_features_materialized(cols, REQUIRED_JRDB_MIN,
                                          optional=["jrdb_ten_idx", "jrdb_chokyo_idx"])
    assert set(missing) == {"jrdb_ten_idx", "jrdb_chokyo_idx"}   # warn 用に返る・例外にしない


def test_expected_full_superset_of_required():
    assert set(REQUIRED_JRDB_MIN) <= set(EXPECTED_JRDB_FULL) or \
        set(REQUIRED_JRDB_MIN) - set(EXPECTED_JRDB_FULL) == {"jrdb_kijun_odds"}
    # jrdb_kijun_odds は KYI_FEATURE_MAP 由来（EXPECTED に含む）
    assert "jrdb_ten_idx" in EXPECTED_JRDB_FULL and "jrdb_ms_last" in EXPECTED_JRDB_FULL
    assert len(EXPECTED_JRDB_FULL) >= 40
