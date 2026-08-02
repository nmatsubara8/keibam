"""時系列3分割 standing protocol の単体テスト。"""
from __future__ import annotations

import pytest

from src.training._temporal_split import (
    assert_clean_final_test,
    assert_freeze_before_test,
    phase_counts,
    phase_of,
    split_records,
)


def test_phase_of_boundaries():
    assert phase_of(2014) == "excluded"      # pre-2015 stub
    assert phase_of(2015) == "dev" and phase_of(2022) == "dev"
    assert phase_of(2023) == "val" and phase_of(2024) == "val"
    assert phase_of(2025) == "burned" and phase_of(2026) == "burned"
    assert phase_of(2027) == "test" and phase_of(2030) == "test"
    assert phase_of(None) == "excluded"


def test_split_records():
    recs = [{"year": y} for y in (2016, 2023, 2025, 2027, 2010)]
    s = split_records(recs)
    assert len(s["dev"]) == 1 and len(s["val"]) == 1
    assert len(s["burned"]) == 1 and len(s["test"]) == 1 and len(s["excluded"]) == 1


def test_phase_counts():
    c = phase_counts([2015, 2016, 2023, 2025, 2026, 2027])
    assert c["dev"] == 2 and c["val"] == 1 and c["burned"] == 2 and c["test"] == 1


def test_assert_clean_final_test_ok():
    assert assert_clean_final_test([2015, 2016, 2023, 2024], [2027]) is True


def test_assert_clean_final_test_rejects_burned():
    with pytest.raises(ValueError):
        assert_clean_final_test([2015, 2016], [2025, 2026])   # 観測済＝不可


def test_assert_clean_final_test_rejects_overlap():
    with pytest.raises(ValueError):
        assert_clean_final_test([2027], [2027])


def test_assert_clean_final_test_rejects_time_order():
    with pytest.raises(ValueError):
        assert_clean_final_test([2028], [2027])               # train が test 以降


def test_assert_freeze_before_test_ok():
    assert assert_freeze_before_test([2015, 2024], [2027]) is True


def test_assert_freeze_before_test_rejects_test_year_in_freeze():
    with pytest.raises(ValueError):
        assert_freeze_before_test([2015, 2027], [2027])       # test 窓年で freeze＝汚染
