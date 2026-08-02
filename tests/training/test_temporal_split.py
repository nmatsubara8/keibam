"""時系列 standing protocol（証拠状態×学習可否・日付/状態 assert）の単体テスト。"""
from __future__ import annotations

import pytest

from src.training._temporal_split import (
    assert_clean_final_test,
    assert_selection_only_on_known,
    assert_test_after_cutoff,
    phase_counts,
    phase_of,
    refit_allowed,
    selection_allowed,
)


def test_phase_of_evidence_status():
    assert phase_of(2014) == "excluded"
    assert phase_of(2015) == "development_known" and phase_of(2024) == "development_known"
    assert phase_of(2025) == "burned_for_evidence" and phase_of(2026) == "burned_for_evidence"
    assert phase_of(2027) == "reserved_test"
    # consumed 済みの窓は再利用不可
    assert phase_of(2027, reserved_test_start=2028, consumed_test_years=(2027,)) == "consumed_test"
    assert phase_of(2028, reserved_test_start=2028, consumed_test_years=(2027,)) == "reserved_test"


def test_refit_allowed_includes_burned():
    # burned(2025-2026) は refit（係数学習）に使える＝test 前は全て可
    assert refit_allowed(2016, 2027) and refit_allowed(2025, 2027) and refit_allowed(2026, 2027)
    assert not refit_allowed(2027, 2027)          # test 年は refit に入れない
    assert not refit_allowed(2014, 2027)


def test_selection_only_on_known():
    assert selection_allowed(2020) and selection_allowed(2024)
    assert not selection_allowed(2025) and not selection_allowed(2027)
    assert assert_selection_only_on_known([2015, 2024]) is True
    with pytest.raises(ValueError):
        assert_selection_only_on_known([2024, 2025])   # burned で選択は不可


def test_phase_counts():
    c = phase_counts([2015, 2023, 2025, 2026, 2027])
    assert c["development_known"] == 2 and c["burned_for_evidence"] == 2 and c["reserved_test"] == 1


def test_clean_final_test_allows_burned_in_train():
    # train に 2025-2026(burned) を含めてよい・test は 2027
    assert assert_clean_final_test([2015, 2024, 2025, 2026], [2027]) is True


def test_clean_final_test_rejects_burned_as_test():
    with pytest.raises(ValueError):
        assert_clean_final_test([2015], [2025, 2026])


def test_clean_final_test_rejects_consumed():
    # consumed 済みの年を test に再利用しようとすると弾く
    with pytest.raises(ValueError):
        assert_clean_final_test([2015], [2027], reserved_test_start=2027,
                                consumed_test_years=(2027,))


def test_clean_final_test_next_hypothesis_uses_next_tranche():
    # B が 2027 を消費した後、新仮説は 2028+ を test に（train は 2027 を含めて refit 可）
    assert assert_clean_final_test([2015, 2026, 2027], [2028],
                                   reserved_test_start=2028, consumed_test_years=(2027,)) is True


def test_clean_final_test_time_order():
    with pytest.raises(ValueError):
        assert_clean_final_test([2028], [2027])


def test_assert_test_after_cutoff():
    assert assert_test_after_cutoff("2026-08-02", ["2026-09-01", "2027-01-01"]) is True
    with pytest.raises(ValueError):
        assert_test_after_cutoff("2026-08-02", ["2026-07-01", "2026-09-01"])
