"""Optuna 探索履歴の保存・選択（_tuning_history）のテスト。"""

import json
import os
import types

import pytest

from src.training._tuning_history import get_params_by_rank
from src.training._tuning_history import latest_version
from src.training._tuning_history import load_tuning_history
from src.training._tuning_history import save_tuning_history
from src.training._tuning_history import trials_to_records
from src.training._tuning_history import tuning_history_path


def _trial(number, value, params):
    """LightGBMTuner trial のダックタイプ（system_attrs に完全パラメータを持つ）。"""
    return types.SimpleNamespace(
        number=number,
        value=value,
        system_attrs={"lightgbm_tuner:lgbm_params": json.dumps(params)},
    )


def _study(trials, direction="MINIMIZE"):
    return types.SimpleNamespace(
        trials=trials,
        direction=types.SimpleNamespace(name=direction),
    )


class TestTrialsToRecords:
    def test_sorted_by_value_ascending_for_minimize(self):
        study = _study([
            _trial(0, 0.60, {"num_leaves": 4}),
            _trial(1, 0.55, {"num_leaves": 8}),
            _trial(2, 0.58, {"num_leaves": 16}),
        ])
        records = trials_to_records(study, "v1")
        assert [r["rank"] for r in records] == [1, 2, 3]
        assert [r["value"] for r in records] == [0.55, 0.58, 0.60]
        assert records[0]["params"] == {"num_leaves": 8}
        assert all(r["version"] == "v1" for r in records)

    def test_maximize_direction_sorts_descending(self):
        study = _study(
            [_trial(0, 0.70, {"a": 1}), _trial(1, 0.80, {"a": 2})],
            direction="MAXIMIZE",
        )
        records = trials_to_records(study, "v1")
        assert records[0]["value"] == 0.80

    def test_duplicate_params_keep_best(self):
        """段階探索は同一パラメータを重複試行するため、最良値だけ残す。"""
        study = _study([
            _trial(0, 0.60, {"num_leaves": 4}),
            _trial(1, 0.55, {"num_leaves": 4}),  # 同一パラメータの改善版
        ])
        records = trials_to_records(study, "v1")
        assert len(records) == 1
        assert records[0]["value"] == 0.55

    def test_skips_incomplete_trials(self):
        incomplete = types.SimpleNamespace(number=9, value=None, system_attrs={})
        study = _study([incomplete, _trial(0, 0.5, {"a": 1})])
        records = trials_to_records(study, "v1")
        assert len(records) == 1

    def test_top_n_limits(self):
        study = _study([_trial(i, 0.5 + i * 0.01, {"n": i}) for i in range(10)])
        records = trials_to_records(study, "v1", top_n=3)
        assert len(records) == 3


class TestSaveLoad:
    def test_roundtrip(self, tmp_path):
        path = tuning_history_path(str(tmp_path))
        study = _study([_trial(0, 0.5, {"a": 1})])
        save_tuning_history(trials_to_records(study, "v1"), path)
        history = load_tuning_history(path)
        assert len(history) == 1
        assert history[0]["params"] == {"a": 1}

    def test_same_version_replaced(self, tmp_path):
        path = tuning_history_path(str(tmp_path))
        save_tuning_history(trials_to_records(_study([_trial(0, 0.5, {"a": 1})]), "v1"), path)
        save_tuning_history(trials_to_records(_study([_trial(0, 0.4, {"a": 2})]), "v1"), path)
        history = load_tuning_history(path)
        assert len(history) == 1
        assert history[0]["params"] == {"a": 2}

    def test_different_versions_accumulate(self, tmp_path):
        path = tuning_history_path(str(tmp_path))
        save_tuning_history(trials_to_records(_study([_trial(0, 0.5, {"a": 1})]), "v1"), path)
        save_tuning_history(trials_to_records(_study([_trial(0, 0.4, {"a": 2})]), "v2"), path)
        assert len(load_tuning_history(path)) == 2

    def test_load_missing_returns_empty(self, tmp_path):
        assert load_tuning_history(os.path.join(tmp_path, "nope.json")) == []


class TestGetParamsByRank:
    def _history(self):
        return [
            {"version": "v1", "rank": 1, "params": {"a": 1}, "tuned_at": "2026-01-01"},
            {"version": "v1", "rank": 2, "params": {"a": 2}, "tuned_at": "2026-01-01"},
            {"version": "v2", "rank": 1, "params": {"a": 3}, "tuned_at": "2026-02-01"},
        ]

    def test_defaults_to_latest_version(self):
        assert get_params_by_rank(self._history(), 1) == {"a": 3}

    def test_explicit_version(self):
        assert get_params_by_rank(self._history(), 2, version="v1") == {"a": 2}

    def test_missing_rank_raises(self):
        with pytest.raises(ValueError):
            get_params_by_rank(self._history(), 99)

    def test_empty_history_raises(self):
        with pytest.raises(ValueError):
            get_params_by_rank([], 1)

    def test_latest_version_helper(self):
        assert latest_version(self._history()) == "v2"
        assert latest_version([]) is None


class TestCategoryAware:
    def test_records_tagged_with_category(self):
        records = trials_to_records(_study([_trial(0, 0.5, {"a": 1})]), "v1", category="central_turf")
        assert all(r["category"] == "central_turf" for r in records)

    def test_default_category_is_combined(self):
        records = trials_to_records(_study([_trial(0, 0.5, {"a": 1})]), "v1")
        assert records[0]["category"] == "combined"

    def test_same_version_different_category_accumulate(self, tmp_path):
        path = tuning_history_path(str(tmp_path))
        save_tuning_history(trials_to_records(_study([_trial(0, 0.5, {"a": 1})]), "v1", category="combined"), path)
        save_tuning_history(trials_to_records(_study([_trial(0, 0.4, {"a": 2})]), "v1", category="central_turf"), path)
        save_tuning_history(trials_to_records(_study([_trial(0, 0.3, {"a": 3})]), "v1", category="central_dirt"), path)
        history = load_tuning_history(path)
        assert len(history) == 3
        assert {r["category"] for r in history} == {"combined", "central_turf", "central_dirt"}

    def test_get_params_by_rank_filters_by_category(self):
        history = [
            {"version": "v1", "category": "combined", "rank": 1, "params": {"a": 1}, "tuned_at": "2026-01-01"},
            {"version": "v1", "category": "central_turf", "rank": 1, "params": {"a": 2}, "tuned_at": "2026-01-01"},
            {"version": "v1", "category": "central_dirt", "rank": 1, "params": {"a": 3}, "tuned_at": "2026-01-01"},
        ]
        assert get_params_by_rank(history, 1, category="central_turf") == {"a": 2}
        assert get_params_by_rank(history, 1, category="central_dirt") == {"a": 3}
        assert get_params_by_rank(history, 1, category="combined") == {"a": 1}

    def test_latest_version_per_category(self):
        history = [
            {"version": "v1", "category": "central_turf", "rank": 1, "params": {}, "tuned_at": "2026-01-01"},
            {"version": "v2", "category": "combined", "rank": 1, "params": {}, "tuned_at": "2026-02-01"},
        ]
        assert latest_version(history, category="central_turf") == "v1"
        assert latest_version(history, category="combined") == "v2"
