"""Phase 5: エンティティ統計（_entity_stats.py）のユニットテスト。"""

from __future__ import annotations

import pandas as pd
import pytest

from src.preprocessing._entity_stats import (
    compute_entity_stats,
    entity_stats_path,
    load_entity_stats,
    save_entity_stats,
)


def _past():
    return pd.DataFrame(
        {
            "jockey_id": ["J1", "J1", "J2", "J1"],
            "着順": [1, 3, 1, 2],
            "n_horses": [10, 10, 8, 10],
            "date": pd.to_datetime(["2023-01-01", "2023-02-01", "2023-01-01", "2023-03-01"]),
        }
    )


class TestComputeEntityStats:
    def test_win_rate_correct(self):
        s = compute_entity_stats(_past(), "jockey_id", "jockey_win_rate", "jockey_avg_rank", 30)
        # J1: 着順=[1,3,2] → win=1/3
        assert s.loc["J1", "jockey_win_rate"] == pytest.approx(1 / 3)
        assert s.loc["J2", "jockey_win_rate"] == pytest.approx(1.0)

    def test_avg_rank_is_relative(self):
        s = compute_entity_stats(_past(), "jockey_id", "jockey_win_rate", "jockey_avg_rank", 30)
        # J1 rel_rank = [1/10, 3/10, 2/10] → mean = 0.2
        assert s.loc["J1", "jockey_avg_rank"] == pytest.approx(0.2)

    def test_recent_n_limits_window(self):
        s = compute_entity_stats(_past(), "jockey_id", "w", "r", 1)
        # J1 最新1走 = 2023-03-01 着順2 → win=0
        assert s.loc["J1", "w"] == pytest.approx(0.0)

    def test_empty_returns_columned_frame(self):
        s = compute_entity_stats(pd.DataFrame(), "jockey_id", "w", "r", 30)
        assert list(s.columns) == ["w", "r"] and s.empty

    def test_missing_id_col_returns_empty(self):
        s = compute_entity_stats(_past().drop(columns=["jockey_id"]), "jockey_id", "w", "r", 30)
        assert s.empty


class TestSaveLoad:
    def test_roundtrip(self, tmp_path):
        s = compute_entity_stats(_past(), "jockey_id", "jockey_win_rate", "jockey_avg_rank", 30)
        path = entity_stats_path(str(tmp_path), "jockey_id")
        save_entity_stats(s, path)
        loaded = load_entity_stats(path)
        assert loaded.loc["J1", "jockey_win_rate"] == pytest.approx(s.loc["J1", "jockey_win_rate"])

    def test_load_missing_returns_empty(self):
        assert load_entity_stats("/nonexistent/x.csv").empty

    def test_path_format(self):
        assert entity_stats_path("data/master", "owner_id").endswith("entity_stats_owner_id.csv")
