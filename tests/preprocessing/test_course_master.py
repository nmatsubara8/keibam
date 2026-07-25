"""Phase 9: コース形状マスタ（_course_master.py）のユニットテスト。"""

from __future__ import annotations

import pandas as pd
import pytest

from src.preprocessing._course_master import attach_course_features, load_course_master


def _seed(tmp_path):
    path = str(tmp_path / "course_master.csv")
    pd.DataFrame([
        {"place_code": "05", "race_type": "芝", "course_len": 14,
         "straight_length": 525.9, "elevation_diff": 2.5, "has_final_hill": 1, "first_corner_dist": 500.0},
        {"place_code": "06", "race_type": "芝", "course_len": 20,
         "straight_length": 310.0, "elevation_diff": 5.3, "has_final_hill": 1, "first_corner_dist": 400.0},
    ]).to_csv(path, index=False)
    return path


class TestLoad:
    def test_place_code_normalized(self, tmp_path):
        cm = load_course_master(_seed(tmp_path))
        assert cm["place_code"].tolist() == ["05", "06"]

    def test_missing_returns_empty(self):
        assert load_course_master("/nonexistent.csv").empty


class TestAttach:
    def _results(self):
        return pd.DataFrame(
            {"開催": pd.array([5, 6], dtype="Int64"), "race_type": ["芝", "芝"],
             "course_len": [14, 20]},
            index=pd.Index(["r", "r"], name="race_id"),
        )

    def test_attaches_by_int_place(self, tmp_path):
        cm = load_course_master(_seed(tmp_path))
        out = attach_course_features(self._results(), cm)
        # 開催=5(Int64) → place_code '05' にマッチ
        assert out["course_straight_length"].iloc[0] == pytest.approx(525.9)
        assert out["course_elevation_diff"].iloc[1] == pytest.approx(5.3)

    def test_all_feature_cols_present(self, tmp_path):
        from src.constants._course_master import COURSE_MASTER_FEATURE_COLS

        cm = load_course_master(_seed(tmp_path))
        out = attach_course_features(self._results(), cm)
        for c in COURSE_MASTER_FEATURE_COLS:
            assert c in out.columns

    def test_empty_master_yields_nan_cols(self):
        out = attach_course_features(self._results(), pd.DataFrame())
        assert out["course_straight_length"].isna().all()

    def test_unknown_course_is_nan(self, tmp_path):
        cm = load_course_master(_seed(tmp_path))
        res = pd.DataFrame(
            {"開催": pd.array([9], dtype="Int64"), "race_type": ["ダート"], "course_len": [12]},
            index=pd.Index(["r"], name="race_id"),
        )
        out = attach_course_features(res, cm)
        assert pd.isna(out["course_straight_length"].iloc[0])

    def test_missing_key_cols_skips(self):
        res = pd.DataFrame({"foo": [1]}, index=pd.Index(["r"], name="race_id"))
        out = attach_course_features(res, pd.DataFrame([{"place_code": "05"}]))
        assert out["course_straight_length"].isna().all()
