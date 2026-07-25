"""コース形状マスタ（_course_shape.py）のユニットテスト。"""

from __future__ import annotations

import pandas as pd
import pytest

from src.preprocessing._course_shape import add_course_shape_features, load_course_master


def _seed(tmp_path):
    path = str(tmp_path / "course_master.csv")
    pd.DataFrame([
        {"place_code": "05", "race_type": "芝", "straight_length": 525.9, "elevation_diff": 2.1,
         "lap_length": 2083.1, "width_min": 31.0, "width_max": 41.0, "turn_direction": 1,
         "turf_type_code": 0, "corner_radius_large": 0, "has_spiral_curve": 1,
         "run_style_bias": -1, "time_bias": 1, "drainage_good": 1},
        {"place_code": "01", "race_type": "芝", "straight_length": 266.1, "elevation_diff": 0.7,
         "lap_length": 1640.9, "width_min": 25.0, "width_max": 27.0, "turn_direction": 0,
         "turf_type_code": 1, "corner_radius_large": 1, "has_spiral_curve": 0,
         "run_style_bias": 1, "time_bias": -1, "drainage_good": 1},
    ]).to_csv(path, index=False)
    return path


class TestLoad:
    def test_place_code_normalized(self, tmp_path):
        cm = load_course_master(_seed(tmp_path))
        assert set(cm["place_code"]) == {"05", "01"}

    def test_missing_returns_empty(self):
        assert load_course_master("/nonexistent.csv").empty


class TestAddCourseShape:
    def _results(self):
        return pd.DataFrame(
            {"開催": pd.array([5, 1], dtype="Int64"), "race_type": ["芝", "芝"], "course_len": [14, 12]},
            index=pd.Index(["r", "r"], name="race_id"),
        )

    def test_attaches_by_place_and_type(self, tmp_path):
        cm = load_course_master(_seed(tmp_path))
        out = add_course_shape_features(self._results(), cm)
        assert out["course_straight_length"].iloc[0] == pytest.approx(525.9)   # 東京
        assert out["course_lap_length"].iloc[1] == pytest.approx(1640.9)       # 札幌
        assert out["course_run_style_bias"].iloc[0] == pytest.approx(-1.0)     # 東京は差し有利

    def test_all_feature_cols_present(self, tmp_path):
        from src.constants._course_master import COURSE_MASTER_FEATURE_COLS

        cm = load_course_master(_seed(tmp_path))
        out = add_course_shape_features(self._results(), cm)
        for c in COURSE_MASTER_FEATURE_COLS:
            assert c in out.columns

    def test_distance_independent(self, tmp_path):
        cm = load_course_master(_seed(tmp_path))
        res = pd.DataFrame(
            {"開催": pd.array([5, 5], dtype="Int64"), "race_type": ["芝", "芝"], "course_len": [14, 24]},
            index=pd.Index(["a", "b"], name="race_id"),
        )
        out = add_course_shape_features(res, cm)
        assert out["course_straight_length"].iloc[0] == out["course_straight_length"].iloc[1]

    def test_empty_master_yields_nan_cols(self):
        out = add_course_shape_features(self._results(), pd.DataFrame())
        assert out["course_straight_length"].isna().all()

    def test_unknown_course_is_nan(self, tmp_path):
        cm = load_course_master(_seed(tmp_path))
        res = pd.DataFrame(
            {"開催": pd.array([9], dtype="Int64"), "race_type": ["ダート"], "course_len": [12]},
            index=pd.Index(["r"], name="race_id"),
        )
        out = add_course_shape_features(res, cm)
        assert pd.isna(out["course_straight_length"].iloc[0])


class TestCourseInteractions:
    def test_course_interactions_generated(self):
        from src.preprocessing._interaction_features import add_interaction_features

        df = pd.DataFrame({
            "開催": pd.array([5, 5], dtype="Int64"), "race_type": ["芝", "芝"],
            "leg_type_binary": [0.0, 1.0], "枠番": pd.array([2, 8], dtype="Int64"),
            "course_straight_length": [525.9, 525.9], "course_width_max": [41.0, 41.0],
            "course_run_style_bias": [-1.0, -1.0], "date": pd.to_datetime(["2024-01-01"] * 2),
        })
        out = add_interaction_features(df)
        assert out["legtype_x_straight"].tolist() == [0.0, 525.9]
        assert out["frame_x_width"].tolist() == [82.0, 328.0]
        # 差し有利コース(-1) で 差し馬(leg=1) が +1（好相性）
        assert out["style_course_fit"].tolist() == [-1.0, 1.0]
