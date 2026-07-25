"""§2b 交互作用特徴量のテスト。"""

import numpy as np
import pandas as pd
import pytest

from src.preprocessing._interaction_features import add_interaction_features


def _make_df():
    return pd.DataFrame(
        {
            "枠番": [1, 2, 3, 4],
            "race_type": pd.Categorical(["芝", "ダート", "芝", "障害"], categories=["芝", "ダート", "障害"]),
            "性": pd.Categorical(["牡", "牝", "セ", "牡"], categories=["牡", "牝", "セ"]),
            "date": pd.to_datetime(["2023-01-01", "2023-04-01", "2023-07-01", "2023-10-01"]),
            "course_len": [16, 18, 20, 14],
            "around": pd.Categorical(["右", "左", "直線", "右"], categories=["右", "左", "直線"]),
        }
    )


class TestFrameXCourse:
    def test_column_created(self):
        df = _make_df()
        result = add_interaction_features(df)
        assert "frame_x_course" in result.columns

    def test_values_are_product(self):
        df = _make_df()
        result = add_interaction_features(df)
        # race_type codes: 芝=0, ダート=1, 障害=2
        # row 0: 枠番=1, race_type=芝(0) → 1*0=0
        assert result["frame_x_course"].iloc[0] == pytest.approx(0.0)
        # row 1: 枠番=2, race_type=ダート(1) → 2*1=2
        assert result["frame_x_course"].iloc[1] == pytest.approx(2.0)

    def test_skips_when_column_absent(self):
        df = _make_df().drop(columns=["race_type"])
        result = add_interaction_features(df)
        assert "frame_x_course" not in result.columns


class TestSexXMonth:
    def test_columns_created(self):
        df = _make_df()
        result = add_interaction_features(df)
        assert "sex_x_month_sin" in result.columns
        assert "sex_x_month_cos" in result.columns

    def test_values_use_month(self):
        df = _make_df()
        result = add_interaction_features(df)
        month_jan = 1
        sex_0 = 0  # 牡 → code 0 → product is 0
        expected_sin = sex_0 * np.sin(2 * np.pi * month_jan / 12)
        assert result["sex_x_month_sin"].iloc[0] == pytest.approx(expected_sin)

    def test_skips_when_date_absent(self):
        df = _make_df().drop(columns=["date"])
        result = add_interaction_features(df)
        assert "sex_x_month_sin" not in result.columns


class TestDistanceXAround:
    def test_column_created(self):
        df = _make_df()
        result = add_interaction_features(df)
        assert "distance_x_around" in result.columns

    def test_values_are_product(self):
        df = _make_df()
        result = add_interaction_features(df)
        # around codes: 右=0, 左=1, 直線=2
        # row 0: course_len=16, around=右(0) → 16*0=0
        assert result["distance_x_around"].iloc[0] == pytest.approx(0.0)
        # row 1: course_len=18, around=左(1) → 18*1=18
        assert result["distance_x_around"].iloc[1] == pytest.approx(18.0)

    def test_skips_when_around_absent(self):
        df = _make_df().drop(columns=["around"])
        result = add_interaction_features(df)
        assert "distance_x_around" not in result.columns


class TestOriginalColumnsPreserved:
    def test_originals_intact(self):
        df = _make_df()
        result = add_interaction_features(df)
        # All original columns should still be present
        for col in df.columns:
            assert col in result.columns


# ──────────────────────────────────────────
# Phase 8: 追加交互作用
# ──────────────────────────────────────────

def _make_df_p8():
    return pd.DataFrame(
        {
            "年齢": pd.array([4, 5], dtype="Int64"),
            "course_len": [16.0, 20.0],
            "体重": [480, 500],
            "枠番": pd.array([3, 7], dtype="Int64"),
            "n_horses": [10, 16],
            "date": pd.to_datetime(["2024-01-01", "2024-01-01"]),
        }
    )


class TestPhase8Interactions:
    def test_age_x_distance(self):
        out = add_interaction_features(_make_df_p8())
        assert out["age_x_distance"].tolist() == [64.0, 100.0]

    def test_age_x_weight(self):
        out = add_interaction_features(_make_df_p8())
        assert out["age_x_weight"].tolist() == [1920.0, 2500.0]

    def test_frame_x_field(self):
        out = add_interaction_features(_make_df_p8())
        assert out["frame_x_field"].tolist() == [30.0, 112.0]

    def test_skips_when_cols_absent(self):
        out = add_interaction_features(_make_df_p8().drop(columns=["n_horses"]))
        assert "frame_x_field" not in out.columns
        assert "age_x_distance" in out.columns  # 他は生成される
