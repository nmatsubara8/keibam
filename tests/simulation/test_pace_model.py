"""発走前ペース特徴 pace_features の単体テスト（前進安全・構成の反映）。"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.simulation._pace_model import (
    PACE_FEATURE_NAMES,
    features_to_row,
    pace_features,
)


def _race(leg, ability, course_len=1600, race_type="芝"):
    n = len(leg)
    return pd.DataFrame(
        {"leg_type_binary": leg, "speed_fig_best": ability,
         "course_len": [course_len] * n, "race_type": [race_type] * n},
        index=["R"] * n,
    )


def test_counts_front_and_back():
    df = _race([0.0, 0.0, 0.0, 1.0, 1.0], [70, 60, 50, 55, 45])
    f = pace_features(df)
    assert f["field_size"] == 5.0
    assert f["n_front"] == 3.0
    assert f["front_ratio"] == 0.6
    assert f["n_front_sq"] == 9.0


def test_front_ability_max_picks_fastest_frontrunner():
    df = _race([0.0, 0.0, 1.0], [80, 60, 90])   # 先行は80,60 → max80; 追込90は別
    f = pace_features(df)
    assert f["front_ability_max"] == 80.0
    assert f["back_ability_max"] == 90.0


def test_dirt_flag_and_distance():
    df = _race([0.0, 1.0], [50, 50], course_len=1200, race_type="ダート")
    f = pace_features(df)
    assert f["is_dirt"] == 1.0
    assert f["course_len"] == 1200.0


def test_forward_safe_ignores_result_columns():
    df = _race([0.0, 1.0], [60, 55])
    leak = df.copy()
    leak["着順"] = [1, 2]; leak["単勝"] = [1.5, 9.0]; leak["通過"] = ["1-1", "5-5"]
    a = features_to_row(pace_features(df))
    b = features_to_row(pace_features(leak))
    assert a == b                                # 結果列を足しても特徴は不変


def test_row_order_matches_names():
    df = _race([0.0, 1.0], [60, 55])
    row = features_to_row(pace_features(df))
    assert len(row) == len(PACE_FEATURE_NAMES)
    assert all(np.isfinite(v) for v in row)


def test_schema_tolerant_no_legtype():
    df = pd.DataFrame({"speed_fig_best": [60, 55]}, index=["R", "R"])
    f = pace_features(df)                          # leg_type 無し → front 0 でも落ちない
    assert f["n_front"] == 0.0 and f["field_size"] == 2.0
