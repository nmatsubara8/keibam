"""§2b 交互作用特徴量の生成。

GBDT は深いツリーで交互作用を近似できるが、One-Hot 化後の組合せは明示生成が有効。
NN(base②) には渡さず LightGBM(base①) 専用の特徴量セットとして扱う。

`add_interaction_features(df)` を `FeatureEngineering.add_interaction_features()` から呼ぶ。
各列が存在しない場合は該当特徴量をスキップする（欠損耐性）。
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def add_interaction_features(df: pd.DataFrame) -> pd.DataFrame:
    """§2b: frame_x_course / sex_x_month_sin/cos / distance_x_around を追加する。

    各元列が存在しない場合は該当列をスキップする。
    dummify 前に呼ぶこと（race_type / around / 性 が必要）。
    """
    df = df.copy()

    # ── frame × course_type ──────────────────────────────────────────
    # 枠番(int) × race_type(カテゴリコード)
    if "枠番" in df.columns and "race_type" in df.columns:
        race_type_num = pd.Categorical(df["race_type"]).codes.astype(float)
        race_type_num = pd.Series(race_type_num, index=df.index)
        race_type_num[race_type_num < 0] = float("nan")  # unseen → NaN
        df["frame_x_course"] = df["枠番"].astype(float) * race_type_num

    # ── sex × 出走月 (sin / cos) ─────────────────────────────────────
    # 性(カテゴリコード) × sin/cos(2π×月/12)
    if "性" in df.columns and "date" in df.columns:
        month = pd.to_datetime(df["date"]).dt.month.astype(float)
        sex_num = pd.Categorical(df["性"]).codes.astype(float)
        sex_num = pd.Series(sex_num, index=df.index)
        sex_num[sex_num < 0] = float("nan")
        df["sex_x_month_sin"] = sex_num * np.sin(2 * np.pi * month / 12)
        df["sex_x_month_cos"] = sex_num * np.cos(2 * np.pi * month / 12)

    # ── distance × around ────────────────────────────────────────────
    # course_len(100m単位 int) × around(カテゴリコード)
    if "course_len" in df.columns and "around" in df.columns:
        around_num = pd.Categorical(df["around"]).codes.astype(float)
        around_num = pd.Series(around_num, index=df.index)
        around_num[around_num < 0] = float("nan")
        df["distance_x_around"] = df["course_len"].astype(float) * around_num

    # ── Phase 8: age × distance（若駒の距離替わり影響）──────────────────
    if "年齢" in df.columns and "course_len" in df.columns:
        df["age_x_distance"] = (
            pd.to_numeric(df["年齢"], errors="coerce") * df["course_len"].astype(float)
        )

    # ── Phase 8: age × weight（若齢戦の馬格）──────────────────────────
    if "年齢" in df.columns and "体重" in df.columns:
        df["age_x_weight"] = (
            pd.to_numeric(df["年齢"], errors="coerce") * pd.to_numeric(df["体重"], errors="coerce")
        )

    # ── Phase 8: frame × field size（頭数込みの枠の価値）───────────────
    if "枠番" in df.columns and "n_horses" in df.columns:
        df["frame_x_field"] = (
            pd.to_numeric(df["枠番"], errors="coerce") * pd.to_numeric(df["n_horses"], errors="coerce")
        )

    # ── Phase 9: 脚質 × 直線長（差し=1 × 長い直線 で有利）─────────────
    if "leg_type_binary" in df.columns and "course_straight_length" in df.columns:
        df["legtype_x_straight"] = (
            pd.to_numeric(df["leg_type_binary"], errors="coerce")
            * pd.to_numeric(df["course_straight_length"], errors="coerce")
        )

    # ── Phase 9-rev: 枠番 × 幅員（広いコースは外枠不利が緩む）─────────
    if "枠番" in df.columns and "course_width_max" in df.columns:
        df["frame_x_width"] = (
            pd.to_numeric(df["枠番"], errors="coerce")
            * pd.to_numeric(df["course_width_max"], errors="coerce")
        )

    # ── Phase 9-rev: 脚質 × コース脚質バイアス（出走馬×コース相性）────
    # 前脚質(leg_type_binary=0) は run_style_bias 正(前有利)で加点、差し(1)は負で加点。
    # fit = run_style_bias × (1 - 2*leg_type_binary): 前馬→+bias, 差し馬→−bias。
    if "leg_type_binary" in df.columns and "course_run_style_bias" in df.columns:
        leg = pd.to_numeric(df["leg_type_binary"], errors="coerce")
        bias = pd.to_numeric(df["course_run_style_bias"], errors="coerce")
        df["style_course_fit"] = bias * (1.0 - 2.0 * leg)

    return df
