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

    # ── 脚質 × 直線長（差し=1 × 長い直線 で有利）: コース形状マスタ由来 ──
    if "leg_type_binary" in df.columns and "course_straight_length" in df.columns:
        df["legtype_x_straight"] = (
            pd.to_numeric(df["leg_type_binary"], errors="coerce")
            * pd.to_numeric(df["course_straight_length"], errors="coerce")
        )

    # ── 枠番 × 幅員（広いコースは外枠不利が緩む）─────────────────────
    if "枠番" in df.columns and "course_width_max" in df.columns:
        df["frame_x_width"] = (
            pd.to_numeric(df["枠番"], errors="coerce")
            * pd.to_numeric(df["course_width_max"], errors="coerce")
        )

    # ── 脚質 × コース脚質バイアス（出走馬×コース相性）────────────────
    # fit = run_style_bias × (1 - 2*leg_type_binary): 前馬(0)→+bias, 差し馬(1)→−bias。
    if "leg_type_binary" in df.columns and "course_run_style_bias" in df.columns:
        leg = pd.to_numeric(df["leg_type_binary"], errors="coerce")
        bias = pd.to_numeric(df["course_run_style_bias"], errors="coerce")
        df["style_course_fit"] = bias * (1.0 - 2.0 * leg)

    # ── 脚質 × 距離別ガイド脚質バイアス（出走馬×コース×距離の相性）─────
    # course_master（track 単位）より粒度が細かい距離固有バイアスでの相性評価。
    if "leg_type_binary" in df.columns and "guide_run_style_bias" in df.columns:
        leg = pd.to_numeric(df["leg_type_binary"], errors="coerce")
        gbias = pd.to_numeric(df["guide_run_style_bias"], errors="coerce")
        df["style_guide_fit"] = gbias * (1.0 - 2.0 * leg)

    return df
