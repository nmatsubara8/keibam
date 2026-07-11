"""ペース（前傾/後傾）を発走前情報から予測するための特徴量（前進安全）。

物理simは「先行タイプが多い→ハイペース」と素朴に仮定して実と逆相関(−0.25)になった。真の
ペースは騎手の駆け引き（先行多数→譲り合ってスロー等）で決まる。それを第一原理でなく
**データから学ぶ**——発走前に既知の隊列構成・各馬の力からペース(pace_diff)を回帰し、前進検証で
「そもそもペースは事前予測可能か」を測る。

pace_features は1レースの featured 行群 → 発走前特徴の dict（着順・単勝・当日ラップは使わない）。
- 隊列構成: 先行数/追込数/先行率、非線形項（先行数²＝譲り合いの逆U字を拾う）
- 力の分布: 能力(speed_fig等)の平均/分散、最速先行馬の力（速い逃げがいると速くなる）
- レース条件: 距離・芝ダ・頭数
これらはすべて as-of/条件で、当日結果に依存しない。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# 特徴量の順序（行列化のため固定）
PACE_FEATURE_NAMES = [
    "field_size", "n_front", "front_ratio", "n_front_sq",
    "ability_mean", "ability_std", "front_ability_max", "back_ability_max",
    "course_len", "is_dirt",
]


def _num(df: pd.DataFrame, col: str):
    return pd.to_numeric(df[col], errors="coerce") if col in df.columns else None


def _ability(df: pd.DataFrame) -> pd.Series:
    for c in ("speed_fig_best", "speed_fig_mean5", "win_rate_5R", "elo_rating"):
        s = _num(df, c)
        if s is not None and int(s.notna().sum()) >= 2:
            return s.fillna(s.median())
    return pd.Series(0.0, index=df.index)


def pace_features(race_df: pd.DataFrame) -> dict:
    """1レースの featured 行群 → 発走前ペース特徴（dict）。前進安全。"""
    n = len(race_df)
    lt = _num(race_df, "leg_type_binary")
    front_mask = (lt < 0.5) if lt is not None else pd.Series(False, index=race_df.index)
    back_mask = (lt >= 0.5) if lt is not None else pd.Series(False, index=race_df.index)
    n_front = int(front_mask.sum())
    ab = _ability(race_df)
    fa = ab[front_mask.to_numpy()] if front_mask.any() else pd.Series([np.nan])
    ba = ab[back_mask.to_numpy()] if back_mask.any() else pd.Series([np.nan])
    cl = _num(race_df, "course_len")
    rt = race_df["race_type"] if "race_type" in race_df.columns else None
    is_dirt = 0.0
    if rt is not None:
        is_dirt = float((rt.astype(str).str.contains("ダ")).any())
    return {
        "field_size": float(n),
        "n_front": float(n_front),
        "front_ratio": n_front / n if n else 0.0,
        "n_front_sq": float(n_front * n_front),
        "ability_mean": float(ab.mean()),
        "ability_std": float(ab.std()) if n > 1 else 0.0,
        "front_ability_max": float(np.nanmax(fa.to_numpy())) if front_mask.any() else float(ab.mean()),
        "back_ability_max": float(np.nanmax(ba.to_numpy())) if back_mask.any() else float(ab.mean()),
        "course_len": float(cl.iloc[0]) if cl is not None and len(cl) else 0.0,
        "is_dirt": is_dirt,
    }


def features_to_row(feat: dict) -> list:
    """dict → PACE_FEATURE_NAMES 順のリスト（行列化用）。"""
    return [feat.get(k, 0.0) for k in PACE_FEATURE_NAMES]
