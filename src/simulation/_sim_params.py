"""Phase 2: featured の as-of 特徴量から エージェントシミュレーションの入力パラメータを作る。

field_from_featured(race_df) は「1レース分の featured 行群」→ RaceField（能力μ/脚質/スタミナ/σ）。
使う列はすべて**発走前に既知の as-of 特徴**（過去走から算出済み）だけなので前進安全（着順・単勝は
使わない）。列が無い環境でも中立既定へフォールバックする（manji 因子と同じスキーマ寛容の作法）。

ヒューリスティック（v1・学習なし）:
- ability : 能力シグナル（speed_fig/勝率/相対着順/Elo）をレース内 z-score 合成し 1.0 中心へ。
- style   : leg_type_binary（<0.4 先行 / >0.6 追込 / 中間 差し）。無ければ全 stalker。
- stamina : キャリア厚み（出走数）と距離実績を 0.8–1.4 にマップ。無ければ 1.0。
- noise   : 過去着順のばらつき（大きいほど不安定）を σ に。無ければ既定。

これは確率生成の**入力**であり、質は特徴量の質で決まる。市場を越えるかは Phase3 で判定する。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.simulation._agent_race import (
    STYLE_CLOSER,
    STYLE_FRONT,
    STYLE_STALKER,
    RaceField,
)

# 能力シグナル（列名, 符号）。符号+1=大きいほど強い、-1=小さいほど強い。
_ABILITY_SIGNALS = [
    ("speed_fig_best", 1.0),
    ("speed_fig_mean5", 1.0),
    ("win_rate_5R", 1.0),
    ("place_rate_5R", 0.5),
    ("avg_rel_rank_5R", -1.0),
    ("elo_rating", 1.0),
    ("elo_vs_field", 1.0),
]


def _num(df: pd.DataFrame, col: str):
    return pd.to_numeric(df[col], errors="coerce") if col in df.columns else None


def _zscore(s: pd.Series) -> pd.Series:
    sd = float(s.std())
    return (s - float(s.mean())) / (sd if sd > 1e-9 else 1.0)


def _ability_z(race_df: pd.DataFrame) -> pd.Series:
    """利用可能な能力シグナルをレース内 z-score して平均合成（無ければ 0）。"""
    parts = []
    for col, sign in _ABILITY_SIGNALS:
        s = _num(race_df, col)
        if s is not None and int(s.notna().sum()) >= 2:
            parts.append(sign * _zscore(s).fillna(0.0))
    if not parts:
        return pd.Series(0.0, index=race_df.index)
    return sum(parts) / len(parts)


def field_from_featured(
    race_df: pd.DataFrame,
    *,
    ability_spread: float = 0.20,
    stamina_lo: float = 0.8,
    stamina_hi: float = 1.4,
    noise_base: float = 0.04,
    noise_gain: float = 0.02,
) -> RaceField:
    """1レースの featured 行群 → RaceField（前進安全・スキーマ寛容）。"""
    n = len(race_df)
    z = _ability_z(race_df).to_numpy()
    ability = np.clip(1.0 + ability_spread * z, 0.3, 2.0)

    # 脚質: leg_type_binary から（無ければ全 stalker）
    style = np.full(n, STYLE_STALKER, dtype=int)
    lt = _num(race_df, "leg_type_binary")
    if lt is not None:
        v = lt.to_numpy()
        style = np.where(v < 0.4, STYLE_FRONT,
                         np.where(v > 0.6, STYLE_CLOSER, STYLE_STALKER))
        style = np.where(np.isnan(v), STYLE_STALKER, style).astype(int)

    # スタミナ: 出走数(経験) をレース内 z→[lo,hi]。無ければ 1.0。
    stamina = np.ones(n)
    exp = _num(race_df, "n_starts")
    if exp is None:
        exp = _num(race_df, "career_starts")
    if exp is not None and int(exp.notna().sum()) >= 2:
        ez = _zscore(exp).fillna(0.0).to_numpy()
        mid = 0.5 * (stamina_lo + stamina_hi)
        half = 0.5 * (stamina_hi - stamina_lo)
        stamina = np.clip(mid + half * np.tanh(ez), stamina_lo, stamina_hi)

    # ノイズ: 過去着順のばらつき（大きいほど不安定）。無ければ既定。
    noise = np.full(n, noise_base)
    sd = _num(race_df, "着順_std_5R")
    if sd is not None and int(sd.notna().sum()) >= 2:
        sdz = _zscore(sd).fillna(0.0).to_numpy()
        noise = np.clip(noise_base + noise_gain * sdz, 0.01, 0.2)

    # ゲート（枠順）: 馬番（無ければ枠番）をレース内で [0,1] に正規化（0=最内..1=最外）。
    # 序盤の位置取り優位に使う（前進安全: 発走前に確定する枠順は as-of 情報）。無ければ中立。
    gate = None
    draw = _num(race_df, "馬番")
    if draw is None or int(draw.notna().sum()) < 2:
        draw = _num(race_df, "枠番")
    if draw is not None and int(draw.notna().sum()) >= 2:
        dv = draw.to_numpy(dtype=float)
        lo, hi = np.nanmin(dv), np.nanmax(dv)
        if hi > lo:
            g = (dv - lo) / (hi - lo)
            gate = np.where(np.isnan(g), 0.5, g)

    return RaceField(ability=ability, style=style, stamina=stamina, noise=noise, gate=gate)
