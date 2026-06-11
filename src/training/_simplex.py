"""シンプレックス（投票シェアベクトル）の変換と距離指標（純粋 numpy、I/O なし）。

単勝オッズは控除率付きパリミュチュエルなので、`s_i = (1/odds_i)/Σ(1/odds_j)` で
正規化するとオーバーラウンドが消え、投票シェアの推定値になる。
シンプレックス制約（Σs=1, s>0）を外すために **CLR 変換**（centered log-ratio:
`x = log s − mean(log s)`）を使う。ALR と違い基準馬を必要としないため、
取消（出走除外）があっても座標系が壊れず、8〜18 頭で対称に扱える。
sum-zero の自由度は逆変換（softmax）が吸収する。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.constants._odds_phases import PHASE_TIMELINE
from src.constants._odds_phases import normalize_phase

_EPS = 1e-12


def shares_from_odds(odds: np.ndarray) -> np.ndarray:
    """オッズ配列 → 投票シェア（Σ=1）。NaN/0 以下の要素は NaN のまま除外して正規化。

    返り値は入力と同じ長さで、無効要素は NaN。有効要素のシェア和が 1 になる。
    """
    odds = np.asarray(odds, dtype=float)
    inv = np.where(np.isfinite(odds) & (odds > 0), 1.0 / odds, np.nan)
    total = np.nansum(inv)
    if total <= 0:
        return np.full_like(odds, np.nan)
    return inv / total


def clr(shares: np.ndarray) -> np.ndarray:
    """シェアベクトル → CLR 座標（`log s − mean(log s)`、和がゼロ）。"""
    s = np.clip(np.asarray(shares, dtype=float), _EPS, None)
    log_s = np.log(s)
    return log_s - log_s.mean()


def clr_inv(x: np.ndarray) -> np.ndarray:
    """CLR 座標 → シェアベクトル（softmax。加法定数は自動的に吸収される）。"""
    x = np.asarray(x, dtype=float)
    z = np.exp(x - x.max())
    return z / z.sum()


def popularity_ranks(shares: np.ndarray) -> np.ndarray:
    """シェアから人気順位（1 始まり、シェア降順）を返す。"""
    order = np.argsort(-np.asarray(shares, dtype=float))
    ranks = np.empty(len(order), dtype=int)
    ranks[order] = np.arange(1, len(order) + 1)
    return ranks


def race_share_sequences(phase_table: pd.DataFrame) -> dict:
    """`snapshots_to_phase_table` の出力をレース別フェーズ別シェア系列へ変換する。

    Parameters
    ----------
    phase_table : MultiIndex (race_id, combo) × ``odds_<phase>`` 列の DataFrame。

    Returns
    -------
    dict[race_id, dict[phase, pd.Series]] :
        各フェーズで観測のあった馬（combo=馬番文字列）のシェア（Σ=1）。
        取消等で途中から消えた馬は、そのフェーズのベクトルから除外され再正規化される。
        フェーズは時系列順（PHASE_TIMELINE）に並ぶ。旧フェーズ名は正規化される。
    """
    sequences: dict = {}
    if phase_table is None or phase_table.empty:
        return sequences

    # 列 → フェーズ名（旧名は正規化。重複したら後勝ちでなく先勝ちを維持）
    phase_cols: dict[str, str] = {}
    for col in phase_table.columns:
        if not str(col).startswith("odds_"):
            continue
        phase = normalize_phase(str(col)[len("odds_"):])
        phase_cols.setdefault(phase, col)

    for race_id, group in phase_table.groupby(level=0):
        per_phase: dict[str, pd.Series] = {}
        horses = group.index.get_level_values(1)
        for phase in PHASE_TIMELINE:
            col = phase_cols.get(phase)
            if col is None:
                continue
            odds = group[col].to_numpy(dtype=float)
            mask = np.isfinite(odds) & (odds > 0)
            if mask.sum() < 2:  # 1 頭以下では分布にならない
                continue
            shares = shares_from_odds(odds[mask])
            per_phase[phase] = pd.Series(shares, index=horses[mask].astype(str))
        if per_phase:
            sequences[str(race_id)] = per_phase
    return sequences


# ---------------------------------------------------------------------------
# 距離・誤差指標（評価ハーネス用、純粋関数）
# ---------------------------------------------------------------------------


def kl_divergence(p: np.ndarray, q: np.ndarray) -> float:
    """KL(p ‖ q)。p=実現シェア、q=予測シェア。"""
    p = np.clip(np.asarray(p, dtype=float), _EPS, None)
    q = np.clip(np.asarray(q, dtype=float), _EPS, None)
    p = p / p.sum()
    q = q / q.sum()
    return float(np.sum(p * np.log(p / q)))


def share_mae(p: np.ndarray, q: np.ndarray) -> float:
    """シェアベクトルの平均絶対誤差。"""
    return float(np.mean(np.abs(np.asarray(p, dtype=float) - np.asarray(q, dtype=float))))


def odds_mape(odds_actual: np.ndarray, odds_pred: np.ndarray) -> float:
    """オッズの平均絶対パーセント誤差。"""
    a = np.asarray(odds_actual, dtype=float)
    p = np.asarray(odds_pred, dtype=float)
    mask = np.isfinite(a) & np.isfinite(p) & (a > 0)
    if not mask.any():
        return float("nan")
    return float(np.mean(np.abs(p[mask] - a[mask]) / a[mask]))
