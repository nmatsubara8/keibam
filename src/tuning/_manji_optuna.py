"""Model 2 / Layer B: Optuna で 因子重み w_f・ゾーン境界・top_k を検証foldで探索する。

Layer A（回収率較正）が各因子の bucket 点を決めるが、①どの因子を使うか ②因子間の重み
③買うゾーン(odds帯)・top_k は未決定。ここを Optuna で最適化する。

前進安全性:
- 学習窓を calib(点較正) / valid(Optuna目的) に時間分割。test fold は一切触れない。
- points は calib で1回だけ較正（因子部分集合に依存しない＝各因子独立）。各 trial は
  重み・ゾーンを変えて valid 上で回収率を測るだけ（高速: score = P @ w）。
- w_f∈[w_min,w_max]。w≈0 は実質「その因子を外す」＝**因子を24に固定しない**。
  w_min<0 なら方向反転も可（符号付き）。
- 目的 = valid回収率 − parsimony·(採用因子率)。買い目 < min_bets の trial は棄却。

過学習は「外側 walk-forward の test fold（未使用）＋プラシーボ」で最終検証される。
Optuna が valid に過適合すれば test で伸びない/プラシーボが崩れる、という自己検査になる。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.tuning._manji_calibration import _win_and_odds, calibrate_points


def optimize_manji_config(
    calib: pd.DataFrame,
    valid: pd.DataFrame,
    factor_names: list[str],
    *,
    n_trials: int = 60,
    w_min: float = 0.0,
    w_max: float = 2.0,
    odds_lo_range: tuple[float, float] = (1.0, 6.0),
    odds_hi_range: tuple[float, float] = (10.0, 80.0),
    top_k_choices: tuple[int, ...] = (1, 2, 3, 4, 5),
    min_bets: int = 500,
    parsimony: float = 0.02,
    active_thresh: float = 0.05,
    seed: int = 0,
    **cal_kwargs,
) -> dict:
    """calib で点較正 → valid 上で Optuna 探索。best config を dict で返す。

    Returns: {points, weights, zone(odds_lo,odds_hi), top_k, value(valid回収率), n_active}
    """
    import optuna

    from src.policies._manji_factors import buckets

    points = calibrate_points(calib, factor_names, **cal_kwargs)
    active = [f for f in factor_names if points.get(f)]
    if not active or valid.empty:
        return {"points": points, "weights": {}, "zone": (3.0, 50.0),
                "top_k": 3, "value": 0.0, "n_active": 0}

    bk = buckets(valid, active)
    # 点行列 P（行=valid馬, 列=active因子）。trial 毎に score = P @ w で高速化。
    P = np.column_stack([
        bk[f].map(lambda b, pm=points[f]: pm.get(b, 0.0)).to_numpy(dtype=float)
        for f in active
    ])
    win, odds = _win_and_odds(valid)
    odds = odds.to_numpy(dtype=float)
    ret = odds * win.to_numpy(dtype=float)
    finite = np.isfinite(ret) & np.isfinite(odds)
    race_code = pd.factorize(valid.index)[0]

    optuna.logging.set_verbosity(optuna.logging.WARNING)

    def objective(trial):
        w = np.array([trial.suggest_float(f"w::{f}", w_min, w_max) for f in active])
        lo = trial.suggest_float("odds_lo", *odds_lo_range)
        hi = trial.suggest_float("odds_hi", *odds_hi_range)
        if hi <= lo + 1.0:
            return -1.0
        top_k = trial.suggest_categorical("top_k", list(top_k_choices))
        score = P @ w
        rank = pd.Series(score).groupby(race_code).rank(ascending=False, method="min").to_numpy()
        keep = finite & (rank <= top_k) & (odds >= lo) & (odds <= hi)
        nb = int(keep.sum())
        if nb < min_bets:
            return -1.0
        # recovery = Σ(odds×的中) / 買い目数（フラット100円: 100倍が分子分母で相殺）
        recovery = float(ret[keep].sum() / nb)
        n_active = int((np.abs(w) > active_thresh).sum())
        return recovery - parsimony * (n_active / len(active))

    study = optuna.create_study(
        direction="maximize", sampler=optuna.samplers.TPESampler(seed=seed))
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)

    bp = study.best_params
    weights = {f: float(bp[f"w::{f}"]) for f in active}
    zone = (float(bp["odds_lo"]), float(bp["odds_hi"]))
    n_active = sum(1 for f in active if abs(weights[f]) > active_thresh)
    return {"points": points, "weights": weights, "zone": zone,
            "top_k": int(bp["top_k"]), "value": float(study.best_value), "n_active": n_active}
