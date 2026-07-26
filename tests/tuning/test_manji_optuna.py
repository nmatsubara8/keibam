"""Layer B（Optuna 探索）の単体テスト。合成データで軽く（少試行）動作を検証。"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.tuning._manji_optuna import optimize_manji_config


def _synth(n_races=800, seed=0):
    rng = np.random.default_rng(seed)
    rows, idx = [], []
    base = pd.Timestamp("2015-01-01")
    for i in range(n_races):
        rid = f"R{i:05d}"
        day = base + pd.Timedelta(days=i // 3)
        # paddock '穴' に小さな本物のエッジ
        pads = [str(rng.choice(["A", "B", "穴"], p=[.5, .4, .1])) for _ in range(6)]
        wp = np.array([1.0 + (0.6 if p == "穴" else 0) for p in pads])
        wp /= wp.sum()
        winner = int(rng.choice(range(6), p=wp))
        for u in range(6):
            odds = round(0.8 / (1 / 6) * (1 + rng.uniform(-.3, .3)), 1)
            rows.append({"馬番": u + 1, "着順": 1 if u == winner else u + 2, "単勝": float(odds),
                         "年齢": int(rng.choice([2, 3, 4, 5, 6])), "斤量": 55.0,
                         "パドック評価__A": int(pads[u] == "A"), "パドック評価__B": int(pads[u] == "B"),
                         "パドック評価__穴": int(pads[u] == "穴"), "date": day})
            idx.append(rid)
    return pd.DataFrame(rows, index=idx)


def test_optimize_returns_valid_config():
    df = _synth()
    cut = int(len(set(df.index)) * 0.7)
    races = list(pd.to_datetime(df["date"]).groupby(level=0).first().sort_values().index)
    calib = df.loc[races[:cut]]
    valid = df.loc[races[cut:]]
    res = optimize_manji_config(
        calib, valid, ["paddock", "age", "umaban_parity"],
        n_trials=8, min_bets=20, min_n=20, universality_slices=1, seed=1,
    )
    assert set(res) >= {"points", "weights", "zone", "top_k", "value", "n_active"}
    assert res["top_k"] in (1, 2, 3, 4, 5)
    lo, hi = res["zone"]
    assert hi > lo
    # 重みは active 因子に付く（0=除外を含む探索なので範囲[0,2]）
    for v in res["weights"].values():
        assert 0.0 <= v <= 2.0


def test_optimize_with_bayes_points_limited_trials():
    """新しい100基準ベイズ点較正器を points_fn で注入し、少試行 Optuna が回る。"""
    from src.tuning._manji_posterior import PosteriorConfig, calibrate_points_bayes

    df = _synth()
    races = list(pd.to_datetime(df["date"]).groupby(level=0).first().sort_values().index)
    cut = int(len(races) * 0.7)
    calib, valid = df.loc[races[:cut]], df.loc[races[cut:]]

    def bayes_fn(d, fn):
        return calibrate_points_bayes(
            d, fn, cfg=PosteriorConfig(min_n=20, universality_slices=1))

    res = optimize_manji_config(
        calib, valid, ["paddock", "age", "umaban_parity"],
        n_trials=4, min_bets=20, points_fn=bayes_fn, seed=1,
    )
    assert set(res) >= {"points", "weights", "zone", "top_k", "value", "n_active"}
    assert res["top_k"] in (1, 2, 3, 4, 5)


def test_optimize_handles_no_active_factors():
    # 因子が featured に無い → active 空 → 既定 config を返す
    df = _synth(n_races=200)
    res = optimize_manji_config(df, df, ["sire_line", "prev_finish"],
                                n_trials=3, min_n=20, universality_slices=1)
    assert res["n_active"] == 0
    assert res["weights"] == {}
