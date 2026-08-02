"""H3 strictly-prior コアの単体テスト（リーク安全が最重要）。"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.features._strictly_prior import (
    PACE_FAST,
    PACE_NORMAL,
    PACE_SLOW,
    has_leak,
    lap_aptitude,
    market_anchored_perf,
    pace_aptitude,
    pace_state_from_balance,
    pace_state_residuals,
    pace_states_of_runs,
    shrink,
    strictly_prior_runs,
)


def _runs():
    return pd.DataFrame({
        "date": ["2020-01-01", "2020-06-01", "2021-03-01", "2021-03-01"],
        "race_id": ["A", "B", "T", "C"],
        "人気": [1, 3, 2, 5],
        "着順": [1, 5, 1, 2],
        "頭数": [10, 10, 12, 12],
        "上り": [34.5, 36.0, 34.0, 35.0],
        "ペース": ["35.1-36.8", "36.9-35.0", "34.8-36.2", "35.5-35.6"],
    })


# ---- リーク安全（最重要）--------------------------------------------------------------------

def test_strictly_prior_excludes_future_and_same_date():
    r = _runs()
    # target = 2021-03-01 の race T。厳密prior＝2020のみ（同日は除外）、T自身も除外
    out = strictly_prior_runs(r, "2021-03-01", target_race_id="T")
    assert set(out["race_id"]) == {"A", "B"}          # 同日(C,T)は入らない
    assert not has_leak(out, "2021-03-01", target_race_id="T")


def test_strictly_prior_target_race_excluded_even_if_earlier_listed():
    r = _runs()
    out = strictly_prior_runs(r, "2021-06-01", target_race_id="T")
    assert "T" not in set(out["race_id"])              # target 自身は日付が前でも除外
    assert set(out["race_id"]) == {"A", "B", "C"}


def test_strictly_prior_nat_dropped():
    r = _runs()
    r.loc[0, "date"] = None
    out = strictly_prior_runs(r, "2021-06-01")
    assert "A" not in set(out["race_id"])              # NaT は安全側で除外


def test_has_leak_detects_future():
    r = _runs()
    assert has_leak(r, "2020-12-31") is True           # 2021 の走が未来
    assert has_leak(r.iloc[:2], "2020-12-31") is False


def test_strictly_prior_empty_and_missing_col():
    assert len(strictly_prior_runs(pd.DataFrame({"date": []}), "2020-01-01")) == 0
    import pytest
    with pytest.raises(KeyError):
        strictly_prior_runs(pd.DataFrame({"x": [1]}), "2020-01-01")


# ---- 市場アンカー残差 -----------------------------------------------------------------------

def test_market_anchored_perf():
    r = _runs()
    perf = market_anchored_perf(r)
    # row0: 人気1/10 − 着順1/10 = 0.0（人気どおり）
    assert abs(perf.iloc[0] - 0.0) < 1e-12
    # row1: 3/10 − 5/10 = -0.2（人気より負けた）
    assert abs(perf.iloc[1] - (-0.2)) < 1e-12
    # row2: 2/12 − 1/12 = +1/12（人気より好走）
    assert perf.iloc[2] > 0


def test_market_anchored_perf_zero_field_nan():
    r = pd.DataFrame({"人気": [1], "着順": [1], "頭数": [0]})
    assert np.isnan(market_anchored_perf(r).iloc[0])


# ---- ペース状態 ----------------------------------------------------------------------------

def test_pace_state_from_balance_thresholds():
    assert pace_state_from_balance(1.0) == PACE_FAST      # 前傾=ハイ
    assert pace_state_from_balance(-1.0) == PACE_SLOW
    assert pace_state_from_balance(0.0) == PACE_NORMAL
    assert pace_state_from_balance(None) is None
    assert pace_state_from_balance(float("nan")) is None


def test_pace_states_prefers_sed():
    r = pd.DataFrame({"race_pace": ["H", "S", "M"], "ペース": ["x", "y", "z"]})
    z = pace_states_of_runs(r)
    assert list(z) == [PACE_FAST, PACE_SLOW, PACE_NORMAL]


def test_pace_states_fallback_to_string():
    r = pd.DataFrame({"ペース": ["35.0-36.5", "36.9-35.0", "35.5-35.6"]})
    z = pace_states_of_runs(r)
    assert z.iloc[0] == PACE_FAST      # back-front=+1.5>0.6
    assert z.iloc[1] == PACE_SLOW      # -1.9<-0.6
    assert z.iloc[2] == PACE_NORMAL    # +0.1


# ---- 縮約 ----------------------------------------------------------------------------------

def test_shrink():
    assert shrink(1.0, 0, k=5) == 0.0                    # 履歴ゼロ→prior
    assert abs(shrink(1.0, 5, k=5) - 0.5) < 1e-12        # n=k で半分
    assert abs(shrink(1.0, 15, k=5) - 0.75) < 1e-12


# ---- H3a 集約 ------------------------------------------------------------------------------

def test_pace_state_residuals_counts_and_shrink():
    r = pd.DataFrame({
        "race_pace": ["H", "H", "S"],
        "人気": [1, 2, 1], "着順": [1, 1, 5], "頭数": [10, 10, 10],
    })
    res = pace_state_residuals(r, k=1.0)
    assert res["n_z"][PACE_FAST] == 2 and res["n_z"][PACE_SLOW] == 1
    assert res[PACE_FAST] > 0     # ハイで人気以上に好走→正
    assert res[PACE_SLOW] < 0     # スローで凡走→負


def test_pace_aptitude_forecast_weighting_and_missing():
    r = pd.DataFrame({
        "race_pace": ["H", "H", "S"],
        "人気": [1, 2, 1], "着順": [1, 1, 5], "頭数": [10, 10, 10],
    })
    # ハイ寄り予想なら正、スロー寄りなら負（fast残差>0, slow残差<0）
    a_hi = pace_aptitude(r, {PACE_FAST: 1.0}, k=1.0)
    a_lo = pace_aptitude(r, {PACE_SLOW: 1.0}, k=1.0)
    assert a_hi > 0 > a_lo
    assert np.isnan(pace_aptitude(r, {}, k=1.0))         # 予想無し→NaN(安全欠損)


def test_pace_aptitude_normalizes_forecast():
    r = pd.DataFrame({"race_pace": ["H"], "人気": [1], "着順": [1], "頭数": [10]})
    # 非正規化 forecast も内部で正規化（合計2→重み1）
    a = pace_aptitude(r, {PACE_FAST: 2.0}, k=0.0)
    assert abs(a - pace_state_residuals(r, k=0.0)[PACE_FAST]) < 1e-12


# ---- H3b 集約 ------------------------------------------------------------------------------

def test_lap_aptitude_basic_and_baseline():
    r = pd.DataFrame({"上り": [34.0, 36.0], "ペース": ["35.0-36.0", "35.0-36.0"]})
    out = lap_aptitude(r, k=0.0)
    assert abs(out["late3f"] - 35.0) < 1e-9              # 平均上り
    assert out["front_back_diff"] > 0                     # back-front=+1
    assert out["n"] == 2
    # baseline 残差
    out2 = lap_aptitude(r, baseline={"late3f": 35.0}, k=0.0)
    assert abs(out2["late3f"] - 0.0) < 1e-9


def test_lap_aptitude_empty():
    out = lap_aptitude(pd.DataFrame({"上り": [], "ペース": []}), k=0.0)
    assert np.isnan(out["late3f"]) and out["n"] == 0
