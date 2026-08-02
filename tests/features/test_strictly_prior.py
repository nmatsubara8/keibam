"""H3 strictly-prior コアの単体テスト（リーク安全が最重要）。"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.features._strictly_prior import (
    PACE_FAST,
    PACE_NORMAL,
    PACE_SLOW,
    PACE_STATES,
    clip_3f,
    fit_pace_calibration,
    h3a_pace_aptitude,
    h3b_lap_aptitude,
    has_leak,
    lap_aptitude,
    market_anchored_perf,
    pace_aptitude,
    pace_state_from_balance,
    pace_state_residuals,
    pace_states_of_runs,
    pr_z_from_forecast,
    sed_market_perf,
    sed_pace_state,
    sed_race_percentile_ato3f,
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


# ============================================================================================
# FROZEN H3 SPEC（SED 主ソース）テスト

def test_clip_3f_physical_range():
    out = clip_3f([280.0, 369.0, 450.0, 0.0, -89.0, 984.0, 279.0, 451.0, None])
    # 有効: 280,369,450 / 無効(NaN): 0,-89,984,279,451,None
    assert out[0] == 280.0 and out[1] == 369.0 and out[2] == 450.0
    assert all(np.isnan(out[i]) for i in (3, 4, 5, 6, 7, 8))


def test_sed_pace_state_mapping():
    z = sed_pace_state(pd.Series(["H", "m", " S ", "", "X"]))
    assert list(z[:3]) == [PACE_FAST, PACE_NORMAL, PACE_SLOW]
    assert z.iloc[3] is None or z.isna().iloc[3]
    assert z.iloc[4] is None or z.isna().iloc[4]


def test_sed_market_perf_formula_and_ijo():
    sed = pd.DataFrame({
        "race_id": ["R"] * 4,
        "kakutei_ninki": [4, 1, 2, 3],
        "chakujun": [1, 4, 2, 3],
        "ijo_kubun": ["0", "0", "3", "0"],   # 3頭目は中止→除外
    })
    perf = sed_market_perf(sed)
    # N=4, r=(人気−着順)/(N−1)=/3。row0: (4−1)/3=1.0（大穴激走）row1:(1−4)/3=-1.0
    assert abs(perf.iloc[0] - 1.0) < 1e-12
    assert abs(perf.iloc[1] - (-1.0)) < 1e-12
    assert np.isnan(perf.iloc[2])            # ijo≠0 除外


def test_sed_market_perf_range_bounds():
    sed = pd.DataFrame({"race_id": ["R"] * 3, "kakutei_ninki": [1, 2, 3],
                        "chakujun": [1, 2, 3], "ijo_kubun": ["0"] * 3})
    perf = sed_market_perf(sed)
    assert (perf.abs() <= 1.0 + 1e-9).all()   # r∈[−1,1]


def test_sed_race_percentile_ato3f():
    # 同一 race で上りが速い(小)ほど s>0
    sed = pd.DataFrame({"race_id": ["R"] * 3, "ato3f_time": [340.0, 360.0, 380.0]})
    s = sed_race_percentile_ato3f(sed)
    assert s.iloc[0] > 0 > s.iloc[2]          # 最速→正, 最遅→負
    # 物理域外は欠測→percentile も NaN
    sed2 = pd.DataFrame({"race_id": ["R", "R"], "ato3f_time": [340.0, 984.0]})
    s2 = sed_race_percentile_ato3f(sed2)
    assert np.isnan(s2.iloc[1])


def test_fit_pace_calibration_rows_normalized_and_smoothed():
    # forecast H の実測が全部 H でも Dirichlet で M/S にわずかな質量が残る
    fy = ["H", "H", "H", "S", "S"]
    az = ["H", "H", "H", "S", "M"]
    cal = fit_pace_calibration(fy, az, alpha=1.0)
    for f in PACE_STATES:
        assert abs(sum(cal[f].values()) - 1.0) < 1e-12   # 各行正規化
    assert cal[PACE_FAST][PACE_FAST] > cal[PACE_FAST][PACE_SLOW]  # H予想はH実測が最大
    assert cal[PACE_FAST][PACE_SLOW] > 0                 # α=1 でゼロにならない


def test_pr_z_from_forecast_missing():
    cal = fit_pace_calibration(["H"], ["H"])
    assert pr_z_from_forecast("H", cal) is not None
    assert pr_z_from_forecast("", cal) is None           # 空→None
    assert pr_z_from_forecast("H", None) is None


def test_h3a_pace_aptitude_calibrated_and_defaults():
    # 履歴: fast で市場超過(+), slow で凡走(−)
    hist = pd.DataFrame({
        "_z": [PACE_FAST, PACE_FAST, PACE_SLOW],
        "_perf": [0.6, 0.4, -0.5],
    })
    cal_hi = {PACE_FAST: {PACE_FAST: 1.0, PACE_NORMAL: 0.0, PACE_SLOW: 0.0},
              PACE_NORMAL: {PACE_FAST: 0, PACE_NORMAL: 1, PACE_SLOW: 0},
              PACE_SLOW: {PACE_FAST: 0, PACE_NORMAL: 0, PACE_SLOW: 1}}
    x = h3a_pace_aptitude(hist, "H", cal_hi, k=0.0)     # k=0→縮約なし
    assert abs(x - 0.5) < 1e-9                            # fast の平均(0.5)を重み1で
    x_lo = h3a_pace_aptitude(hist, "S", cal_hi, k=0.0)
    assert abs(x_lo - (-0.5)) < 1e-9
    # 予想欠測→0, 履歴なし→0
    assert h3a_pace_aptitude(hist, "", cal_hi) == 0.0
    assert h3a_pace_aptitude(pd.DataFrame({"_z": [], "_perf": []}), "H", cal_hi) == 0.0


def test_h3b_lap_aptitude_shrink_and_default():
    hist = pd.DataFrame({"_agari_pct": [0.4, 0.2]})
    assert abs(h3b_lap_aptitude(hist, k=0.0) - 0.3) < 1e-9   # 平均
    assert abs(h3b_lap_aptitude(hist, k=2.0) - 0.3 * (2 / 4)) < 1e-9  # n=2,k=2→半分
    assert h3b_lap_aptitude(pd.DataFrame({"_agari_pct": []})) == 0.0
