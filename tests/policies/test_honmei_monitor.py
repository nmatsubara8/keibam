"""複勝本命 運用点モニタ（④）のテスト。"""
from __future__ import annotations

import pandas as pd

from src.policies._honmei_monitor import detect_drift, monitor_honmei


def _score_table():
    # 3レース×2頭。index=race_id、score と 馬番。
    return pd.DataFrame(
        {"score": [0.55, 0.30, 0.48, 0.20, 0.35, 0.33], "馬番": [1, 2, 3, 4, 5, 6]},
        index=["r1", "r1", "r2", "r2", "r3", "r3"],
    )


def test_monitor_adoption_and_score_distribution():
    m = monitor_honmei(_score_table(), min_score=0.40)
    # 各レース最高 score = r1:0.55, r2:0.48, r3:0.35 → min_score0.40 で r1,r2 採用（r3 見送り）
    assert m["n_races"] == 3 and m["n_bet_races"] == 2
    assert abs(m["adoption_rate"] - 2 / 3) < 1e-9
    assert abs(m["below_min_rate"] - 1 / 3) < 1e-9
    assert abs(m["honmei_score_mean"] - (0.55 + 0.48) / 2) < 1e-9


def test_monitor_realized_returns():
    m = monitor_honmei(_score_table(), min_score=0.40, realized_returns=[250.0, 0.0])
    assert m["hit_rate"] == 0.5                       # 2ベット中1的中
    assert abs(m["return_rate"] - 250.0 / 200.0) < 1e-9   # (250+0)/(100*2)=1.25


def test_monitor_empty_table():
    m = monitor_honmei(pd.DataFrame(), min_score=0.4)
    assert m["n_races"] == 0 and m["adoption_rate"] == 0.0


def test_detect_drift_flags_deviations():
    ref = {"adoption_rate": 0.30, "honmei_score_mean": 0.50, "return_rate": 0.90}
    cur = {"adoption_rate": 0.55, "honmei_score_mean": 0.50, "return_rate": 0.80}
    alerts = detect_drift(ref, cur)                   # 採用率+0.25(>0.10), 回収率-0.10(>0.05)
    assert any("採用率" in a for a in alerts)
    assert any("回収率" in a for a in alerts)
    assert not any("本命score平均" in a for a in alerts)   # 変化なし→非検知


def test_detect_drift_silent_when_within_tolerance():
    ref = {"adoption_rate": 0.30, "honmei_score_mean": 0.50, "return_rate": 0.90}
    cur = {"adoption_rate": 0.33, "honmei_score_mean": 0.49, "return_rate": 0.88}
    assert detect_drift(ref, cur) == []
