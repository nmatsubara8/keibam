"""P(z) 予測器（_pace_state）の純関数テスト＝Mixture-PL の入力を作る前段の検証。

教師ラベルは事後(結果)情報で可・特徴量は発走前のみ、という境界を守れているかと、
ラベル3分割がほぼ均等・特徴集計が正しいことを合成データで確認する。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.constants._pace_states import PACE_STATES
from src.preprocessing._pace_state import (
    build_race_features,
    evaluate_pz,
    label_pace_states,
    parse_pace_string,
    race_pace_balance,
)


def test_parse_pace_string():
    assert parse_pace_string("35.1-36.8") == (35.1, 36.8)
    assert parse_pace_string(" 34.0 - 35.5 ") == (34.0, 35.5)
    assert parse_pace_string("なし") is None
    assert parse_pace_string(None) is None
    assert parse_pace_string(float("nan")) is None


def test_label_pace_states_balanced_thirds():
    # balance 昇順 90件 → rank ベース3分割でほぼ均等（slow/normal/fast 各≈30）
    bal = pd.Series(np.arange(90, dtype=float), index=[f"R{i:03d}" for i in range(90)])
    lab = label_pace_states(bal)
    vc = lab.value_counts()
    assert set(lab.unique()) == set(PACE_STATES)
    assert vc.min() >= 29 and vc.max() <= 31          # ほぼ均等
    # 前傾(balance大)=fast、後傾(小)=slow
    assert lab.iloc[-1] == "fast" and lab.iloc[0] == "slow"


def test_label_pace_states_within_group():
    # 2グループ（芝/ダ）で別々に3分割される（レース条件相対）
    bal = pd.Series(np.arange(60, dtype=float), index=[f"R{i:02d}" for i in range(60)])
    groups = pd.DataFrame({"race_type": ["芝"] * 30 + ["ダ"] * 30}, index=bal.index)
    lab = label_pace_states(bal, groups)
    # 各グループ内で slow/normal/fast が出る
    for gv in ("芝", "ダ"):
        sub = lab[groups["race_type"] == gv]
        assert set(sub.unique()) == set(PACE_STATES)


def test_build_race_features_forward_safe_aggregates():
    # 2レース×3頭。pace_median から先行勢・逃げ頭数・頭数を集計。結果列(着順)は使わない。
    df = pd.DataFrame({
        "pace_median": [0.1, 0.3, 0.9,  0.6, 0.7, 0.8],   # R1: 逃げ1 先行1 差し1 / R2: 全部後方
        "枠番":       [1, 2, 8,        3, 4, 5],
        "course_len": [1600] * 3 + [2000] * 3,
        "race_type":  ["芝"] * 3 + ["ダ"] * 3,
        "ground_state": ["良"] * 6,
        "着順":       [1, 2, 3, 1, 2, 3],                  # 結果列＝使われてはいけない
    }, index=pd.Index(["R1", "R1", "R1", "R2", "R2", "R2"], name="race_id"))
    feat = build_race_features(df)
    assert list(feat.index) == ["R1", "R2"]
    assert feat.loc["R1", "n_horses"] == 3
    assert feat.loc["R1", "nige_count"] == 1              # pace_median<0.2 が1頭
    assert feat.loc["R1", "front_ratio"] == pytest_approx(2 / 3)  # <0.5 が2頭
    assert feat.loc["R2", "nige_count"] == 0              # 全部>=0.2
    assert feat.loc["R2", "front_ratio"] == 0.0
    assert feat.loc["R2", "is_dirt"] == 1.0
    # 着順(結果)は特徴に混ざらない
    assert "着順" not in feat.columns


def test_race_pace_balance_by_horse_id_path():
    # (horse_id, date) 経路でレースのバランス(back-front)を復元
    fk = pd.DataFrame({"race_id": ["R1", "R1"], "date": ["2023-01-05", "2023-01-05"],
                       "horse_id": ["100", "101"]})
    hr = pd.DataFrame({"horse_id": ["100", "101"], "日付": ["2023-01-05", "2023-01-05"],
                       "ペース": ["34.0-36.0", "34.0-36.0"]})     # back-front = +2.0（前傾）
    bal = race_pace_balance(fk, hr)
    assert bal.loc["R1"] == pytest_approx(2.0)


def test_evaluate_pz_beats_uniform_when_informative():
    idx = [f"R{i}" for i in range(30)]
    labels = pd.Series((["slow", "normal", "fast"] * 10), index=idx)
    # 正解クラスに 0.8 を寄せた予測 → 一様(1/3)よりlogloss改善
    rows = []
    for z in labels:
        p = {s: 0.1 for s in PACE_STATES}
        p[z] = 0.8
        rows.append(p)
    pred = pd.DataFrame(rows, index=idx)[list(PACE_STATES)]
    ev = evaluate_pz(pred, labels)
    assert ev["n"] == 30
    assert ev["d_logloss"] < 0                          # 一様より改善
    assert ev["accuracy"] == pytest_approx(1.0)


def pytest_approx(x, tol=1e-9):
    import pytest
    return pytest.approx(x, abs=tol)
