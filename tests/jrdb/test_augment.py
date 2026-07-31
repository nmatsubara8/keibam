"""JRDB 特徴量付与（attach）の単体テスト。結合・乖離・前走トラブルのリーク無し参照を検証。"""
from __future__ import annotations

import pandas as pd

from src.jrdb._augment import attach


def _featured():
    # 2レース。R1(2020-02-01) の馬1, R2(2020-03-01) の馬1。index=race_id。
    return pd.DataFrame(
        {"馬番": [1, 2, 1], "単勝": [5.0, 8.0, 4.0],
         "date": ["2020-02-01", "2020-02-01", "2020-03-01"]},
        index=["202005010101", "202005010101", "202005010201"],
    )


def _kyi():
    return pd.DataFrame({
        "race_id": ["202005010101", "202005010101", "202005010201"],
        "umaban": [1, 2, 1],
        "ketto": ["20170001", "20170002", "20170001"],  # R1馬1 と R2馬1 は同一馬
        "jrdb_idm": [45.0, 30.0, 46.0],
        "jrdb_kijun_odds": [4.0, 9.0, 3.5],
        "jrdb_kijun_ninki": [1, 3, 1],
    })


def _history():
    # ketto 20170001 は 2020-02-01 に不利(trouble)を受けた（→ R2 の前走トラブル）
    return pd.DataFrame({
        "ketto": ["20170001"],
        "hist_date": pd.to_datetime(["2020-02-01"]),
        "prev_deokure": [0],
        "prev_trouble": [1],
    })


def test_attach_kijun_gap():
    out = attach(_featured(), _kyi(), _history())
    # R1馬1: 基準4.0 / 市場5.0 = 0.8
    assert out.loc["202005010101"].iloc[0]["jrdb_kijun_odds"] == 4.0
    assert abs(out.loc["202005010101"].iloc[0]["jrdb_kijun_gap"] - 0.8) < 1e-9
    assert out.loc["202005010101"].iloc[0]["jrdb_idm"] == 45.0


def test_prev_trouble_leak_safe():
    out = attach(_featured(), _kyi(), _history())
    # R2馬1(2020-03-01): 前走(2020-02-01)で trouble → prev_trouble=1
    r2 = out.loc["202005010201"]
    assert r2["prev_trouble"] == 1
    # R1馬1(2020-02-01): その日のtroubleは「今走」なので参照しない（exact不可）→ 前走なし=NaN
    r1 = out.loc["202005010101"].iloc[0]
    assert pd.isna(r1["prev_trouble"])


def test_attach_empty_jrdb_is_safe():
    out = attach(_featured(), pd.DataFrame(), pd.DataFrame())
    assert "jrdb_kijun_odds" in out.columns
    assert out["jrdb_kijun_odds"].isna().all()


def test_attach_is_idempotent_no_xy_dupes():
    # 既に attach 済みの featured へ再適用しても jrdb_*_x/_y 重複列を作らない（冪等・二重マージ防止）。
    once = attach(_featured(), _kyi(), pd.DataFrame())
    assert "jrdb_idm" in once.columns
    twice = attach(once, _kyi(), pd.DataFrame())
    assert not any(str(c).endswith(("_x", "_y")) for c in twice.columns)  # 重複列なし
    assert "jrdb_idm" in twice.columns and "jrdb_idm_x" not in twice.columns
    assert set(once.columns) == set(twice.columns)                        # 列集合は不変
    # 値も一度目と一致（再付与で壊れない）
    assert twice["jrdb_idm"].tolist() == once["jrdb_idm"].tolist()
