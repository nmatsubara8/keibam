"""raw MySpeed（JRDB-SED 素点履歴）付与の単体テスト（Issue #22）。

検証内容:
  - soten_history_aggregates: trailing 集約の値が正しく、当該走を含む inclusive 定義であること。
  - attach: merge_asof(backward, exact不可) で「今走より前の最新走」の集約が貼られ、
    当該走が除外される（leak-safe）こと。デビュー戦（過去なし）は NaN で埋まること。
  - build_hist（参照実装）との数値等価: asof後の featured 各走の集約が、参照の shift(1) 定義と一致。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from scripts.myspeed_staged_gate import build_hist
from src.constants._feature_cols import MYSPEED_FEATURE_COLS
from src.jrdb._augment import MYSPEED_COLS, attach, soten_history_aggregates


def _soten_hist():
    """馬 A(20170001) の3走・馬 B(20170002) の1走。素点は既知値。"""
    return pd.DataFrame({
        "ketto": ["20170001", "20170001", "20170001", "20170002"],
        "hist_date": pd.to_datetime(["2020-01-01", "2020-02-01", "2020-03-01",
                                     "2020-02-01"]),
        "soten": [50.0, 60.0, 40.0, 70.0],
    })


def test_column_contract_is_canonical():
    # attach が出す列名・列順は constants の契約と一致（学習/推論の列順ドリフト防止）。
    assert MYSPEED_COLS == list(MYSPEED_FEATURE_COLS)
    out = soten_history_aggregates(_soten_hist())
    assert list(out.columns) == ["ketto", "hist_date", *MYSPEED_FEATURE_COLS]


def test_soten_aggregates_values():
    out = soten_history_aggregates(_soten_hist()).set_index(["ketto", "hist_date"])
    a3 = out.loc[("20170001", pd.Timestamp("2020-03-01"))]  # 3走目（素点 50,60,40）
    assert a3["jrdb_ms_last"] == 40.0
    assert a3["jrdb_ms_mean3"] == (50 + 60 + 40) / 3
    assert a3["jrdb_ms_max5"] == 60.0
    assert a3["jrdb_ms_trend"] == 40.0 - (50 + 60) / 2  # 直近 − 前2走平均
    assert a3["jrdb_ms_npast"] == 3
    # ewm(α=0.3) inclusive を pandas 参照値と照合
    ref = pd.Series([50.0, 60.0, 40.0]).ewm(alpha=0.3, min_periods=1).mean().iloc[-1]
    assert abs(a3["jrdb_ms_ewm"] - ref) < 1e-9


def test_soten_aggregates_first_race_has_no_trend():
    out = soten_history_aggregates(_soten_hist()).set_index(["ketto", "hist_date"])
    a1 = out.loc[("20170001", pd.Timestamp("2020-01-01"))]  # 初走 → 前走なし
    assert a1["jrdb_ms_last"] == 50.0
    assert a1["jrdb_ms_npast"] == 1
    assert pd.isna(a1["jrdb_ms_trend"])  # shift(1)+shift(2) が NaN


def _featured():
    # 馬 A の R1(01-15)・R2(02-15)・R3(03-15)。index=race_id。
    return pd.DataFrame(
        {"馬番": [1, 1, 1], "単勝": [5.0, 4.0, 3.0],
         "date": ["2020-01-15", "2020-02-15", "2020-03-15"]},
        index=["202001010101", "202001010201", "202001010301"],
    )


def _kyi():
    return pd.DataFrame({
        "race_id": ["202001010101", "202001010201", "202001010301"],
        "umaban": [1, 1, 1],
        "ketto": ["20170001", "20170001", "20170001"],
        "jrdb_idm": [45.0, 46.0, 47.0],
    })


def test_attach_myspeed_leak_safe():
    # 素点履歴: A は 01-01=50, 02-01=60（featured の各走より前の実走）。
    soten = pd.DataFrame({
        "ketto": ["20170001", "20170001"],
        "hist_date": pd.to_datetime(["2020-01-01", "2020-02-01"]),
        "soten": [50.0, 60.0],
    })
    soten = soten_history_aggregates(soten)
    out = attach(_featured(), _kyi(), pd.DataFrame(), soten=soten)

    # R1(01-15): 直近過去走=01-01 のみ → last=50, npast=1
    r1 = out.loc["202001010101"]
    assert r1["jrdb_ms_last"] == 50.0
    assert r1["jrdb_ms_npast"] == 1
    # R2(02-15): 直近過去走=02-01（01-01,02-01 が過去）→ last=60, mean3=(50+60)/2, npast=2
    r2 = out.loc["202001010201"]
    assert r2["jrdb_ms_last"] == 60.0
    assert r2["jrdb_ms_mean3"] == (50 + 60) / 2
    assert r2["jrdb_ms_npast"] == 2
    # R3(03-15): 過去は 01-01,02-01 のまま（03-15 の実走は soten に無い）→ last=60（leak無し）
    r3 = out.loc["202001010301"]
    assert r3["jrdb_ms_last"] == 60.0


def test_attach_myspeed_debut_is_nan():
    # 素点履歴が featured の全走より後 → どの今走にも過去走なし → NaN。
    soten = soten_history_aggregates(pd.DataFrame({
        "ketto": ["20170001"], "hist_date": pd.to_datetime(["2021-01-01"]),
        "soten": [55.0],
    }))
    out = attach(_featured(), _kyi(), pd.DataFrame(), soten=soten)
    assert out["jrdb_ms_last"].isna().all()


def test_attach_without_soten_is_safe():
    # soten 未指定でも列は生成され、全 NaN（後方互換）。
    out = attach(_featured(), _kyi(), pd.DataFrame())
    for c in MYSPEED_COLS:
        assert c in out.columns
        assert out[c].isna().all()


def test_matches_reference_build_hist():
    """asof 後の featured 各走の集約が、参照 build_hist の shift(1) 定義と数値一致。"""
    # 馬 A の連続4走を featured とし、その素点履歴を同じ4走から作る。
    dates = ["2020-01-01", "2020-02-01", "2020-03-01", "2020-04-01"]
    sotens = [50.0, 60.0, 40.0, 80.0]
    featured = pd.DataFrame(
        {"馬番": [1, 1, 1, 1], "単勝": [5.0, 4.0, 3.0, 2.0], "date": dates},
        index=[f"20200101010{i}" for i in range(1, 5)],
    )
    kyi = pd.DataFrame({
        "race_id": featured.index.tolist(), "umaban": [1] * 4,
        "ketto": ["20170001"] * 4,
    })
    soten = soten_history_aggregates(pd.DataFrame({
        "ketto": ["20170001"] * 4, "hist_date": pd.to_datetime(dates), "soten": sotens,
    }))
    out = attach(featured, kyi, pd.DataFrame(), soten=soten)

    # 参照: build_hist は各走の shift(1) 集約（今走除外）を row 整列で持つ。
    ref = build_hist(
        pd.DataFrame({"horse_id": ["20170001"] * 4,
                      "rid": [1, 2, 3, 4], "soten": sotens}),
        value_col="soten", prefix="raw",
    ).sort_values("rid")

    got = out.reset_index(drop=True)
    # 参照 raw_last と本番 jrdb_ms_last が全走で一致（NaN 位置も含め）。
    assert np.allclose(got["jrdb_ms_last"].to_numpy(dtype=float),
                       ref["raw_last"].to_numpy(dtype=float), equal_nan=True)
    assert np.allclose(got["jrdb_ms_mean3"].to_numpy(dtype=float),
                       ref["raw_mean3"].to_numpy(dtype=float), equal_nan=True)
    assert np.allclose(got["jrdb_ms_max5"].to_numpy(dtype=float),
                       ref["raw_max5"].to_numpy(dtype=float), equal_nan=True)
    assert np.allclose(got["jrdb_ms_trend"].to_numpy(dtype=float),
                       ref["raw_trend"].to_numpy(dtype=float), equal_nan=True)
