"""manji_walk_forward の決済ヘルパ（_winners / _settle）の単体テスト。"""
from __future__ import annotations

import pandas as pd

from manji_walk_forward import ODDS_BUCKETS, _settle, _winners


def _featured():
    return pd.DataFrame(
        {"馬番": [1, 2, 3, 1, 2], "着順": [1, 2, 3, 3, 1]},
        index=["R1", "R1", "R1", "R2", "R2"],
    )


def test_winners_maps_first_place():
    w = _winners(_featured())
    assert w == {"R1": {1}, "R2": {2}}


def test_settle_flat_100_pays_odds_on_win():
    winners = {"R1": {1}, "R2": {2}}
    chosen = pd.DataFrame({
        "race_id": ["R1", "R1", "R2"],
        "umaban": [1, 3, 2],       # R1:1 勝ち, R1:3 負け, R2:2 勝ち
        "odds": [2.5, 10.0, 4.0],
    })
    n, hit, stake, ret = _settle(chosen, winners)
    assert n == 3
    assert hit == 2
    assert stake == 300.0
    # 100*2.5 + 0 + 100*4.0 = 650
    assert ret == 650.0


def test_settle_accumulates_odds_band():
    winners = {"R1": {1}}
    chosen = pd.DataFrame({"race_id": ["R1"], "umaban": [1], "odds": [4.0]})
    band = {b: {"n": 0, "hit": 0, "stake": 0.0, "ret": 0.0} for b in ODDS_BUCKETS}
    _settle(chosen, winners, band)
    # odds 4.0 は 3–7 帯
    b = band[(3.0, 7.0)]
    assert b["n"] == 1 and b["hit"] == 1 and b["stake"] == 100.0 and b["ret"] == 400.0
