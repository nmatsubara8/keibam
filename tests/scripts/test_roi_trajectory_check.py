"""オッズ軌跡 realizable 検証ハーネスの純ロジック単体テスト。"""
from __future__ import annotations

import importlib.util
from pathlib import Path

_MOD = Path(__file__).resolve().parents[2] / "scripts" / "roi_trajectory_check.py"
_spec = importlib.util.spec_from_file_location("roi_trajectory_check", _MOD)
tc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tc)


def test_infer_scale_detects_implied_decimal():
    final = {("R1", 1): 12.3, ("R1", 2): 4.0}
    raw_tenths = {("R1", 1): 123.0, ("R1", 2): 40.0}   # ZZZ9.9 raw ≈ 10×
    raw_actual = {("R1", 1): 12.3, ("R1", 2): 4.0}     # 既に実オッズ
    assert tc.infer_odds_scale(raw_tenths, final) == 0.1
    assert tc.infer_odds_scale(raw_actual, final) == 1.0
    assert tc.infer_odds_scale({}, final) == 1.0


def test_build_traj_records_requires_all_three_sources():
    pred = {("R1", 1): (0.5, 4.0, 1.0), ("R1", 2): (0.2, 8.0, 0.0)}
    oz = {"R1": {1: 3.0, 2: 9.0, 3: 50.0}}     # 馬3 は予測/TYB に無く除外
    tyb = {("R1", 1): 3.8, ("R1", 2): 8.5}
    recs = tc.build_traj_records(pred, oz, tyb)
    assert len(recs) == 2
    r1 = next(r for r in recs if r["uma"] == 1)
    assert r1["o_pre"] == 3.0 and r1["o_tyb"] == 3.8 and r1["o_final"] == 4.0


def test_ev_settle_uses_selection_odds_but_settles_final():
    # 馬1: p=.3, pre=4(EV_pre=1.2>1), tyb=3(EV_tyb=0.9<1)。前売りで選び直前で見送り。精算は最終=5。
    recs = [{"rid": "R1", "uma": 1, "p": 0.3, "o_pre": 4.0, "o_tyb": 3.0,
             "o_final": 5.0, "won": 1.0}]
    rp = tc.ev_settle(recs, "o_pre", 1.0)
    rt = tc.ev_settle(recs, "o_tyb", 1.0)
    assert rp["n_bets"] == 1 and abs(rp["return_rate"] - 5.0) < 1e-9   # 最終5で精算
    assert rt["n_bets"] == 0                                            # 直前だと EV<1
