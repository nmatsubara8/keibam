"""前売り vs 最終 選定ハーネスの純ロジック単体テスト。"""
from __future__ import annotations

import importlib.util
from pathlib import Path

_MOD = Path(__file__).resolve().parents[2] / "scripts" / "roi_prerace_odds_check.py"
_spec = importlib.util.spec_from_file_location("roi_prerace_odds_check", _MOD)
pc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(pc)


def test_build_records_joins_on_race_and_uma():
    # 予測: R1 の馬1,2。OZ: R1 の馬1,2,3（馬3 は予測に無く除外）＋ R2（予測に無く除外）。
    pred = {("R1", 1): (0.5, 4.0, 1.0), ("R1", 2): (0.2, 8.0, 0.0)}
    oz = {"R1": {1: 3.0, 2: 9.0, 3: 50.0}, "R2": {1: 2.0}}
    recs = pc.build_prerace_records(pred, oz)
    assert len(recs) == 2
    r1 = next(r for r in recs if r["uma"] == 1)
    assert r1["o_final"] == 4.0 and r1["o_pre"] == 3.0 and r1["won"] == 1.0


def test_ev_settle_selection_differs_by_odds_source():
    # 馬1: p=.3, final=4(EV_f=1.2>1), pre=3(EV_p=0.9<1) → final では選ぶ/前売りでは選ばない
    recs = [
        {"rid": "R1", "uma": 1, "p": 0.3, "o_final": 4.0, "o_pre": 3.0, "won": 1.0},
        {"rid": "R1", "uma": 2, "p": 0.1, "o_final": 2.0, "o_pre": 2.0, "won": 0.0},
    ]
    rf = pc.ev_settle(recs, "o_final", 1.0)
    rp = pc.ev_settle(recs, "o_pre", 1.0)
    assert rf["n_bets"] == 1 and abs(rf["return_rate"] - 4.0) < 1e-9   # 精算は最終odds=4
    assert rp["n_bets"] == 0                                            # 前売りだと EV<1 で見送り


def test_ev_settle_settles_at_final_not_selection_odds():
    # 前売りで選んでも払戻は最終オッズで精算されることを確認。
    recs = [{"rid": "R1", "uma": 1, "p": 0.5, "o_final": 10.0, "o_pre": 3.0, "won": 1.0}]
    rp = pc.ev_settle(recs, "o_pre", 1.0)     # EV_pre=1.5>1 で選ぶ
    assert rp["n_bets"] == 1
    assert abs(rp["return_rate"] - 10.0) < 1e-9    # 最終10で精算（前売り3ではない）
