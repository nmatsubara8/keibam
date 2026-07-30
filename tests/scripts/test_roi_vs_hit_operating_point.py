"""的中率/回収率 運用点ハーネスの純ロジック単体テスト。"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np

_MOD = Path(__file__).resolve().parents[2] / "scripts" / "roi_vs_hit_operating_point.py"
_spec = importlib.util.spec_from_file_location("roi_vs_hit_operating_point", _MOD)
op = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(op)


def test_race_hit_rate_race_level():
    # R1: 当たり1本あり→ヒット。R2: 全外れ→非ヒット。→ 1/2
    rids = np.array(["R1", "R1", "R2"])
    wins = np.array([1.0, 0.0, 0.0])
    assert op._race_hit_rate(rids, wins) == 0.5
    assert np.isnan(op._race_hit_rate(np.array([]), np.array([])))


def test_top_pick_one_bet_per_race():
    # R1 は馬1(p=.6,odds=2,win=1)を本命→的中。R2 は馬2(p=.7,odds=3,win=0)→外れ。
    prob = np.array([0.6, 0.4, 0.3, 0.7])
    odds = np.array([2.0, 5.0, 5.0, 3.0])
    wins = np.array([1.0, 0.0, 0.0, 0.0])
    rids = np.array(["R1", "R1", "R2", "R2"])
    r = op.top_pick_operating_point(prob, odds, wins, rids)
    assert r["n_bets"] == 2                         # レース数＝賭け数
    assert abs(r["hit_rate"] - 0.5) < 1e-9          # 2レース中1的中
    assert abs(r["return_rate"] - (2.0 * 1.0) / 2) < 1e-9  # 払戻2.0 / 2点 = 1.0


def test_ev_operating_point_filters_and_settles():
    prob = np.array([0.6, 0.1])
    odds = np.array([2.0, 2.0])                      # EV: 1.2, 0.2
    wins = np.array([1.0, 0.0])
    rids = np.array(["R1", "R1"])
    r = op.ev_operating_point(prob, odds, wins, rids, ev_thr=1.0)
    assert r["n_bets"] == 1                          # EV>1 は馬1のみ
    assert abs(r["return_rate"] - 2.0) < 1e-9        # 2.0払戻 / 1点
    assert abs(r["hit_rate"] - 1.0) < 1e-9


def test_ev_operating_point_no_bet_when_null():
    # 全馬 EV<=1（＝市場に何も足せない）→ 見送り＝0点
    prob = np.array([0.3, 0.2])
    odds = np.array([2.0, 2.0])                      # EV 0.6, 0.4
    wins = np.array([1.0, 0.0])
    rids = np.array(["R1", "R1"])
    r = op.ev_operating_point(prob, odds, wins, rids, ev_thr=1.0)
    assert r["n_bets"] == 0
    assert np.isnan(r["return_rate"])
