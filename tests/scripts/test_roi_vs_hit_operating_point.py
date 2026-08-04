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


def test_odds_band_boundaries():
    assert op._odds_band(2.9) == "本命<3" and op._odds_band(3.0) == "対抗3-7"
    assert op._odds_band(6.9) == "対抗3-7" and op._odds_band(7.0) == "中穴7-20"
    assert op._odds_band(19.9) == "中穴7-20" and op._odds_band(20.0) == "大穴≥20"


def test_ev_band_breakdown_splits_by_odds():
    prob = np.array([0.6, 0.2, 0.1])
    odds = np.array([2.0, 5.0, 30.0])       # EV 1.2, 1.0, 3.0 → EV>1: 馬1,馬3
    wins = np.array([1.0, 0.0, 0.0])
    rows = op.ev_band_breakdown(prob, odds, wins, ev_thr=1.0)
    bands = {r["帯"]: r for r in rows}
    assert set(bands) == {"本命<3", "大穴≥20"}   # EV>1 の2頭のみ、別帯
    assert bands["本命<3"]["n_bets"] == 1


def test_shuffle_within_race_is_permutation_per_race():
    vals = np.array([0.1, 0.2, 0.3, 0.9, 0.8])
    rids = np.array(["A", "A", "A", "B", "B"])
    out = op.shuffle_within_race(vals, rids, seed=3)
    # レース内は同じ多重集合（並べ替えのみ）／レース跨ぎで値は混ざらない
    assert sorted(out[:3]) == [0.1, 0.2, 0.3]
    assert sorted(out[3:]) == [0.8, 0.9]


def test_ev_operating_point_no_bet_when_null():
    # 全馬 EV<=1（＝市場に何も足せない）→ 見送り＝0点
    prob = np.array([0.3, 0.2])
    odds = np.array([2.0, 2.0])                      # EV 0.6, 0.4
    wins = np.array([1.0, 0.0])
    rids = np.array(["R1", "R1"])
    r = op.ev_operating_point(prob, odds, wins, rids, ev_thr=1.0)
    assert r["n_bets"] == 0
    assert np.isnan(r["return_rate"])
