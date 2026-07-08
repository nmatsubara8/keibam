"""一撃分散設計（bold play）モジュールの単体テスト。

損失最小化の双対: EV<0 を所与とし、目標到達確率 (1−t)/m と「脚を増やすほど不利」を固定する。
"""
from __future__ import annotations

import math

from src.policies._bold_play import (
    BoldPlayDesign,
    bet_to_target_stake,
    expected_pnl_rate,
    fair_win_prob,
    is_subfair,
    optimal_legs,
    parlay_depth_for_target,
    parlay_reach_prob,
    reach_prob_at_odds,
    single_shot_reach_prob,
    target_multiple,
)


def test_subfair_and_pnl():
    # market fair-but-for-takeout: p=(1-t)/o → p·o=1-t<1 ⇒ subfair, EV=-t
    o = 5.0
    p = fair_win_prob(o, 0.2)
    assert math.isclose(p, 0.16)
    assert is_subfair(p, o)
    assert math.isclose(expected_pnl_rate(p, o), -0.2)


def test_single_shot_reach_prob():
    assert math.isclose(single_shot_reach_prob(5, 0.2), 0.16)   # (1-0.2)/5
    assert math.isclose(single_shot_reach_prob(100, 0.2), 0.008)
    # 目標が高いほど到達確率は反比例で低下
    assert single_shot_reach_prob(1000, 0.2) < single_shot_reach_prob(100, 0.2)


def test_parlay_decreases_with_legs():
    m = 100
    p1 = single_shot_reach_prob(m, 0.2)
    p2 = parlay_reach_prob(m, 2, 0.2)
    p4 = parlay_reach_prob(m, 4, 0.2)
    assert p1 > p2 > p4                       # 脚を増やすほど不利（控除の累乗）
    assert math.isclose(p2, 0.8**2 / 100)
    assert optimal_legs() == 1               # 最適は常に1脚


def test_reach_prob_at_odds_needs_odds_ge_multiple():
    # 目標10倍。オッズ8倍では1発で届かない→0。ちょうど10倍なら (1-t)/10。
    assert reach_prob_at_odds(10000, 100000, 8.0, 0.2) == 0.0
    assert math.isclose(reach_prob_at_odds(10000, 100000, 10.0, 0.2), 0.08)
    # 必要以上に長いオッズを狙うと当たり確率が下がる（過剰狙いは損）
    assert reach_prob_at_odds(10000, 100000, 50.0, 0.2) < reach_prob_at_odds(10000, 100000, 10.0, 0.2)


def test_parlay_depth_for_target():
    # 1000倍を 7.5倍オッズで: ceil(log1000/log7.5)=ceil(3.43)=4
    assert parlay_depth_for_target(1000, 7.5) == 4
    assert parlay_depth_for_target(1, 5) == 0


def test_bet_to_target_stake():
    # B=10000, T=50000, o=6 → need=(50000-10000)/5=8000（<B, unit100丸め）
    assert math.isclose(bet_to_target_stake(10000, 50000, 6.0, 100), 8000.0)
    # 1発で届かない場合は all-in（=B）
    assert bet_to_target_stake(10000, 1_000_000, 6.0, 100) == 10000.0
    assert bet_to_target_stake(10000, 10000, 6.0) == 0.0  # 既に到達


def test_design_report_expected_pnl_is_minus_takeout():
    d = BoldPlayDesign(bankroll=10000, target=1_000_000, takeout=0.2)
    r = d.report()
    assert math.isclose(r["multiple"], 100.0)
    assert math.isclose(r["required_odds_single_shot"], 100.0)
    assert math.isclose(r["single_shot_reach_prob"], 0.008)
    assert r["parlay4_reach_prob"] < r["single_shot_reach_prob"]  # 脚増=不利
    assert math.isclose(r["expected_pnl_rate"], -0.2)             # 設計に依らず −控除率


def test_target_multiple():
    assert math.isclose(target_multiple(10000, 50000), 5.0)
