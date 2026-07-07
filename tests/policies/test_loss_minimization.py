"""損失最小化ポリシーの単体テスト。

市場効率は所与とし、「損失を確実なコストとして最小化する」3レバー
（券種選択・回転量予算・賭けないゲート）の挙動を固定する。
"""

from __future__ import annotations

import math

from src.constants._bet_types import BetType
from src.constants._takeout import payout_rate, rank_by_takeout, takeout
from src.policies._bet_candidate import BetCandidate
from src.policies._loss_minimization import (
    GateResult,
    LossMinimizationConfig,
    LossMinimizingPolicy,
    cheapest_bet_types,
    evaluate_candidate,
    expected_loss,
    expected_pnl_rate,
    filter_candidates,
    market_loss_rate,
    prefer_lowest_takeout_per_race,
    required_ev,
    turnover_cap_for_loss_budget,
)


def _cand(bet_type=BetType.TANSHO, ev=1.2, prob=0.3, odds=None, race_id="r1", combo=(1,)):
    odds = odds if odds is not None else ev / prob
    return BetCandidate(
        race_id=race_id,
        bet_type=bet_type,
        combo=combo,
        probability=prob,
        odds=odds,
        expected_value=ev,
    )


# --- 控除率テーブル（レバー1の土台）-----------------------------------------


def test_takeout_table_values():
    assert takeout(BetType.TANSHO) == 0.200
    assert takeout(BetType.FUKUSHO) == 0.200
    assert takeout(BetType.UMAREN) == 0.225
    assert takeout(BetType.SANRENPUKU) == 0.250
    assert takeout(BetType.SANRENTAN) == 0.275


def test_payout_rate_is_complement():
    assert math.isclose(payout_rate(BetType.TANSHO), 0.800)
    assert math.isclose(payout_rate(BetType.SANRENTAN), 0.725)


def test_unknown_bet_type_defaults_to_min():
    assert takeout("unknown") == 0.200


def test_rank_by_takeout_orders_cheapest_first():
    ranked = rank_by_takeout()
    assert ranked[0][1] == 0.200  # 単勝/複勝
    assert ranked[-1][0] == BetType.SANRENTAN
    assert ranked[-1][1] == 0.275
    # 単調非減少
    assert all(ranked[i][1] <= ranked[i + 1][1] for i in range(len(ranked) - 1))


# --- 期待損失と回転量予算（レバー2）-----------------------------------------


def test_expected_pnl_rate():
    assert math.isclose(expected_pnl_rate(1.2), 0.2)
    assert math.isclose(expected_pnl_rate(0.83), -0.17)


def test_market_loss_rate_equals_takeout():
    assert market_loss_rate(BetType.TANSHO) == 0.200
    assert market_loss_rate(BetType.SANRENTAN) == 0.275


def test_expected_loss_scales_with_takeout():
    # 同じ回転量でも三連単は単勝より損失が大きい
    assert expected_loss(10000, BetType.TANSHO) == 2000.0
    assert expected_loss(10000, BetType.SANRENTAN) == 2750.0


def test_turnover_cap_for_loss_budget():
    # 単勝(0.2)で損失予算¥2,000 → 回転上限¥10,000
    assert math.isclose(turnover_cap_for_loss_budget(2000, BetType.TANSHO), 10000.0)
    # 三連単(0.275)は同予算で回転が小さくなる（高コストゆえ抑制）
    assert turnover_cap_for_loss_budget(2000, BetType.SANRENTAN) < 8000.0


# --- 賭けないゲート（既定 DENY）---------------------------------------------


def test_gate_denies_when_threshold_not_oos():
    cfg = LossMinimizationConfig()  # require_oos_threshold=True
    res = evaluate_candidate(_cand(ev=2.0), cfg, threshold_is_oos=False)
    assert isinstance(res, GateResult)
    assert res.allowed is False
    assert "OOS" in res.reason


def test_gate_denies_disallowed_bet_type():
    cfg = LossMinimizationConfig()  # 既定は単勝/複勝のみ
    res = evaluate_candidate(_cand(bet_type=BetType.SANRENTAN, ev=2.0), cfg, threshold_is_oos=True)
    assert res.allowed is False
    assert "許可券種外" in res.reason


def test_gate_denies_high_takeout_even_if_allowed_listed():
    # 券種は許可リストに入れても、max_takeout で弾く二重ガード
    cfg = LossMinimizationConfig(
        allowed_bet_types=(BetType.TANSHO, BetType.SANRENTAN), max_takeout=0.20
    )
    res = evaluate_candidate(_cand(bet_type=BetType.SANRENTAN, ev=2.0), cfg, threshold_is_oos=True)
    assert res.allowed is False
    assert "控除率" in res.reason


def test_gate_denies_low_ev():
    cfg = LossMinimizationConfig()
    res = evaluate_candidate(_cand(ev=0.95), cfg, threshold_is_oos=True)
    assert res.allowed is False
    assert "EV" in res.reason


def test_gate_denies_low_probability_tail():
    cfg = LossMinimizationConfig(min_probability=0.05)
    res = evaluate_candidate(_cand(ev=1.5, prob=0.01), cfg, threshold_is_oos=True)
    assert res.allowed is False
    assert "テール除外" in res.reason


def test_gate_allows_valid_tansho():
    cfg = LossMinimizationConfig(ev_safety_margin=0.1)
    # 必要 EV = 1.1、候補は 1.2 で通過
    res = evaluate_candidate(_cand(bet_type=BetType.TANSHO, ev=1.2, prob=0.3), cfg, threshold_is_oos=True)
    assert res.allowed is True


def test_required_ev_includes_margin():
    cfg = LossMinimizationConfig(ev_bar=1.0, ev_safety_margin=0.15)
    assert math.isclose(required_ev(cfg), 1.15)


# --- 低控除優先とポリシー統合 ------------------------------------------------


def test_prefer_lowest_takeout_picks_tansho_over_sanrentan():
    cands = [
        _cand(bet_type=BetType.SANRENTAN, ev=3.0, race_id="rA", combo=(1, 2, 3)),
        _cand(bet_type=BetType.TANSHO, ev=1.2, race_id="rA", combo=(1,)),
    ]
    kept = prefer_lowest_takeout_per_race(cands)
    assert len(kept) == 1
    assert kept[0].bet_type == BetType.TANSHO


def test_filter_candidates_records_all_decisions():
    cfg = LossMinimizationConfig()
    cands = [_cand(ev=1.2), _cand(ev=0.9), _cand(bet_type=BetType.UMAREN, ev=1.5)]
    allowed, records = filter_candidates(cands, cfg, threshold_is_oos=True)
    assert len(records) == 3
    assert len(allowed) == 1  # 只の単勝EV1.2のみ通過
    assert allowed[0].expected_value == 1.2


def test_policy_default_bets_nothing_without_oos():
    policy = LossMinimizingPolicy()
    cands = [_cand(ev=5.0), _cand(bet_type=BetType.FUKUSHO, ev=3.0)]
    # OOS でない閾値 → 既定で一切賭けない
    assert policy.select(cands, threshold_is_oos=False) == []


def test_policy_selects_lowest_takeout_when_valid():
    policy = LossMinimizingPolicy(LossMinimizationConfig(allowed_bet_types=(BetType.TANSHO, BetType.FUKUSHO)))
    cands = [
        _cand(bet_type=BetType.FUKUSHO, ev=1.3, race_id="rZ", combo=(4,)),
        _cand(bet_type=BetType.TANSHO, ev=1.1, race_id="rZ", combo=(4,)),
    ]
    kept = policy.select(cands, threshold_is_oos=True)
    # 単勝・複勝は同控除(0.2) → 両方残り、EV降順で複勝が先頭
    assert {c.bet_type for c in kept} == {BetType.TANSHO, BetType.FUKUSHO}
    assert kept[0].bet_type == BetType.FUKUSHO


def test_loss_budget_report_orders_cheapest_and_caps():
    policy = LossMinimizingPolicy(
        LossMinimizationConfig(allowed_bet_types=(BetType.TANSHO, BetType.UMAREN))
    )
    rows = policy.loss_budget_report(2000)
    # 単勝(0.2)が先頭、回転上限が大きい
    assert rows[0]["bet_type"] == BetType.TANSHO
    assert math.isclose(rows[0]["turnover_cap"], 10000.0)
    # 馬連(0.225)は上限が小さい
    assert rows[1]["turnover_cap"] < rows[0]["turnover_cap"]


def test_cheapest_bet_types_subset():
    ranked = cheapest_bet_types((BetType.SANRENTAN, BetType.TANSHO))
    assert ranked[0][0] == BetType.TANSHO
    assert ranked[1][0] == BetType.SANRENTAN
