"""券種別バックテスト（着順標本→券種確率→買い方→確定払戻決済）のテスト。

核心の差異化: 連系確率は周辺確率の積ではなく **着順標本の同時頻度** から出る。この従属を
固定するテスト（_joint_not_product）が本モジュールの価値の回帰ガード。
"""
from __future__ import annotations

import numpy as np

from src.constants._bet_types import BetType
from src.simulation._ticket_backtest import (
    SANRENTAN,
    UMAREN,
    aggregate_ticket_probabilities,
    build_candidates,
    race_bootstrap_ci,
    roi_by_year,
    settle_per_race,
    sim_rank,
    trifecta_top2_reverse,
)


def _orders():
    # 4頭(index0..3)。馬番は index+1。着順標本(1着,2着,3着 のindex):
    #  50% → (0,1,2)、50% → (1,0,2)。1着2着は 0↔1 で入替、3着は常に2。
    return np.array([[0, 1, 2]] * 5 + [[1, 0, 2]] * 5)


def test_tansho_fukusho_frequencies():
    probs = aggregate_ticket_probabilities(_orders(), umaban=[1, 2, 3, 4])
    assert probs["tansho"][1] == 0.5 and probs["tansho"][2] == 0.5   # 馬番1,2 が半々で1着
    assert probs["fukusho"][3] == 1.0                                 # 馬番3 は常に3着以内
    assert 4 not in probs["fukusho"]                                  # 馬番4 は一度も来ない


def test_joint_not_product_of_marginals():
    # 差異化の核: 馬連(1,2)の同時確率は 1.0（毎回 top2）。周辺積 P(1)·P(2)=0.25 とは違う。
    probs = aggregate_ticket_probabilities(_orders(), umaban=[1, 2, 3, 4])
    assert probs[UMAREN][(1, 2)] == 1.0
    # 三連単は (1,2,3) と (2,1,3) が半々。3着固定の相関が確率に入る。
    assert probs[SANRENTAN][(1, 2, 3)] == 0.5
    assert probs[SANRENTAN][(2, 1, 3)] == 0.5
    assert (1, 3, 2) not in probs[SANRENTAN]                          # 起きない順序は不在


def test_sim_rank_and_generator():
    rank = sim_rank(win_probs=[0.1, 0.4, 0.3, 0.2], umaban=[10, 20, 30, 40])
    assert rank == [20, 30, 40, 10]                                   # 勝率降順の馬番
    tickets = trifecta_top2_reverse(rank, (2, 3))                     # 1↔2位→3,4位
    assert (20, 30, 40) in tickets and (30, 20, 40) in tickets
    assert len(tickets) == 4                                          # 2順列 × 2三着 = 4点


def test_s4_shortfall_is_small_field_not_duplicates():
    from src.simulation._ticket_backtest import s4_point_audit, validate_ranking
    # 6頭以上=8点（full）。正常な順位列は重複なし（validate 通過）。
    full = s4_point_audit([1, 2, 3, 4, 5, 6, 7])
    assert full["actual"] == 8 and full["reason"] == "full"
    validate_ranking([1, 2, 3, 4, 5, 6, 7], "R")           # 重複なし→例外なし
    # 5頭=6点（3着候補が1つ足りない＝小頭数）。重複ではない。
    small = s4_point_audit([1, 2, 3, 4, 5])
    assert small["actual"] == 6 and small["reason"].startswith("small_field")


def test_validate_ranking_detects_duplicates():
    import pytest
    from src.simulation._ticket_backtest import validate_ranking
    with pytest.raises(ValueError, match="重複"):
        validate_ranking([1, 2, 2, 3], "R1")


def test_exacta_and_wide_axis_generators():
    from src.simulation._ticket_backtest import (
        exacta_single_winner, exacta_top2_reverse, wide_axis_flow,
    )
    rank = [7, 3, 5, 9, 2]
    assert exacta_top2_reverse(rank) == [(7, 3), (3, 7)]              # S7 2点・順序あり
    assert exacta_single_winner(rank, (1, 2, 3)) == [(7, 3), (7, 5), (7, 9)]  # S8 3点
    assert wide_axis_flow(rank, (1, 2, 3, 4)) == [(3, 7), (5, 7), (7, 9), (2, 7)]  # S3b 4点・昇順


def test_joint_topk_uses_joint_probability_not_rank():
    from src.simulation._ticket_backtest import SANRENTAN, joint_topk
    probs = {SANRENTAN: {(1, 2, 3): 0.10, (1, 2, 7): 0.25, (2, 1, 3): 0.05}}
    # 同時確率の高い順（周辺順位ではない）: (1,2,7) が最上位
    assert joint_topk(probs, SANRENTAN, 2) == [(1, 2, 7), (1, 2, 3)]


def test_s9_conditional_on_p1():
    from src.simulation._ticket_backtest import STRATEGY_TEMPLATES, TANSHO
    s9 = STRATEGY_TEMPLATES["S9_三連単1着固定_p1≥0.50"]
    rank = [1, 2, 3, 4, 5]
    assert s9(rank, {TANSHO: {1: 0.60}}) != []                       # p1>=0.5→購入(12点)
    assert len(s9(rank, {TANSHO: {1: 0.60}})) == 12
    assert s9(rank, {TANSHO: {1: 0.40}}) == []                       # p1<0.5→見送り


def test_empty_orders():
    probs = aggregate_ticket_probabilities(np.empty((0, 3)), umaban=[1, 2])
    assert probs[SANRENTAN] == {}


def _hjc_return_processor():
    """三連単(1,2,3)が払戻4100円で当選する1レースの HJC 払戻源を作る。"""
    import pandas as pd

    from src.simulation._jrdb_return_source import JrdbHjcReturnSource
    hjc = pd.DataFrame([{
        "race_id": "202601010101",
        "sanrentan_combo1": "010203", "sanrentan_pay1": 4100,
        "umaren_combo1": "0102", "umaren_pay1": 800,
    }])
    return JrdbHjcReturnSource(engine=None, hjc=hjc)


def test_hjc_source_settles_trifecta():
    src = _hjc_return_processor()
    from src.simulation._betting_tickets import BettingTickets
    tickets = BettingTickets(src)
    # 当たり組（1,2,3）: 1点100円が払戻4100円
    n, stake, ret = tickets.settle_one(BetType.SANRENTAN, "202601010101", (1, 2, 3), 1)
    assert n == 1 and ret == 41.0                                     # PAYOUT_UNIT_YEN=100 で 4100/100
    # 外れ組（1,3,2）: 払戻0
    _, _, ret_miss = tickets.settle_one(BetType.SANRENTAN, "202601010101", (1, 3, 2), 1)
    assert ret_miss == 0.0
    assert src.coverage(["202601010101"], BetType.SANRENTAN) == 1.0


def test_portfolio_metrics_total_and_groups():
    from src.simulation._ticket_backtest import portfolio_metrics
    # 3点: 単勝(投資100,払戻200)・三連単(100,4100)・三連単(100,0)。全戦略同時運用のTOTAL。
    rows = [
        ("202601010101", "tansho", 100.0, 200.0),
        ("202601010101", "sanrentan", 100.0, 4100.0),
        ("202601010102", "sanrentan", 100.0, 0.0),
    ]
    m = portfolio_metrics(rows, race_order=["202601010101", "202601010102"], top_k=5)
    assert m["total_stake"] == 300.0 and m["total_return"] == 4300.0
    assert m["roi"] == 4300.0 / 300.0
    assert m["roi_ex_top1"] == (4300.0 - 4100.0) / 300.0     # 最大払戻1件除外
    assert m["n_races"] == 2 and m["n_tickets"] == 3
    assert m["by_group"]["三連単"]["roi"] == 4100.0 / 200.0  # 三連単だけのROI
    assert m["by_group"]["単複"]["roi"] == 2.0
    assert m["by_year"] == {"2026": 4300.0 / 300.0}
    assert m["max_dd"] >= 100.0                               # R2で純-100の落ち込み


def test_market_favorite_uses_purchase_time_odds():
    from src.simulation._ticket_backtest import market_favorite
    fav = market_favorite({"R1": {1: 3.5, 2: 2.1, 3: 9.0}, "R2": {4: 1.8, 5: 5.0}})
    assert fav == {"R1": 2, "R2": 4}                          # 最小オッズ=1番人気


def test_kelly_log_growth_and_paired():
    from src.simulation._ticket_backtest import kelly_log_growth, paired_log_growth_ci
    # a: 的中でオッズ3.0(returned=3), b: 外れ(returned=0)。f=0.05。
    a = {"R1": {"stake": 1.0, "returned": 3.0}, "R2": {"stake": 1.0, "returned": 3.0}}
    b = {"R1": {"stake": 1.0, "returned": 0.0}, "R2": {"stake": 1.0, "returned": 0.0}}
    ga, gb = kelly_log_growth(a), kelly_log_growth(b)
    assert ga > 0 > gb                                   # 的中側は成長・外れ側は縮小
    d = paired_log_growth_ci(a, b, n_boot=200, seed=0)
    assert d["n_races"] == 2 and abs(d["delta"] - (ga - gb)) < 1e-9


def test_paired_delta_roi_ci():
    from src.simulation._ticket_backtest import paired_delta_roi_ci
    sim = {"R1": {"stake": 100.0, "returned": 300.0}, "R2": {"stake": 100.0, "returned": 0.0}}
    mkt = {"R1": {"stake": 100.0, "returned": 150.0}, "R2": {"stake": 100.0, "returned": 100.0}}
    d = paired_delta_roi_ci(sim, mkt, n_boot=300, seed=0)
    assert d["n_races"] == 2
    assert abs(d["roi_sim"] - 1.5) < 1e-9 and abs(d["roi_mkt"] - 1.25) < 1e-9
    assert abs(d["delta"] - 0.25) < 1e-9
    assert d["lo"] <= d["delta"] <= d["hi"]


def test_runner_end_to_end_and_bootstrap():
    src = _hjc_return_processor()
    probs = aggregate_ticket_probabilities(np.array([[0, 1, 2]] * 10), umaban=[1, 2, 3, 4])
    rank = [1, 2, 3, 4]
    cands = build_candidates("202601010101", rank, probs,
                             lambda r, p: [(BetType.SANRENTAN, (1, 2, 3)),
                                           (BetType.SANRENTAN, (1, 3, 2))])
    per = settle_per_race(cands, src)
    d = per["202601010101"]
    assert d["n_bets"] == 2 and d["returned"] == 41.0                 # 2点買い・1点的中
    ci = race_bootstrap_ci(per, n_boot=200, seed=1)
    assert ci["n_races"] == 1 and ci["roi"] == d["returned"] / d["stake"]
    assert roi_by_year(per) == {"2026": d["returned"] / d["stake"]}
