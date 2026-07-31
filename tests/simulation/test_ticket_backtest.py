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


def test_runner_end_to_end_and_bootstrap():
    src = _hjc_return_processor()
    probs = aggregate_ticket_probabilities(np.array([[0, 1, 2]] * 10), umaban=[1, 2, 3, 4])
    rank = [1, 2, 3, 4]
    cands = build_candidates("202601010101", rank, probs,
                             lambda r: [(BetType.SANRENTAN, (1, 2, 3)), (BetType.SANRENTAN, (1, 3, 2))])
    per = settle_per_race(cands, src)
    d = per["202601010101"]
    assert d["n_bets"] == 2 and d["returned"] == 41.0                 # 2点買い・1点的中
    ci = race_bootstrap_ci(per, n_boot=200, seed=1)
    assert ci["n_races"] == 1 and ci["roi"] == d["returned"] / d["stake"]
    assert roi_by_year(per) == {"2026": d["returned"] / d["stake"]}
