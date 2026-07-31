"""JrdbReturnSource（SED 払戻→ReturnProcessor 互換）の単体テスト。BettingTickets 経由も検証。"""
from __future__ import annotations

import pandas as pd

from src.constants._bet_types import BetType
from src.simulation._betting_tickets import BettingTickets
from src.simulation._jrdb_return_source import (
    JrdbReturnSource,
    build_single_table,
    race_payout_row,
)


def test_race_payout_row_pads_slots():
    row = race_payout_row([(3, 250.0), (5, 180.0)], n_slots=3)
    assert row["win_0"] == 3 and row["return_0"] == 250.0
    assert row["win_1"] == 5 and row["return_1"] == 180.0
    assert row["win_2"] == 0 and row["return_2"] == 0.0        # 不足スロットは0埋め


def test_build_single_table_only_positive_payoff():
    df = pd.DataFrame({
        "race_id": ["r1", "r1", "r1", "r2"],
        "umaban": [3, 5, 8, 1],
        "fukusho_payoff": [250.0, 180.0, 0.0, 130.0],          # r1 は 2 頭的中、r2 は 1 頭
    })
    tbl = build_single_table(df, "fukusho_payoff", 3)
    assert set(tbl.index) == {"r1", "r2"}
    assert set(tbl.loc["r1"][["win_0", "win_1"]]) == {3, 5} and tbl.loc["r1"]["win_2"] == 0


def test_end_to_end_via_betting_tickets():
    # SED 合成: r1 の馬3が複勝250円/100円で的中。BettingTickets で 100円ベット→250 払戻。
    sed = pd.DataFrame({
        "race_id": ["202401010101", "202401010101"],
        "umaban": [3, 5],
        "tansho_payoff": [0.0, 0.0],
        "fukusho_payoff": [250.0, 0.0],
    })
    src = JrdbReturnSource(engine=None, sed=sed)
    assert not src.preprocessed_data[BetType.FUKUSHO].empty
    bt = BettingTickets(src)
    n_bets, bet_amount, ret = bt.bet_fukusho("202401010101", [3], 100)
    assert n_bets == 1 and bet_amount == 100 and abs(ret - 250.0) < 1e-9
    # 外れ馬(5)は払戻0
    _, _, ret0 = bt.bet_fukusho("202401010101", [5], 100)
    assert ret0 == 0.0
