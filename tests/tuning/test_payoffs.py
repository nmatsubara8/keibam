"""払戻ルックアップと複勝決済ロジックの単体テスト。"""
from __future__ import annotations

import pandas as pd

from src.tuning._payoffs import single_horse_payoff_lookup


def _payoffs():
    return pd.DataFrame({
        "race_id": ["R1", "R1", "R1", "R2"],
        "bet_type": ["fukusho", "fukusho", "tansho", "fukusho"],
        "combo_key": ["3", "5", "3", "7"],
        "payoff_yen": [130.0, 240.0, 310.0, 180.0],
        "popularity": [1, 4, 1, 2],
    })


def test_fukusho_lookup_only_fukusho_rows():
    lk = single_horse_payoff_lookup(_payoffs(), "fukusho")
    assert lk == {("R1", 3): 130.0, ("R1", 5): 240.0, ("R2", 7): 180.0}
    # tansho 行は混ざらない
    assert ("R1", 3) in lk and lk[("R1", 3)] == 130.0  # fukusho の 130（tansho 310 でない）


def test_tansho_lookup():
    lk = single_horse_payoff_lookup(_payoffs(), "tansho")
    assert lk == {("R1", 3): 310.0}


def test_empty_payoffs():
    assert single_horse_payoff_lookup(pd.DataFrame(), "fukusho") == {}


def test_settle_fukusho_mode():
    from manji_walk_forward import _settle
    # 選択馬: R1 馬3(複勝130), R1 馬5(複勝240), R1 馬9(圏外→払戻なし)
    chosen = pd.DataFrame({"race_id": ["R1", "R1", "R1"], "umaban": [3, 5, 9],
                           "odds": [4.0, 8.0, 20.0]})
    lk = {("R1", 3): 130.0, ("R1", 5): 240.0}
    n, hit, stake, ret = _settle(chosen, {}, payoffs=lk)
    assert n == 3 and stake == 300.0
    assert hit == 2                      # 3 と 5 が複勝圏内、9 は圏外
    assert ret == 130.0 + 240.0          # 払戻円の合計（単勝オッズは使わない）


def test_settle_tansho_unchanged():
    from manji_walk_forward import _settle
    chosen = pd.DataFrame({"race_id": ["R1", "R1"], "umaban": [3, 9], "odds": [4.0, 20.0]})
    winners = {"R1": {3}}
    n, hit, stake, ret = _settle(chosen, winners)     # payoffs=None → 単勝
    assert n == 2 and hit == 1 and stake == 200.0
    assert ret == 100.0 * 4.0            # 勝馬3のみ、100×単勝オッズ
