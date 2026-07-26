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


def test_place_payoff_lookup_from_returns():
    """return_tables（列0=券種/1=当選馬番/2=払戻・br区切り）から複勝lookupを作る。"""
    import pandas as pd

    from src.tuning._payoffs import place_payoff_lookup_from_returns
    # index=race_id、列 0/1/2。複勝は3頭 br 区切り。旧空白区切りも1行入れて両対応を確認。
    df = pd.DataFrame(
        {0: ["単勝", "複勝", "複勝"],
         1: ["3", "3br5br7", "2 6"],
         2: ["230", "150br170br110", "150 320"]},
        index=["202444060101", "202444060101", "202444060102"],
    )
    lk = place_payoff_lookup_from_returns(df)
    assert lk[("202444060101", 3)] == 150.0
    assert lk[("202444060101", 5)] == 170.0
    assert lk[("202444060101", 7)] == 110.0
    assert lk[("202444060102", 2)] == 150.0   # 空白区切りも解釈
    assert lk[("202444060102", 6)] == 320.0
    assert ("202444060101", 99) not in lk


def test_place_payoff_lookup_empty_and_race_id_column():
    import pandas as pd

    from src.tuning._payoffs import place_payoff_lookup_from_returns
    assert place_payoff_lookup_from_returns(pd.DataFrame()) == {}
    # race_id が列のケース
    df = pd.DataFrame({0: ["複勝"], 1: ["4"], 2: ["180"], "race_id": ["202450010101"]})
    lk = place_payoff_lookup_from_returns(df)
    assert lk == {("202450010101", 4): 180.0}


def test_merged_fukusho_lookup_combines(tmp_path):
    import pandas as pd

    from src.tuning._payoffs import merged_fukusho_lookup
    # payoffs.pkl（縦持ち）＝中央archive相当、return_tables.pkl＝NAR相当
    payoffs = pd.DataFrame({"race_id": ["202401010101"], "bet_type": ["fukusho"],
                            "combo_key": ["2"], "payoff_yen": [140.0], "popularity": [1]})
    pp = tmp_path / "payoffs.pkl"
    payoffs.to_pickle(pp)
    rt = pd.DataFrame({0: ["複勝"], 1: ["3"], 2: ["150"]}, index=["202444060101"])
    rtp = tmp_path / "return_tables.pkl"
    rt.to_pickle(rtp)
    lk = merged_fukusho_lookup(str(pp), str(rtp))
    assert lk[("202401010101", 2)] == 140.0   # 中央archive
    assert lk[("202444060101", 3)] == 150.0   # NAR(return_tables)
