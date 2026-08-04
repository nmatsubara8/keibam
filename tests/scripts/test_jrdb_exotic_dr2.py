"""連系 Test A ハーネスの純ロジック（勝率正規化・馬連 implied・Harville ペア）の単体テスト。"""
from __future__ import annotations

import importlib.util
import math
from pathlib import Path

_MOD = Path(__file__).resolve().parents[2] / "scripts" / "jrdb_exotic_dr2_check.py"
_spec = importlib.util.spec_from_file_location("jrdb_exotic_dr2_check", _MOD)
h = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(h)


def test_combo_to_pair_sorts():
    assert h._combo_to_pair("02-01") == (1, 2)
    assert h._combo_to_pair("07-03") == (3, 7)


def test_win_probs_normalize_removes_takeout():
    # 単勝 2.0/4.0/4.0 → 1/odds = .5/.25/.25 = Σ1.0 → 正規化で不変（この例は控除0相当）
    wp = h.win_probs_from_tansho({1: 2.0, 2: 4.0, 3: 4.0})
    assert abs(sum(wp.values()) - 1.0) < 1e-9
    assert abs(wp[1] - 0.5) < 1e-9


def test_market_pair_probs_sum_to_one():
    um = {(1, 2): 3.0, (1, 3): 6.0, (2, 3): 6.0}
    p = h.market_pair_probs(um)
    assert abs(sum(p.values()) - 1.0) < 1e-9
    assert p[(1, 2)] > p[(1, 3)]        # 低オッズほど高確率


def test_harville_pair_probs_sum_to_one_and_ordering():
    # 3頭 勝率 0.5/0.3/0.2。Harville 馬連は全ペアで正規化され Σ=1、
    # 上位2頭(1,2)のペアが最大。
    wp = {1: 0.5, 2: 0.3, 3: 0.2}
    pp = h.harville_pair_probs(wp)
    assert set(pp) == {(1, 2), (1, 3), (2, 3)}
    assert abs(sum(pp.values()) - 1.0) < 1e-9
    assert pp[(1, 2)] == max(pp.values())


def test_valid_pair_filters_zero_and_invalid():
    assert h._valid_pair("0102") == (1, 2)
    assert h._valid_pair("01-02") == (1, 2)
    assert h._valid_pair("1807") == (7, 18)
    assert h._valid_pair("0000") is None     # 未使用スロットの 0 埋め
    assert h._valid_pair("") is None
    assert h._valid_pair(None) is None
    assert h._valid_pair("0505") is None      # 同一馬（無効）


def test_pair_id_unique_and_blend_race():
    assert h._pair_id(1, 2) == 102 and h._pair_id(3, 12) == 312
    p_harv = {(1, 2): 0.5, (1, 3): 0.3, (2, 3): 0.2}
    p_mkt = {(1, 2): 0.6, (1, 3): 0.25, (2, 3): 0.15}
    br = h.blend_race_from_probs(p_harv, p_mkt, (1, 2))
    assert br is not None
    hf, mf, win = br
    assert win == 102
    assert abs(sum(hf.values()) - 1.0) < 1e-9 and abs(sum(mf.values()) - 1.0) < 1e-9
    # 勝ちペアが市場に無ければ None
    assert h.blend_race_from_probs(p_harv, {(1, 3): 1.0}, (1, 2)) is None


def test_oz_period_from_filename():
    assert h._oz_period("data/x/OZ230105.txt") == "H1"   # 01月
    assert h._oz_period("data/x/OZ230815.txt") == "H2"   # 08月
    assert h._oz_period("OZ231231.txt") == "H2"


def test_logloss_and_argmax():
    pp = {(1, 2): 0.6, (1, 3): 0.3, (2, 3): 0.1}
    assert h._argmax_pair(pp) == (1, 2)
    assert abs(h._logloss(pp, (1, 2)) - (-math.log(0.6))) < 1e-9
    # 存在しないペアは EPS 下限で大きな logloss
    assert h._logloss(pp, (4, 5)) > 10
