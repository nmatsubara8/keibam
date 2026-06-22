"""券種別 EV バックテスト（_backtest）のテスト。

決済が BOX 再展開ではなく「候補の組合せだけ」を評価することと、
回収率/的中率の集計、2ヘッド予測→確定オッズ選定→実払戻の一連を固定する。
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.constants._bet_types import BetType
from src.constants._results_cols import ResultsCols
from src.policies._bet_candidate import BetCandidate
from src.simulation._backtest import BetTypeStats
from src.simulation._backtest import default_thresholds
from src.simulation._backtest import run_backtest
from src.simulation._backtest import settle_candidates

UM = ResultsCols.UMABAN
WAK = ResultsCols.WAKUBAN
TAN = ResultsCols.TANSHO_ODDS


class _FakeReturnProcessor:
    """BettingTickets が触る preprocessed_data 属性だけを持つフェイク。"""

    def __init__(self, **tables):
        keys = ("tansho", "fukusho", "wakuren", "umaren", "umatan", "wide", "sanrenpuku", "sanrentan")
        self.preprocessed_data = {k: tables.get(k, pd.DataFrame()) for k in keys}


def _df(rows, index):
    return pd.DataFrame(rows, index=index)


def _cand(race_id, bet_type, combo, odds=5.0, prob=0.5):
    return BetCandidate(
        race_id=race_id, bet_type=bet_type, combo=tuple(combo),
        probability=prob, odds=odds, expected_value=prob * odds,
    )


class _StubModel:
    """馬番→top3確率を返すスタブ（predict_proba の [:,1] を使う）。"""

    def __init__(self, prob_by_um):
        self._p = prob_by_um

    def predict_proba(self, X):
        p = np.array([self._p[int(u)] for u in X[UM]], dtype=float)
        return np.column_stack([1.0 - p, p])


# ── BetTypeStats ────────────────────────────────────────────


def test_bet_type_stats_props():
    s = BetTypeStats("tansho", n_bets=4, n_hits=1, stake=4.0, returned=5.0)
    assert s.roi == pytest.approx(1.25)
    assert s.hit_rate == pytest.approx(0.25)
    assert s.profit == pytest.approx(1.0)


def test_bet_type_stats_zero_safe():
    s = BetTypeStats("wide")
    assert s.roi == 0.0
    assert s.hit_rate == 0.0


def test_default_thresholds_has_all_types():
    th = default_thresholds()
    for bt in (BetType.TANSHO, BetType.FUKUSHO, BetType.UMAREN, BetType.UMATAN,
               BetType.WIDE, BetType.SANRENPUKU, BetType.SANRENTAN):
        assert bt in th


# ── settle_candidates: 単一点決済（BOX展開しない）────────────


def test_settle_exact_combo_not_box():
    # 馬連(1,3)が的中・払戻800。候補(1,3)的中＋(2,4)不的中 → BOX なら(1,4)(2,3)等も
    # 生成されるが、各候補の組合せだけ決済するので n_bets=2, n_hits=1。
    umaren = _df({"win_0": [(1, 3)], "return_0": [800]}, index=["1"])
    rp = _FakeReturnProcessor(umaren=umaren)
    cands = [_cand("1", BetType.UMAREN, (1, 3)), _cand("1", BetType.UMAREN, (2, 4))]
    stats = settle_candidates(cands, rp, unit=1)
    s = stats[BetType.UMAREN]
    assert s.n_bets == 2
    assert s.n_hits == 1
    assert s.returned == pytest.approx(8.0)  # 800 * 1 / 100
    assert s.stake == 2
    assert s.roi == pytest.approx(4.0)


def test_settle_skips_race_absent_from_return_table():
    # 払戻テーブルに無いレースは評価不能 → 集計から除外（0回収で薄めない）
    tansho = _df({"win_0": [5], "return_0": [350]}, index=["1"])
    rp = _FakeReturnProcessor(tansho=tansho)
    stats = settle_candidates([_cand("999", BetType.TANSHO, (5,))], rp)
    assert stats == {}


def test_settle_groups_by_bet_type():
    tansho = _df({"win_0": [1], "return_0": [500]}, index=["1"])
    fukusho = _df({"win_0": [2], "return_0": [150]}, index=["1"])
    rp = _FakeReturnProcessor(tansho=tansho, fukusho=fukusho)
    cands = [
        _cand("1", BetType.TANSHO, (1,)),   # 的中
        _cand("1", BetType.FUKUSHO, (2,)),  # 的中
        _cand("1", BetType.FUKUSHO, (3,)),  # 不的中
    ]
    stats = settle_candidates(cands, rp)
    assert stats[BetType.TANSHO].n_hits == 1
    assert stats[BetType.FUKUSHO].n_bets == 2
    assert stats[BetType.FUKUSHO].n_hits == 1
    assert stats[BetType.FUKUSHO].returned == pytest.approx(1.5)


# ── run_backtest: 予測→選定→決済の統合 ─────────────────────


def test_run_backtest_integration():
    X = _df(
        {UM: [1, 2, 3], WAK: [1, 2, 3], TAN: [2.0, 5.0, 10.0], "feat": [0.1, 0.2, 0.3]},
        index=["1", "1", "1"],
    )
    X.index.name = "race_id"
    place = _StubModel({1: 0.6, 2: 0.3, 3: 0.1})
    tansho = _df({"win_0": [1], "return_0": [500]}, index=["1"])
    rp = _FakeReturnProcessor(tansho=tansho)
    # 確定オッズ: 馬1=5倍, 馬2=3倍, 馬3=20倍
    lookup = {("1", "tansho", "1"): 5.0, ("1", "tansho", "2"): 3.0, ("1", "tansho", "3"): 20.0}

    res = run_backtest(place, X, rp, final_odds_lookup=lookup, thresholds={BetType.TANSHO: 1.0})

    # 馬1 EV=0.6*5=3.0>1 採用→的中 / 馬3 EV=0.1*20=2.0>1 採用→不的中 / 馬2 EV=0.9<1 不採用
    s = res["per_bet_type"][BetType.TANSHO]
    assert s.n_bets == 2
    assert s.n_hits == 1
    assert s.returned == pytest.approx(5.0)  # 500/100
    assert res["overall"].roi == pytest.approx(2.5)  # 5.0 / 2
    assert res["n_races"] == 1
    assert res["n_candidates"] == 2


def test_run_backtest_empty_when_no_ev():
    X = _df({UM: [1, 2], WAK: [1, 2], TAN: [2.0, 2.0], "feat": [0.1, 0.2]}, index=["1", "1"])
    X.index.name = "race_id"
    place = _StubModel({1: 0.1, 2: 0.1})  # 低確率
    rp = _FakeReturnProcessor(tansho=_df({"win_0": [1], "return_0": [200]}, index=["1"]))
    lookup = {("1", "tansho", "1"): 2.0, ("1", "tansho", "2"): 2.0}  # EV=0.2 < 1
    res = run_backtest(place, X, rp, final_odds_lookup=lookup, thresholds={BetType.TANSHO: 1.0})
    assert res["per_bet_type"] == {}
    assert res["overall"].n_bets == 0
    assert res["n_candidates"] == 0
