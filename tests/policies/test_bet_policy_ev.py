"""ExpectedValueBetPolicy のテスト。"""

import pandas as pd
import pytest

from src.constants._bet_thresholds import RiskLimits
from src.constants._bet_types import BetType
from src.constants._results_cols import ResultsCols
from src.policies._bet_policy import ExpectedValueBetPolicy
from src.policies._odds_provider import AbstractOddsProvider


class _FixedOddsProvider(AbstractOddsProvider):
    """全馬券に一定オッズを返すスタブ（選定ロジックの検証用）。"""

    def __init__(self, odds: float) -> None:
        self._odds = odds

    def get_odds(self, race_id, bet_type, combo) -> float:
        return self._odds


def _prob_table(race_id, umaban_probs):
    rows = [{ResultsCols.UMABAN: u, "prob": p} for u, p in umaban_probs]
    return pd.DataFrame(rows, index=[race_id] * len(rows))


def test_threshold_filters_low_ev():
    # 単勝: prob=0.5, odds=1.0 -> EV=0.5 < 閾値1.0 なので選ばれない
    table = _prob_table("r1", [(1, 0.5), (2, 0.5)])
    policy = ExpectedValueBetPolicy(_FixedOddsProvider(1.0), thresholds={BetType.TANSHO: 1.0})
    assert policy.select(table) == []


def test_high_ev_selected():
    # odds=4.0, prob(tansho,1)=0.5 -> EV=2.0 > 1.0
    table = _prob_table("r1", [(1, 0.5), (2, 0.5)])
    policy = ExpectedValueBetPolicy(_FixedOddsProvider(4.0), thresholds={BetType.TANSHO: 1.0})
    selected = policy.select(table)
    assert len(selected) == 2
    assert all(c.expected_value > 1.0 for c in selected)
    assert all(c.expected_value == pytest.approx(c.probability * c.odds) for c in selected)


def test_max_tickets_cap():
    # 10頭・馬連、全EVが閾値超 -> 組合せ45通りだが上限でキャップ
    table = _prob_table("r1", [(i, 0.1) for i in range(1, 11)])
    limits = RiskLimits(MAX_TICKETS_PER_RACE=5)
    policy = ExpectedValueBetPolicy(
        _FixedOddsProvider(1000.0), thresholds={BetType.UMAREN: 1.0}, risk_limits=limits
    )
    selected = policy.select(table)
    assert len(selected) == 5


def test_min_win_prob_filter():
    # MIN_WIN_PROB 未満の馬は候補から除外され、組合せに現れない
    table = _prob_table("r1", [(1, 0.5), (2, 0.49), (3, 0.001)])
    limits = RiskLimits(MIN_WIN_PROB=0.01)
    policy = ExpectedValueBetPolicy(
        _FixedOddsProvider(100.0), thresholds={BetType.TANSHO: 1.0}, risk_limits=limits
    )
    selected = policy.select(table)
    umaban_used = {c.combo[0] for c in selected}
    assert 3 not in umaban_used


def test_multiple_races():
    table = pd.concat(
        [_prob_table("r1", [(1, 0.6), (2, 0.4)]), _prob_table("r2", [(1, 0.7), (2, 0.3)])]
    )
    policy = ExpectedValueBetPolicy(_FixedOddsProvider(4.0), thresholds={BetType.TANSHO: 1.0})
    selected = policy.select(table)
    race_ids = {c.race_id for c in selected}
    assert race_ids == {"r1", "r2"}


# ──────────────────────────────────────────
# Stage A: 複勝はモデルの top3 出力を直接使う
# ──────────────────────────────────────────

class TestDirectPlaceProb:
    def test_fukusho_uses_model_prob_directly(self):
        # モデル top3 出力 0.4 をそのまま的中確率に。odds=3 → EV=1.2
        table = _prob_table("r1", [(1, 0.4), (2, 0.4), (3, 0.4)])
        policy = ExpectedValueBetPolicy(
            _FixedOddsProvider(3.0), thresholds={BetType.FUKUSHO: 1.0}
        )
        selected = policy.select(table)
        c1 = next(c for c in selected if c.combo == (1,))
        assert c1.probability == pytest.approx(0.4)
        assert c1.expected_value == pytest.approx(1.2)

    def test_direct_differs_from_harville(self):
        # direct=False（従来 Harville 経路）では複勝確率が正規化勝率由来になり
        # 直接出力 0.4 と一致しない（再導出されている）ことを示す。
        table = _prob_table("r1", [(1, 0.4), (2, 0.3), (3, 0.3)])
        direct = ExpectedValueBetPolicy(
            _FixedOddsProvider(3.0), thresholds={BetType.FUKUSHO: 0.0},
            direct_place_prob=True,
        ).select(table)
        harv = ExpectedValueBetPolicy(
            _FixedOddsProvider(3.0), thresholds={BetType.FUKUSHO: 0.0},
            direct_place_prob=False,
        ).select(table)
        p_direct = next(c for c in direct if c.combo == (1,)).probability
        p_harv = next(c for c in harv if c.combo == (1,)).probability
        assert p_direct == pytest.approx(0.4)
        assert p_harv != pytest.approx(0.4)

    def test_separate_place_table_used_for_fukusho(self):
        # prob_table（Win ヘッド相当）と place_prob_table（Place ヘッド）を分離。
        # 複勝は place 側 0.55 を使い、win 側 0.2 は使わない。
        win_table = _prob_table("r1", [(1, 0.2), (2, 0.2), (3, 0.2)])
        place_table = _prob_table("r1", [(1, 0.55), (2, 0.5), (3, 0.5)])
        policy = ExpectedValueBetPolicy(
            _FixedOddsProvider(2.0), thresholds={BetType.FUKUSHO: 0.0}
        )
        selected = policy.select(win_table, place_prob_table=place_table)
        c1 = next(c for c in selected if c.combo == (1,))
        assert c1.probability == pytest.approx(0.55)


def test_ev_max_excludes_super_high_ev():
    # odds=100, prob=0.5 -> EV=50 をオーバーする ev_max=10 で除外
    table = _prob_table("r1", [(1, 0.5), (2, 0.5)])
    policy = ExpectedValueBetPolicy(
        _FixedOddsProvider(100.0), thresholds={BetType.TANSHO: 1.0}, ev_max=10.0
    )
    assert policy.select(table) == []


def test_ev_max_keeps_within_bound():
    # EV=2.0 は閾値1.0超・上限10.0以内なので採用
    table = _prob_table("r1", [(1, 0.5), (2, 0.5)])
    policy = ExpectedValueBetPolicy(
        _FixedOddsProvider(4.0), thresholds={BetType.TANSHO: 1.0}, ev_max=10.0
    )
    selected = policy.select(table)
    assert len(selected) == 2


def test_ev_max_default_is_infinite():
    # 既定 ev_max=inf → 超高EVも除外しない（後方互換）
    table = _prob_table("r1", [(1, 0.5), (2, 0.5)])
    policy = ExpectedValueBetPolicy(_FixedOddsProvider(1000.0), thresholds={BetType.TANSHO: 1.0})
    selected = policy.select(table)
    assert len(selected) == 2


def test_judge_returns_dict_format():
    table = _prob_table("r1", [(1, 0.6), (2, 0.4)])
    policy = ExpectedValueBetPolicy(_FixedOddsProvider(4.0), thresholds={BetType.TANSHO: 1.0})
    bet_dict = policy.judge(table)
    assert "r1" in bet_dict
    assert BetType.TANSHO in bet_dict["r1"]
    assert set(bet_dict["r1"][BetType.TANSHO]) == {1, 2}


def test_judge_flattens_combo_umaban():
    # 馬連: 組合せ (1,2) を馬番リスト [1,2] に展開
    table = _prob_table("r1", [(1, 0.5), (2, 0.5)])
    policy = ExpectedValueBetPolicy(_FixedOddsProvider(100.0), thresholds={BetType.UMAREN: 1.0})
    bet_dict = policy.judge(table)
    assert BetType.UMAREN in bet_dict["r1"]
    assert set(bet_dict["r1"][BetType.UMAREN]) == {1, 2}


def test_judge_empty_when_no_candidates():
    table = _prob_table("r1", [(1, 0.5), (2, 0.5)])
    policy = ExpectedValueBetPolicy(_FixedOddsProvider(1.0), thresholds={BetType.TANSHO: 1.0})
    assert policy.judge(table) == {}


class _BadOddsProvider(AbstractOddsProvider):
    """異常オッズ（NaN/0/inf）を返すスタブ。"""

    def __init__(self, odds: float) -> None:
        self._odds = odds

    def get_odds(self, race_id, bet_type, combo) -> float:
        return self._odds


def test_nan_probability_excluded():
    # prob=NaN の馬は eligible から除外され候補に出ない
    table = _prob_table("r1", [(1, 0.5), (2, float("nan"))])
    policy = ExpectedValueBetPolicy(_FixedOddsProvider(4.0), thresholds={BetType.TANSHO: 1.0})
    selected = policy.select(table)
    umabans = {c.combo[0] for c in selected}
    assert 2 not in umabans
    assert 1 in umabans


def test_nonpositive_probability_excluded():
    table = _prob_table("r1", [(1, 0.5), (2, 0.0), (3, -0.1)])
    policy = ExpectedValueBetPolicy(_FixedOddsProvider(4.0), thresholds={BetType.TANSHO: 1.0})
    umabans = {c.combo[0] for c in policy.select(table)}
    assert umabans == {1}


def test_abnormal_odds_skipped():
    # オッズが 0 / inf / NaN の馬券は EV 計算されず採用されない
    table = _prob_table("r1", [(1, 0.5), (2, 0.5)])
    for bad in (0.0, float("inf"), float("nan")):
        policy = ExpectedValueBetPolicy(_BadOddsProvider(bad), thresholds={BetType.TANSHO: 1.0})
        assert policy.select(table) == []
