"""券種別パラメータ DTO・永続化・EV ポリシー配線のテスト。"""

import math

import pandas as pd
import pytest

from src.constants._bet_types import BetType
from src.constants._results_cols import ResultsCols
from src.policies._bet_policy import ExpectedValueBetPolicy
from src.policies._bet_type_params import BetTypeParams
from src.policies._bet_type_params import apply_temperature
from src.policies._bet_type_params import bet_type_params_path
from src.policies._bet_type_params import default_params
from src.policies._bet_type_params import default_params_set
from src.policies._bet_type_params import latest_bet_type_params
from src.policies._bet_type_params import load_bet_type_params_records
from src.policies._bet_type_params import params_for
from src.policies._bet_type_params import save_bet_type_params
from src.policies._odds_provider import AbstractOddsProvider


# ---------------------------------------------------------------------------
# DTO
# ---------------------------------------------------------------------------

def test_default_params_uses_bet_thresholds():
    p = default_params(BetType.UMAREN)
    assert p.ev_threshold == pytest.approx(3.26)
    assert p.temperature == 1.0
    assert p.prob_scale == 1.0
    assert math.isinf(p.ev_max)


def test_to_from_dict_roundtrip_with_inf():
    p = BetTypeParams(ev_threshold=1.5, temperature=1.3, prob_scale=0.9, ev_max=math.inf)
    d = p.to_dict()
    assert d["ev_max"] is None  # inf は None で保存
    assert BetTypeParams.from_dict(d) == p


def test_to_from_dict_roundtrip_finite_ev_max():
    p = BetTypeParams(ev_threshold=1.2, ev_max=50.0)
    assert BetTypeParams.from_dict(p.to_dict()) == p


def test_params_for_fallback_to_default():
    assert params_for(BetType.WIDE, None) == default_params(BetType.WIDE)
    custom = BetTypeParams(ev_threshold=2.0)
    assert params_for(BetType.WIDE, {BetType.WIDE: custom}) is custom


def test_default_params_set_excludes_wakuren():
    s = default_params_set()
    assert BetType.WAKUREN not in s
    assert BetType.TANSHO in s and BetType.SANRENTAN in s


# ---------------------------------------------------------------------------
# apply_temperature
# ---------------------------------------------------------------------------

def test_apply_temperature_identity():
    wp = {1: 0.5, 2: 0.3, 3: 0.2}
    assert apply_temperature(wp, 1.0) == wp


def test_apply_temperature_sharpens():
    """β>1 で勝率比が拡大（人気側が尖る）。"""
    wp = {1: 0.6, 2: 0.4}
    out = apply_temperature(wp, 2.0)
    # 0.6^2 / 0.4^2 = 2.25 > 0.6/0.4 = 1.5
    assert out[1] / out[2] > wp[1] / wp[2]


# ---------------------------------------------------------------------------
# 永続化
# ---------------------------------------------------------------------------

def test_save_load_latest_roundtrip(tmp_path):
    path = bet_type_params_path(str(tmp_path))
    pmap = {
        BetType.UMAREN: BetTypeParams(ev_threshold=1.5, temperature=1.3),
        BetType.WIDE: BetTypeParams(ev_threshold=1.1, prob_scale=0.8),
    }
    save_bet_type_params(pmap, path, objective="return_rate", metrics={BetType.UMAREN: {"return_rate": 1.1}})

    records = load_bet_type_params_records(path)
    assert len(records) == 1
    assert records[0]["objective"] == "return_rate"

    latest = latest_bet_type_params(path)
    assert latest[BetType.UMAREN] == pmap[BetType.UMAREN]
    assert latest[BetType.WIDE] == pmap[BetType.WIDE]


def test_save_same_day_replaces(tmp_path):
    path = bet_type_params_path(str(tmp_path))
    save_bet_type_params({BetType.UMAREN: BetTypeParams(ev_threshold=1.5)}, path)
    save_bet_type_params({BetType.UMAREN: BetTypeParams(ev_threshold=2.0)}, path)
    records = load_bet_type_params_records(path)
    assert len(records) == 1  # 同日は置換
    assert latest_bet_type_params(path)[BetType.UMAREN].ev_threshold == 2.0


def test_latest_empty_when_no_file(tmp_path):
    assert latest_bet_type_params(bet_type_params_path(str(tmp_path))) == {}


# ---------------------------------------------------------------------------
# ExpectedValueBetPolicy への配線
# ---------------------------------------------------------------------------

class _FixedOddsProvider(AbstractOddsProvider):
    def __init__(self, odds: float) -> None:
        self._odds = odds

    def get_odds(self, race_id, bet_type, combo) -> float:
        return self._odds


def _prob_table(race_id, umaban_probs):
    rows = [{ResultsCols.UMABAN: u, "prob": p} for u, p in umaban_probs]
    return pd.DataFrame(rows, index=[race_id] * len(rows))


def test_bet_type_params_override_threshold():
    """bet_type_params の ev_threshold が thresholds より優先される。"""
    table = _prob_table("r1", [(1, 0.5), (2, 0.5)])  # tansho EV = 0.5*odds
    # odds=4 → EV=2.0。thresholds=1.0 なら採用されるが、params で 2.5 に上げると除外。
    policy = ExpectedValueBetPolicy(
        _FixedOddsProvider(4.0),
        thresholds={BetType.TANSHO: 1.0},
        bet_type_params={BetType.TANSHO: BetTypeParams(ev_threshold=2.5)},
    )
    assert policy.select(table) == []


def test_temperature_changes_combo_probability():
    """温度 β>1 で人気馬中心の組合せ確率が上がる（EV と選定が変わる）。"""
    table = _prob_table("r1", [(1, 0.6), (2, 0.3), (3, 0.1)])
    base = ExpectedValueBetPolicy(
        _FixedOddsProvider(10.0), thresholds={BetType.UMAREN: 1.0}, bet_types=[BetType.UMAREN]
    )
    hot = ExpectedValueBetPolicy(
        _FixedOddsProvider(10.0), thresholds={BetType.UMAREN: 1.0}, bet_types=[BetType.UMAREN],
        bet_type_params={BetType.UMAREN: BetTypeParams(ev_threshold=1.0, temperature=2.0)},
    )
    base_probs = {tuple(sorted(c.combo)): c.probability for c in base.select(table)}
    hot_probs = {tuple(sorted(c.combo)): c.probability for c in hot.select(table)}
    # 人気上位ペア (1,2) の確率は温度を上げると増える
    assert hot_probs[(1, 2)] > base_probs[(1, 2)]


def test_prob_scale_scales_ev():
    """prob_scale=0.5 は EV を半分にし、閾値ギリギリの賭けを除外する。"""
    table = _prob_table("r1", [(1, 0.5), (2, 0.5)])
    # EV(base) = 0.5 * 3.0 = 1.5。scale=0.5 → 0.75 < 1.0 で除外。
    scaled = ExpectedValueBetPolicy(
        _FixedOddsProvider(3.0), thresholds={BetType.TANSHO: 1.0},
        bet_type_params={BetType.TANSHO: BetTypeParams(ev_threshold=1.0, prob_scale=0.5)},
    )
    assert scaled.select(table) == []


def test_no_params_preserves_legacy_behavior():
    """bet_type_params 未指定なら従来挙動（thresholds + ev_max）を保持。"""
    table = _prob_table("r1", [(1, 0.5), (2, 0.5)])
    policy = ExpectedValueBetPolicy(_FixedOddsProvider(4.0), thresholds={BetType.TANSHO: 1.0})
    selected = policy.select(table)
    assert len(selected) == 2
