"""app/_prediction_service.py のテスト（スタブモデル使用）。"""

import numpy as np
import pandas as pd

from app._prediction_service import default_thresholds
from app._prediction_service import run_prediction
from src.constants._bet_types import BetType
from src.constants._results_cols import ResultsCols
from src.operation._config import OperationConfig


class _StubModel:
    """固定確率を返すスタブ較正モデル。"""

    def __init__(self, probs):
        self._probs = np.asarray(probs)

    def predict_proba(self, x):
        return np.column_stack([1.0 - self._probs, self._probs])


def _make_X(race_id: str, rows: list) -> pd.DataFrame:
    """(umaban, wakuban, tansho_odds, feat) のリストから X を組む。"""
    data = []
    for umaban, wakuban, tansho_odds, feat in rows:
        data.append(
            {
                ResultsCols.UMABAN: umaban,
                ResultsCols.WAKUBAN: wakuban,
                ResultsCols.TANSHO_ODDS: tansho_odds,
                "feat": feat,
            }
        )
    return pd.DataFrame(data, index=[race_id] * len(data))


def _default_op_config(**kwargs) -> OperationConfig:
    defaults = dict(bankroll=100_000.0, kelly_fraction_ratio=0.5, per_bet_cap_ratio=0.1, max_daily_ratio=1.0)
    defaults.update(kwargs)
    return OperationConfig(**defaults)


def test_run_prediction_returns_candidates_when_ev_positive():
    X = _make_X("r1", [(1, 1, 2.0, 0.1), (2, 2, 5.0, 0.2), (3, 3, 20.0, 0.3)])
    model = _StubModel([0.65, 0.25, 0.10])
    thresholds = {BetType.TANSHO: 1.0, BetType.UMAREN: 1.0, BetType.SANRENPUKU: 1.0}
    result = run_prediction(model, X, _default_op_config(), thresholds=thresholds)
    assert isinstance(result, list)
    assert len(result) > 0
    assert all(c.expected_value > 1.0 for c in result)


def test_run_prediction_max_odds_filters_longshots():
    # 馬3は EV2.0(0.10*20) で閾値は超えるが、オッズ20倍 > max_odds=15 で除外されるべき。
    X = _make_X("r1", [(1, 1, 2.0, 0.1), (2, 2, 5.0, 0.2), (3, 3, 20.0, 0.3)])
    model = _StubModel([0.65, 0.25, 0.10])
    thresholds = {BetType.TANSHO: 1.0}
    uncapped = run_prediction(model, X, _default_op_config(), thresholds=thresholds)
    capped = run_prediction(model, X, _default_op_config(max_odds=15.0), thresholds=thresholds)
    assert any(c.combo == (3,) for c in uncapped)  # 上限なしなら20倍馬は採用
    assert all(c.odds <= 15.0 for c in capped)
    assert all(c.combo != (3,) for c in capped)  # 上限ありなら20倍馬は除外


def test_run_prediction_tansho_ev_threshold_override():
    # 既定 BetThresholds(単勝1.78) では EV1.3 の本命は不採用。config で1.1へ下げると採用。
    X = _make_X("r1", [(1, 1, 2.0, 0.1), (2, 2, 5.0, 0.2), (3, 3, 20.0, 0.3)])
    model = _StubModel([0.65, 0.25, 0.10])
    base = run_prediction(model, X, _default_op_config())
    lowered = run_prediction(model, X, _default_op_config(tansho_ev_threshold=1.1))
    base_tansho = [c for c in base if c.combo == (1,)]
    lowered_tansho = [c for c in lowered if c.combo == (1,)]
    assert not base_tansho       # 1.78 では本命(EV1.3)は出ない
    assert lowered_tansho        # 1.1 では本命が出る


def test_run_prediction_stake_within_bankroll():
    X = _make_X("r1", [(1, 1, 2.0, 0.1), (2, 2, 5.0, 0.2), (3, 3, 20.0, 0.3)])
    model = _StubModel([0.65, 0.25, 0.10])
    thresholds = {BetType.TANSHO: 1.0, BetType.UMAREN: 1.0}
    op = _default_op_config(bankroll=50_000.0)
    result = run_prediction(model, X, op, thresholds=thresholds)
    total = sum(c.stake for c in result)
    assert total <= 50_000.0


def test_run_prediction_returns_empty_when_no_ev():
    """全馬の EV が閾値未満なら空リスト。"""
    X = _make_X("r1", [(1, 1, 1.1, 0.1), (2, 2, 1.2, 0.1)])
    model = _StubModel([0.4, 0.6])
    # 高い閾値を設定して全候補を弾く
    thresholds = {BetType.TANSHO: 10.0, BetType.UMAREN: 10.0}
    result = run_prediction(model, X, _default_op_config(), thresholds=thresholds)
    assert result == []


def test_run_prediction_confidence_in_range():
    X = _make_X("r1", [(1, 1, 2.0, 0.1), (2, 2, 5.0, 0.2), (3, 3, 20.0, 0.3)])
    model = _StubModel([0.65, 0.25, 0.10])
    thresholds = {BetType.TANSHO: 1.0}
    result = run_prediction(model, X, _default_op_config(), thresholds=thresholds)
    for c in result:
        assert 0.0 <= c.confidence <= 1.0


class TestLiveTakeout:
    def test_explicit_takeout_passthrough(self):
        from app._prediction_service import _load_live_takeout

        assert _load_live_takeout(0.25) == 0.25
        m = {BetType.UMAREN: 0.3}
        assert _load_live_takeout(m) is m

    def test_auto_loads_calibration(self, monkeypatch):
        import app._prediction_service as ps
        import src.policies._takeout_calibration as tc

        monkeypatch.setattr(tc, "latest_takeout_map", lambda path: {BetType.UMAREN: 0.27})
        assert ps._load_live_takeout(None) == {BetType.UMAREN: 0.27}

    def test_falls_back_to_default_when_no_calibration(self, monkeypatch):
        import app._prediction_service as ps
        import src.policies._takeout_calibration as tc

        monkeypatch.setattr(tc, "latest_takeout_map", lambda path: {})
        assert ps._load_live_takeout(None) == 0.2

    def test_higher_takeout_lowers_combo_ev(self):
        """券種別控除率を上げると連系（馬連）の推定オッズ＝EV が下がる。"""
        X = _make_X("r1", [(1, 1, 2.0, 0.1), (2, 2, 5.0, 0.2), (3, 3, 20.0, 0.3)])
        model = _StubModel([0.65, 0.25, 0.10])
        thresholds = {BetType.UMAREN: 0.0}  # 全馬連を採用してEVを観測

        def _umaren_ev(takeout):
            res = run_prediction(
                model, X, _default_op_config(), thresholds=thresholds, takeout=takeout
            )
            evs = [c.expected_value for c in res if c.bet_type == BetType.UMAREN]
            return max(evs) if evs else None

        low = _umaren_ev({BetType.UMAREN: 0.0})
        high = _umaren_ev({BetType.UMAREN: 0.5})
        assert low is not None and high is not None
        assert high < low


class TestEvCalibrationWiring:
    def test_load_ev_artifacts_all_none_when_absent(self, tmp_path):
        from app._prediction_service import _load_ev_artifacts

        assert _load_ev_artifacts(str(tmp_path)) == (None, None, None)

    def test_disabled_by_default_does_not_load(self, monkeypatch):
        """use_ev_calibration 既定 False では _load_ev_artifacts を呼ばない。"""
        import app._prediction_service as ps

        called = {"n": 0}

        def _spy(*a, **k):
            called["n"] += 1
            return (None, None, None)

        monkeypatch.setattr(ps, "_load_ev_artifacts", _spy)
        X = _make_X("r1", [(1, 1, 2.0, 0.1), (2, 2, 5.0, 0.2), (3, 3, 20.0, 0.3)])
        model = _StubModel([0.65, 0.25, 0.10])
        ps.run_prediction(model, X, _default_op_config(), thresholds={BetType.TANSHO: 1.0})
        assert called["n"] == 0

    def test_enabled_loads_and_applies_calibrator(self, monkeypatch):
        """use_ev_calibration=True で較正器を適用すると勝率（候補確率）が変わる。"""
        import app._prediction_service as ps
        from src.policies._calibration import IsotonicCalibrator

        # 本命(高raw)を持ち上げる較正写像
        cal = IsotonicCalibrator(x=(0.10, 0.65), y=(0.05, 0.95))
        monkeypatch.setattr(ps, "_load_ev_artifacts", lambda *a, **k: (None, cal, None))

        X = _make_X("r1", [(1, 1, 2.0, 0.1), (2, 2, 5.0, 0.2), (3, 3, 20.0, 0.3)])
        model = _StubModel([0.65, 0.25, 0.10])
        th = {BetType.TANSHO: 0.0}
        base = ps.run_prediction(model, X, _default_op_config(), thresholds=th)
        wired = ps.run_prediction(
            model, X, _default_op_config(use_ev_calibration=True), thresholds=th
        )
        p_base = {c.combo: c.probability for c in base if c.bet_type == BetType.TANSHO}
        p_wired = {c.combo: c.probability for c in wired if c.bet_type == BetType.TANSHO}
        assert (1,) in p_base and (1,) in p_wired
        assert p_wired[(1,)] != p_base[(1,)]  # 較正で勝率が変化


def test_default_thresholds_covers_all_bet_types():
    th = default_thresholds()
    for bt in (BetType.TANSHO, BetType.FUKUSHO, BetType.UMAREN, BetType.UMATAN,
               BetType.WIDE, BetType.SANRENPUKU, BetType.SANRENTAN):
        assert bt in th
        assert th[bt] > 0.0
