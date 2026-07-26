"""Layer2: スナップショット系列 → 確定オッズ予測 学習データ構築のテスト。"""

import datetime as dt

import numpy as np
import pytest

from src.constants._bet_types import BetType
from src.constants._odds_phases import OddsPhase
from src.preparing._odds_snapshot import OddsSnapshot
from src.policies import _arbitrage as _arb
from src.training._odds_feature_builder import build_training_frame
from src.training._odds_feature_builder import recover_pools_by_phase
from src.training._odds_feature_builder import snapshots_to_phase_table
from src.training._odds_feature_builder import train_odds_predictor

_BASE = dt.datetime(2024, 1, 1, 12, 0)


def _snap(race_id, combo, odds, phase, minutes=30, bet_type=BetType.TANSHO, captured=None):
    return OddsSnapshot(
        race_id=race_id,
        bet_type=bet_type,
        combo=tuple(combo),
        odds=float(odds),
        captured_at=captured or _BASE,
        minutes_to_post=minutes,
        phase=phase,
    )


def _race_series(race_id, horse, prev_day, hours_before, thirty_min, just_before):
    """1 頭分の 4 フェーズのスナップショット系列を作る。"""
    return [
        _snap(race_id, [horse], prev_day, OddsPhase.PREV_DAY, 1200),
        _snap(race_id, [horse], hours_before, OddsPhase.HOURS_BEFORE, 180),
        _snap(race_id, [horse], thirty_min, OddsPhase.THIRTY_MIN, 30),
        _snap(race_id, [horse], just_before, OddsPhase.T0, 1),
    ]


class TestSnapshotsToPhaseTable:
    def test_pivot_to_wide(self):
        snaps = _race_series("r1", 1, 4.0, 3.5, 3.0, 2.5)
        wide = snapshots_to_phase_table(snaps)
        assert wide.loc[("r1", "1"), f"odds_{OddsPhase.PREV_DAY}"] == 4.0
        assert wide.loc[("r1", "1"), f"odds_{OddsPhase.T0}"] == 2.5

    def test_filters_bet_type(self):
        snaps = [
            _snap("r1", [1], 2.0, OddsPhase.THIRTY_MIN),
            _snap("r1", [1, 2], 12.0, OddsPhase.THIRTY_MIN, bet_type=BetType.UMAREN),
        ]
        wide = snapshots_to_phase_table(snaps, BetType.UMAREN)
        assert list(wide.index) == [("r1", "1-2")]

    def test_duplicate_phase_keeps_latest_captured(self):
        snaps = [
            _snap("r1", [1], 3.0, OddsPhase.THIRTY_MIN, captured=_BASE),
            _snap("r1", [1], 2.0, OddsPhase.THIRTY_MIN, captured=_BASE + dt.timedelta(minutes=5)),
        ]
        wide = snapshots_to_phase_table(snaps)
        assert wide.loc[("r1", "1"), f"odds_{OddsPhase.THIRTY_MIN}"] == 2.0

    def test_empty_input(self):
        wide = snapshots_to_phase_table([])
        assert wide.empty


class TestBuildTrainingFrame:
    def test_features_and_target(self):
        snaps = _race_series("r1", 1, 4.0, 3.5, 3.0, 2.5)
        features, final_odds, feature_cols = build_training_frame(snaps)
        assert feature_cols == [
            "current_odds",
            f"log_ratio_{OddsPhase.PREV_DAY}",
            f"log_ratio_{OddsPhase.HOURS_BEFORE}",
        ]
        row = features.loc[("r1", "1")]
        assert row["current_odds"] == 3.0
        assert row[f"log_ratio_{OddsPhase.PREV_DAY}"] == pytest.approx(np.log(3.0 / 4.0))
        assert final_odds.loc[("r1", "1")] == 2.5

    def test_drops_rows_missing_current_or_target(self):
        snaps = [
            # target なし
            _snap("r1", [1], 3.0, OddsPhase.THIRTY_MIN),
            # current なし
            _snap("r1", [2], 2.5, OddsPhase.T0),
            # 両方あり
            _snap("r1", [3], 6.0, OddsPhase.THIRTY_MIN),
            _snap("r1", [3], 5.0, OddsPhase.T0),
        ]
        features, final_odds, _ = build_training_frame(snaps)
        assert list(features.index) == [("r1", "3")]
        assert final_odds.tolist() == [5.0]

    def test_missing_earlier_phase_fills_zero_ratio(self):
        snaps = [
            _snap("r1", [1], 3.0, OddsPhase.THIRTY_MIN),
            _snap("r1", [1], 2.5, OddsPhase.T0),
        ]
        features, _, _ = build_training_frame(snaps)
        assert features.loc[("r1", "1"), f"log_ratio_{OddsPhase.PREV_DAY}"] == 0.0

    def test_invalid_current_phase_raises(self):
        with pytest.raises(ValueError):
            build_training_frame([], current_phase=OddsPhase.T0)

    def test_empty_snapshots(self):
        features, final_odds, feature_cols = build_training_frame([])
        assert features.empty
        assert final_odds.empty
        assert feature_cols == []


class TestTrainOddsPredictor:
    def _make_snapshots(self, n_races=30, horses=6, seed=0):
        rng = np.random.default_rng(seed)
        snaps = []
        for r in range(n_races):
            race_id = f"r{r:03d}"
            for h in range(1, horses + 1):
                prev = float(rng.uniform(2, 50))
                # 締切に向けて緩やかに変動する系列
                hb = prev * float(rng.uniform(0.9, 1.1))
                tm = hb * float(rng.uniform(0.9, 1.1))
                jb = tm * float(rng.uniform(0.95, 1.05))
                snaps.extend(_race_series(race_id, h, prev, hb, tm, jb))
        return snaps

    def test_returns_none_when_insufficient_rows(self):
        snaps = _race_series("r1", 1, 4.0, 3.5, 3.0, 2.5)
        assert train_odds_predictor(snaps, min_rows=100) is None

    def test_trains_and_predicts(self):
        snaps = self._make_snapshots()
        predictor = train_odds_predictor(snaps, min_rows=50, n_estimators=10, verbose=-1)
        assert predictor is not None

        features, final_odds, _ = build_training_frame(snaps)
        pred = predictor.predict(features)
        assert len(pred) == len(features)
        assert (pred > 0).all()
        # レース内 implied-prob 総和は現在オッズと整合するよう正規化される
        race = features.index.get_level_values(0)[0]
        cur_sum = (1.0 / features.loc[race, "current_odds"]).sum()
        pred_sum = (1.0 / pred.loc[race]).sum()
        assert pred_sum == pytest.approx(cur_sum, rel=1e-6)


# ──────────────────────────────────────────
# プール逆算（V_t 復元）の配線（芦谷 2012）
# ──────────────────────────────────────────

def _trifecta_snaps(race_id, phase, pool_S, counts, minutes):
    """既知の総売上 S と {combo: 売上枚数} から三連単スナップショット群を作る。"""
    snaps = []
    for combo, s in counts.items():
        odds = _arb.odds_of(pool_S, s, truncate=False)
        snaps.append(_snap(race_id, list(combo), odds, phase, minutes,
                           bet_type=BetType.SANRENTAN))
    return snaps


class TestRecoverPoolsByPhase:
    def _counts(self):
        return {(1, 2, 3): 50, (1, 3, 2): 30, (2, 1, 3): 20, (3, 2, 1): 8, (2, 3, 1): 13}

    def test_recovers_pool_per_phase(self):
        c = self._counts()
        snaps = (
            _trifecta_snaps("r1", OddsPhase.THIRTY_MIN, 10000, c, 30)
            + _trifecta_snaps("r1", OddsPhase.T0, 14000, c, 1)
        )
        pools = recover_pools_by_phase(snaps, BetType.SANRENTAN)
        assert pools[("r1", OddsPhase.THIRTY_MIN)] == 10000
        assert pools[("r1", OddsPhase.T0)] == 14000

    def test_ignores_other_bet_types_and_singletons(self):
        # 単一組合せのみ・他券種は逆算しない
        snaps = [_snap("r1", [1], 3.0, OddsPhase.THIRTY_MIN, bet_type=BetType.TANSHO)]
        assert recover_pools_by_phase(snaps, BetType.SANRENTAN) == {}


class TestBuildFrameWithPool:
    def test_pool_features_added_when_enabled(self):
        c = {(1, 2, 3): 50, (1, 3, 2): 30, (2, 1, 3): 20, (3, 2, 1): 8, (2, 3, 1): 13}
        # 三連単で 30分前=10000 → T0=14000 にプール成長。current_phase=THIRTY_MIN
        snaps = (
            _trifecta_snaps("r1", OddsPhase.PREV_DAY, 5000, c, 1200)
            + _trifecta_snaps("r1", OddsPhase.THIRTY_MIN, 10000, c, 30)
            + _trifecta_snaps("r1", OddsPhase.T0, 14000, c, 1)
        )
        features, _, feature_cols = build_training_frame(
            snaps, bet_type=BetType.SANRENTAN, include_pool=True
        )
        assert "pool_current" in feature_cols
        assert f"pool_log_ratio_{OddsPhase.PREV_DAY}" in feature_cols
        row = features.iloc[0]
        assert row["pool_current"] == 10000
        # 5000 → 10000 の成長 = log(10000/5000) = log 2
        assert row[f"pool_log_ratio_{OddsPhase.PREV_DAY}"] == pytest.approx(np.log(2.0))

    def test_no_pool_features_by_default(self):
        snaps = _race_series("r1", 1, 4.0, 3.5, 3.0, 2.5)
        _, _, feature_cols = build_training_frame(snaps)
        assert not any(c.startswith("pool_") for c in feature_cols)
