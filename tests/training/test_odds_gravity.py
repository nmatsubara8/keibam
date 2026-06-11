"""「市場の重力」（_odds_gravity）のテスト。"""

import numpy as np
import pandas as pd
import pytest

from src.constants._odds_dynamics import DEFAULT_DRIFT
from src.constants._odds_dynamics import bucket_for_rank
from src.constants._odds_phases import OddsPhase
from src.training._odds_gravity import GravityStats
from src.training._odds_gravity import adjacent_phase_pairs
from src.training._odds_gravity import default_vol
from src.training._odds_gravity import fit_gravity
from src.training._odds_gravity import gravity_path
from src.training._odds_gravity import load_gravity
from src.training._odds_gravity import save_gravity
from src.training._simplex import clr
from src.training._simplex import clr_inv


def _make_sequences(n_races=200, n_horses=8, fav_drift=0.3, vol=0.05, seed=0):
    """1 番人気の CLR が fav_drift だけ上がる合成系列（thirty_min → t0）。"""
    rng = np.random.default_rng(seed)
    seqs = {}
    for r in range(n_races):
        base = np.sort(rng.uniform(0.5, 3.0, n_horses))[::-1]
        s0 = clr_inv(base)
        x0 = clr(s0)
        x1 = x0 + rng.normal(0, vol, n_horses)
        x1[np.argmax(s0)] += fav_drift  # 1 番人気にだけ既知の drift
        idx = pd.Index([str(i + 1) for i in range(n_horses)])
        seqs[f"r{r}"] = {
            OddsPhase.THIRTY_MIN: pd.Series(s0, index=idx),
            OddsPhase.T0: pd.Series(clr_inv(x1), index=idx),
        }
    return seqs


class TestFitGravity:
    def test_recovers_known_drift(self):
        stats = fit_gravity(_make_sequences())
        drift, vol = stats.lookup(OddsPhase.THIRTY_MIN, OddsPhase.T0, rank=1)
        # 縮小推定込みでも n=200 なら既知 drift≈0.3 の近くに回復する
        # (CLR の中心化により 0.3 - 0.3/8 ≈ 0.26 が真値)
        assert 0.15 < drift < 0.35

    def test_empty_returns_defaults(self):
        stats = fit_gravity({})
        drift, vol = stats.lookup(OddsPhase.THIRTY_MIN, OddsPhase.T0, rank=1)
        assert drift == DEFAULT_DRIFT
        assert vol == default_vol(OddsPhase.THIRTY_MIN, OddsPhase.T0)

    def test_shrinkage_monotone_in_n(self):
        """観測数が多いほど推定 drift が経験値に近づく（縮小が弱まる）。"""
        small = fit_gravity(_make_sequences(n_races=10))
        large = fit_gravity(_make_sequences(n_races=300))
        d_small, _ = small.lookup(OddsPhase.THIRTY_MIN, OddsPhase.T0, rank=1)
        d_large, _ = large.lookup(OddsPhase.THIRTY_MIN, OddsPhase.T0, rank=1)
        assert abs(d_large) > abs(d_small)  # 既定値 0 からの距離が大きい

    def test_adjacent_phase_pairs_ordering(self):
        pairs = adjacent_phase_pairs([OddsPhase.T0, OddsPhase.THIRTY_MIN, OddsPhase.T10])
        assert pairs == [
            (OddsPhase.THIRTY_MIN, OddsPhase.T10),
            (OddsPhase.T10, OddsPhase.T0),
        ]


def test_save_load_roundtrip(tmp_path):
    stats = fit_gravity(_make_sequences(n_races=50))
    path = gravity_path(str(tmp_path))
    save_gravity(stats, path)
    loaded = load_gravity(path)
    key = (OddsPhase.THIRTY_MIN, OddsPhase.T0, bucket_for_rank(1))
    assert loaded.table[key]["n"] == stats.table[key]["n"]
    assert loaded.table[key]["drift"] == pytest.approx(stats.table[key]["drift"])


def test_load_missing_returns_empty():
    stats = load_gravity("/nonexistent/odds_gravity.json")
    assert isinstance(stats, GravityStats)
    assert stats.table == {}
