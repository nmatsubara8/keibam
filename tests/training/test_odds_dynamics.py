"""オッズ力学モデル（_odds_dynamics）のテスト — 合成シンプレックスウォークで検証。"""

import numpy as np
import pandas as pd
import pytest

from src.constants._odds_phases import OddsPhase
from src.training._odds_dynamics import DirichletShareModel
from src.training._odds_dynamics import EnsembleShareModel
from src.training._odds_dynamics import HORIZON_FINAL
from src.training._odds_dynamics import HORIZON_NEXT
from src.training._odds_dynamics import IdentityShareModel
from src.training._odds_dynamics import KalmanShareModel
from src.training._odds_dynamics import ParticleShareModel
from src.training._odds_dynamics import default_models
from src.training._odds_gravity import GravityStats
from src.training._odds_gravity import fit_gravity
from src.training._simplex import clr
from src.training._simplex import clr_inv
from src.training._simplex import kl_divergence

PHASES = (OddsPhase.THIRTY_MIN, OddsPhase.T10, OddsPhase.T5, OddsPhase.T0)


def simulate_simplex_walks(
    n_races=120, n_horses=8, fav_drift_per_step=0.1, vol=0.04, heavy_tail=False, seed=0
):
    """CLR 空間の Gaussian（または t）ランダムウォークでシェア系列を合成する。

    1 番人気にだけ各遷移で fav_drift_per_step の上昇 drift を入れる
    （「人気側に資金が集中していく」市場を模す）。
    """
    rng = np.random.default_rng(seed)
    seqs = {}
    for r in range(n_races):
        base = np.sort(rng.uniform(0.3, 2.5, n_horses))[::-1]
        x = clr(clr_inv(base))
        fav = int(np.argmax(x))
        idx = pd.Index([str(i + 1) for i in range(n_horses)])
        per_phase = {}
        for k, phase in enumerate(PHASES):
            if k > 0:
                noise = (
                    rng.standard_t(3, n_horses) * vol if heavy_tail
                    else rng.normal(0, vol, n_horses)
                )
                x = x + noise
                x[fav] += fav_drift_per_step
            per_phase[phase] = pd.Series(clr_inv(x), index=idx)
        seqs[f"r{r:03d}"] = per_phase
    return seqs


def _inputs_only(per_phase):
    """t0 を除いた観測（推論入力）を返す。"""
    return {p: s for p, s in per_phase.items() if p != OddsPhase.T0}


def _mean_kl(model, seqs, horizon=HORIZON_FINAL):
    kls = []
    for per_phase in seqs.values():
        pred = model.predict_shares(_inputs_only(per_phase), horizon)
        actual = per_phase[OddsPhase.T0]
        common = pred.index.intersection(actual.index)
        kls.append(kl_divergence(actual.loc[common].to_numpy(), pred.loc[common].to_numpy()))
    return float(np.mean(kls))


class TestIdentity:
    def test_returns_latest_observation(self):
        seqs = simulate_simplex_walks(n_races=1)
        per_phase = next(iter(seqs.values()))
        pred = IdentityShareModel().fit({}, GravityStats()).predict_shares(_inputs_only(per_phase))
        pd.testing.assert_series_equal(pred, per_phase[OddsPhase.T5], check_names=False)

    def test_empty_obs(self):
        assert IdentityShareModel().predict_shares({}).empty


class TestKalman:
    def test_beats_identity_with_drift(self):
        """drift≠0 の市場では重力込み Kalman が恒等予測の KL を下回る。"""
        train = simulate_simplex_walks(n_races=200, seed=1)
        test = simulate_simplex_walks(n_races=60, seed=2)
        gravity = fit_gravity(train)
        kalman = KalmanShareModel().fit(train, gravity)
        identity = IdentityShareModel()
        assert _mean_kl(kalman, test) < _mean_kl(identity, test)

    def test_prediction_is_valid_simplex(self):
        seqs = simulate_simplex_walks(n_races=3)
        kalman = KalmanShareModel().fit(seqs, fit_gravity(seqs))
        pred = kalman.predict_shares(_inputs_only(next(iter(seqs.values()))))
        assert pred.sum() == pytest.approx(1.0)
        assert (pred > 0).all()

    def test_handles_missing_phase(self):
        seqs = simulate_simplex_walks(n_races=2)
        per_phase = next(iter(seqs.values()))
        obs = {OddsPhase.THIRTY_MIN: per_phase[OddsPhase.THIRTY_MIN]}  # t10/t5 欠損
        pred = KalmanShareModel().fit(seqs, GravityStats()).predict_shares(obs)
        assert pred.sum() == pytest.approx(1.0)

    def test_horizon_next_differs_from_final(self):
        train = simulate_simplex_walks(n_races=100, fav_drift_per_step=0.2, seed=3)
        gravity = fit_gravity(train)
        kalman = KalmanShareModel().fit(train, gravity)
        per_phase = next(iter(train.values()))
        obs = {OddsPhase.THIRTY_MIN: per_phase[OddsPhase.THIRTY_MIN]}
        nxt = kalman.predict_shares(obs, HORIZON_NEXT)
        fin = kalman.predict_shares(obs, HORIZON_FINAL)
        # drift が正なので final の方が 1 番人気シェアが大きい
        fav = per_phase[OddsPhase.THIRTY_MIN].idxmax()
        assert fin[fav] > nxt[fav]


class TestParticle:
    def test_close_to_kalman_on_gaussian_data(self):
        train = simulate_simplex_walks(n_races=150, seed=4)
        test = simulate_simplex_walks(n_races=40, seed=5)
        gravity = fit_gravity(train)
        kal = _mean_kl(KalmanShareModel().fit(train, gravity), test)
        par = _mean_kl(ParticleShareModel(seed=11).fit(train, gravity), test)
        # Gaussian データでは両者は同等（パーティクル誤差ぶんの余裕を持たせる）
        assert par == pytest.approx(kal, rel=0.5)

    def test_valid_simplex_with_heavy_tail_data(self):
        seqs = simulate_simplex_walks(n_races=5, heavy_tail=True, seed=6)
        model = ParticleShareModel(seed=12).fit(seqs, fit_gravity(seqs))
        pred = model.predict_shares(_inputs_only(next(iter(seqs.values()))))
        assert pred.sum() == pytest.approx(1.0)
        assert (pred > 0).all()


class TestDirichlet:
    def test_zero_races_falls_back_to_identity(self):
        model = DirichletShareModel().fit({}, GravityStats())
        seqs = simulate_simplex_walks(n_races=1)
        per_phase = next(iter(seqs.values()))
        pred = model.predict_shares(_inputs_only(per_phase))
        pd.testing.assert_series_equal(pred, per_phase[OddsPhase.T5], check_names=False)

    def test_small_data_uses_moment_matching(self):
        seqs = simulate_simplex_walks(n_races=20)
        model = DirichletShareModel().fit(seqs, GravityStats())
        assert model.beta_ == 1.0
        assert model.precision_ > 5.0

    def test_full_fit_learns_persistence(self):
        seqs = simulate_simplex_walks(n_races=80, fav_drift_per_step=0.0, vol=0.03, seed=7)
        model = DirichletShareModel().fit(seqs, GravityStats())
        # drift なしの市場では現在シェアがほぼ持続する（β ≈ 1）
        assert 0.7 < model.beta_ < 1.3

    def test_prediction_is_valid_simplex(self):
        seqs = simulate_simplex_walks(n_races=60)
        model = DirichletShareModel().fit(seqs, GravityStats())
        pred = model.predict_shares(_inputs_only(next(iter(seqs.values()))))
        assert pred.sum() == pytest.approx(1.0)


class TestEnsemble:
    def test_weighted_average_renormalized(self):
        seqs = simulate_simplex_walks(n_races=30, seed=8)
        gravity = fit_gravity(seqs)
        members = default_models()
        ens = EnsembleShareModel(members, weights={"identity": 1.0, "dirichlet": 1.0, "kalman": 2.0, "particle": 0.0})
        ens.fit(seqs, gravity)
        pred = ens.predict_shares(_inputs_only(next(iter(seqs.values()))))
        assert pred.sum() == pytest.approx(1.0)

    def test_field_sizes_8_and_18(self):
        for n in (8, 18):
            seqs = simulate_simplex_walks(n_races=10, n_horses=n, seed=n)
            ens = EnsembleShareModel(default_models()).fit(seqs, fit_gravity(seqs))
            pred = ens.predict_shares(_inputs_only(next(iter(seqs.values()))))
            assert len(pred) == n
            assert pred.sum() == pytest.approx(1.0)
