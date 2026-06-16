"""オッズ力学モデル評価ハーネス（_odds_dynamics_eval）のテスト。"""

import numpy as np
import pytest

from src.training._odds_dynamics import default_models
from src.training._odds_dynamics_eval import dynamics_eval_path
from src.training._odds_dynamics_eval import ensemble_weights_from_kl
from src.training._odds_dynamics_eval import evaluate_dynamics_models
from src.training._odds_dynamics_eval import latest_ensemble_weights
from src.training._odds_dynamics_eval import load_dynamics_eval
from src.training._odds_dynamics_eval import save_dynamics_eval
from src.training._odds_dynamics_eval import split_sequences

from tests.training.test_odds_dynamics import simulate_simplex_walks


def test_split_sequences_chronological():
    seqs = {f"r{i:03d}": {"x": None} for i in range(10)}
    train, test = split_sequences(seqs, holdout_frac=0.2)
    assert len(test) == 2
    assert set(test) == {"r008", "r009"}  # 末尾（時系列の直近）が検証


def test_ensemble_weights_inverse_kl():
    weights = ensemble_weights_from_kl({"a": 0.1, "b": 0.2})
    assert weights["a"] > weights["b"]
    assert sum(weights.values()) == pytest.approx(1.0)
    # KL 比 2:1 → 重み比 2:1（逆数比）
    assert weights["a"] / weights["b"] == pytest.approx(2.0, rel=0.01)


def test_evaluate_dynamics_models_end_to_end():
    seqs = simulate_simplex_walks(n_races=80, seed=20)
    evaluation = evaluate_dynamics_models(seqs, models=default_models(), holdout_frac=0.2)
    results = evaluation["results"]
    assert set(results) == {"identity", "dirichlet", "kalman", "particle", "ensemble"}
    for metrics in results.values():
        assert metrics["n_test_races"] > 0
        assert np.isfinite(metrics["kl_mean"])
    assert sum(evaluation["ensemble_weights"].values()) == pytest.approx(1.0)


def test_winner_logloss_computed_when_winners_given():
    seqs = simulate_simplex_walks(n_races=30, seed=21)
    winners = {rid: "1" for rid in seqs}
    evaluation = evaluate_dynamics_models(seqs, holdout_frac=0.2, winners=winners)
    assert np.isfinite(evaluation["results"]["identity"]["winner_logloss"])


def test_save_load_and_latest_weights(tmp_path):
    seqs = simulate_simplex_walks(n_races=40, seed=22)
    evaluation = evaluate_dynamics_models(seqs, holdout_frac=0.2)
    path = dynamics_eval_path(str(tmp_path))
    save_dynamics_eval(evaluation, path)

    records = load_dynamics_eval(path)
    assert {r["model"] for r in records} >= {"identity", "kalman", "ensemble"}

    weights = latest_ensemble_weights(path)
    assert set(weights) == {"identity", "dirichlet", "kalman", "particle"}
    assert sum(weights.values()) == pytest.approx(1.0)

    # 同日の再評価は置き換え（重複しない）
    save_dynamics_eval(evaluation, path)
    assert len(load_dynamics_eval(path)) == len(records)
