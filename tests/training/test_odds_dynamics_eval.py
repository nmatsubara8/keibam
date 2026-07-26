"""オッズ力学モデル評価ハーネス（_odds_dynamics_eval）のテスト。"""

import numpy as np
import pandas as pd
import pytest

from src.constants._results_cols import ResultsCols
from src.training._odds_dynamics import default_models
from src.training._odds_dynamics_eval import dynamics_eval_path
from src.training._odds_dynamics_eval import ensemble_weights_from_kl
from src.training._odds_dynamics_eval import evaluate_dynamics_models
from src.training._odds_dynamics_eval import latest_ensemble_weights
from src.training._odds_dynamics_eval import load_dynamics_eval
from src.training._odds_dynamics_eval import race_winners
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


# ---------------------------------------------------------------------------
# race_winners（results → race_id→勝ち馬番）
# ---------------------------------------------------------------------------

def _results_df(rows, index_race_id=False):
    df = pd.DataFrame(rows)
    if index_race_id:
        df = df.set_index("race_id")
    return df


def test_race_winners_basic():
    df = _results_df([
        {"race_id": 202401010101, ResultsCols.RANK: 1, ResultsCols.UMABAN: 7},
        {"race_id": 202401010101, ResultsCols.RANK: 2, ResultsCols.UMABAN: 3},
        {"race_id": 202401010102, ResultsCols.RANK: 3, ResultsCols.UMABAN: 5},
        {"race_id": 202401010102, ResultsCols.RANK: 1, ResultsCols.UMABAN: 11},
    ])
    assert race_winners(df) == {"202401010101": "7", "202401010102": "11"}


def test_race_winners_race_id_in_index():
    df = _results_df([
        {"race_id": 202401010101, ResultsCols.RANK: 1, ResultsCols.UMABAN: 4},
        {"race_id": 202401010101, ResultsCols.RANK: 2, ResultsCols.UMABAN: 8},
    ], index_race_id=True)
    assert race_winners(df) == {"202401010101": "4"}


def test_race_winners_canonicalizes_float_race_id_and_umaban():
    """int64 由来でなく float64 由来（202401010101.0）でも snapshot の str と一致する。"""
    df = _results_df([
        {"race_id": 202401010101.0, ResultsCols.RANK: 1.0, ResultsCols.UMABAN: 7.0},
    ])
    assert race_winners(df) == {"202401010101": "7"}


def test_race_winners_ignores_non_numeric_rank():
    """中止/除外（着順が非数値）の行は無視する。"""
    df = _results_df([
        {"race_id": 1, ResultsCols.RANK: "中", ResultsCols.UMABAN: 2},
        {"race_id": 1, ResultsCols.RANK: "1", ResultsCols.UMABAN: 5},
    ])
    assert race_winners(df) == {"1": "5"}


def test_race_winners_dead_heat_takes_first():
    df = _results_df([
        {"race_id": 1, ResultsCols.RANK: 1, ResultsCols.UMABAN: 2},
        {"race_id": 1, ResultsCols.RANK: 1, ResultsCols.UMABAN: 9},
    ])
    assert race_winners(df) == {"1": "2"}


def test_race_winners_empty_or_missing_columns():
    assert race_winners(pd.DataFrame()) == {}
    assert race_winners(None) == {}
    # 必要列が無い場合は空 dict（落ちない）
    assert race_winners(pd.DataFrame({"race_id": [1], "foo": [2]})) == {}


def test_race_winners_integrates_with_evaluation():
    """race_winners の出力を evaluate_dynamics_models の winners に渡せる。"""
    seqs = simulate_simplex_walks(n_races=30, seed=23)
    # 各レースの勝ち馬番（"1"）を results 形式で用意
    rows = []
    for rid in seqs:
        rows.append({"race_id": rid, ResultsCols.RANK: 1, ResultsCols.UMABAN: 1})
        rows.append({"race_id": rid, ResultsCols.RANK: 2, ResultsCols.UMABAN: 2})
    winners = race_winners(pd.DataFrame(rows))
    evaluation = evaluate_dynamics_models(seqs, holdout_frac=0.2, winners=winners)
    assert np.isfinite(evaluation["results"]["ensemble"]["winner_logloss"])
