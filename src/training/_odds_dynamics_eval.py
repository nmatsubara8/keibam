"""オッズ力学モデルの比較評価ハーネス（時系列 holdout）。

蓄積スナップショットを時系列で train/test に分け、各モデル + アンサンブルの
予測精度（KL / 勝ち馬 log-loss / シェア MAE / オッズ MAPE）を比較する。
結果は `models/odds_dynamics_eval.json` に保存し（同日評価は置き換え）、
アンサンブル重み（検証 KL の逆数比）もここで算出・永続化する。
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os

import numpy as np
import pandas as pd

from src.constants._odds_dynamics import DYNAMICS_EVAL_FILENAME
from src.constants._odds_phases import OddsPhase
from src.training._odds_dynamics import AbstractShareDynamicsModel
from src.training._odds_dynamics import EnsembleShareModel
from src.training._odds_dynamics import HORIZON_FINAL
from src.training._odds_dynamics import default_models
from src.training._odds_gravity import fit_gravity
from src.training._share_predictor_adapter import shares_to_odds
from src.training._simplex import kl_divergence
from src.training._simplex import odds_mape
from src.training._simplex import share_mae

logger = logging.getLogger(__name__)


def dynamics_eval_path(models_dir: str = "models") -> str:
    return os.path.join(models_dir, DYNAMICS_EVAL_FILENAME)


def split_sequences(sequences: dict, holdout_frac: float = 0.2) -> tuple[dict, dict]:
    """race_id 昇順（≒時系列）で train/test に分割する（純粋関数）。"""
    race_ids = sorted(sequences.keys())
    n_test = max(1, int(len(race_ids) * holdout_frac))
    test_ids = set(race_ids[-n_test:])
    train = {rid: s for rid, s in sequences.items() if rid not in test_ids}
    test = {rid: s for rid, s in sequences.items() if rid in test_ids}
    return train, test


def _evaluate_one(
    model: AbstractShareDynamicsModel, test: dict, winners: dict | None
) -> dict:
    """1 モデルの held-out 指標を集計する。"""
    kls, maes, mapes, loglosses = [], [], [], []
    for race_id, per_phase in test.items():
        if OddsPhase.T0 not in per_phase:
            continue
        obs = {p: s for p, s in per_phase.items() if p != OddsPhase.T0}
        if not obs:
            continue
        pred = model.predict_shares(obs, HORIZON_FINAL)
        actual = per_phase[OddsPhase.T0]
        common = pred.index.intersection(actual.index)
        if len(common) < 2:
            continue
        p = actual.loc[common].to_numpy()
        q = pred.loc[common].to_numpy()
        p = p / p.sum()
        q = q / q.sum()
        kls.append(kl_divergence(p, q))
        maes.append(share_mae(p, q))
        mapes.append(odds_mape(shares_to_odds(pd.Series(p)).to_numpy(),
                               shares_to_odds(pd.Series(q)).to_numpy()))
        if winners:
            winner = winners.get(race_id)
            if winner is not None and str(winner) in common:
                pos = list(common).index(str(winner))
                loglosses.append(-float(np.log(max(q[pos], 1e-12))))

    def _mean(xs):
        return float(np.mean(xs)) if xs else float("nan")

    return {
        "n_test_races": len(kls),
        "kl_mean": _mean(kls),
        "share_mae": _mean(maes),
        "odds_mape": _mean(mapes),
        "winner_logloss": _mean(loglosses),
    }


def ensemble_weights_from_kl(kl_by_model: dict[str, float], eps: float = 1e-6) -> dict[str, float]:
    """検証 KL の逆数比でアンサンブル重みを算出する（純粋関数）。"""
    inv = {name: 1.0 / (kl + eps) for name, kl in kl_by_model.items() if np.isfinite(kl)}
    total = sum(inv.values())
    if total <= 0:
        return {name: 1.0 / len(kl_by_model) for name in kl_by_model}
    return {name: w / total for name, w in inv.items()}


def evaluate_dynamics_models(
    sequences: dict,
    models: dict[str, AbstractShareDynamicsModel] | None = None,
    holdout_frac: float = 0.2,
    winners: dict | None = None,
) -> dict:
    """全モデル + アンサンブルを時系列 holdout で比較評価する。

    Parameters
    ----------
    sequences : race_share_sequences の出力。
    models : 比較対象（既定は default_models()）。
    winners : race_id → 勝ち馬番（str）。指定時は勝ち馬 log-loss も算出。

    Returns
    -------
    dict : {"evaluated_at", "gravity": GravityStats, "results": {model: 指標},
            "ensemble_weights": {model: w}}
    """
    models = models or default_models()
    train, test = split_sequences(sequences, holdout_frac)
    gravity = fit_gravity(train)

    results: dict = {}
    for name, model in models.items():
        model.fit(train, gravity)
        results[name] = _evaluate_one(model, test, winners)

    weights = ensemble_weights_from_kl(
        {name: r["kl_mean"] for name, r in results.items()}
    )
    ensemble = EnsembleShareModel(models, weights=weights)
    results["ensemble"] = _evaluate_one(ensemble, test, winners)

    return {
        "evaluated_at": dt.datetime.now().isoformat(),
        "n_train_races": len(train),
        "gravity": gravity,
        "results": results,
        "ensemble_weights": weights,
    }


# ---------------------------------------------------------------------------
# 永続化（models/odds_dynamics_eval.json）
# ---------------------------------------------------------------------------


def save_dynamics_eval(evaluation: dict, path: str) -> None:
    """評価結果を JSON 保存する（同日付の評価は置き換え）。"""
    day = evaluation["evaluated_at"][:10]
    existing = [r for r in load_dynamics_eval(path) if r.get("evaluated_at", "")[:10] != day]
    records = []
    for model_name, metrics in evaluation["results"].items():
        records.append(
            {
                "evaluated_at": evaluation["evaluated_at"],
                "model": model_name,
                "n_train_races": evaluation["n_train_races"],
                "ensemble_weight": evaluation["ensemble_weights"].get(model_name),
                **metrics,
            }
        )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(existing + records, f, ensure_ascii=False, indent=2)
    logger.info("[dynamics_eval] %s: %d records saved", path, len(records))


def load_dynamics_eval(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return json.load(f)


def latest_ensemble_weights(path: str) -> dict[str, float]:
    """保存済み評価から最新のアンサンブル重みを取り出す（無ければ空 = 等重み扱い）。"""
    records = load_dynamics_eval(path)
    if not records:
        return {}
    latest_ts = max(r["evaluated_at"] for r in records)
    return {
        r["model"]: r["ensemble_weight"]
        for r in records
        if r["evaluated_at"] == latest_ts and r.get("ensemble_weight") is not None and r["model"] != "ensemble"
    }
