"""Optuna ハイパラ探索の設定（探索範囲・種類・回数）を一元管理する。

従来は `LightGBMTuner`（optuna_integration.lightgbm）の自動段階探索に丸ごと委譲して
おり、探索する種類・範囲・回数はすべてライブラリ内部に固定されていた。本モジュールは
手書き Optuna（`objective` + `suggest_*`）の探索を設定可能にするための DTO を提供する。

## 2 つの探索方式（`method`）

- ``"lightgbm_tuner"``（既定・後方互換）: LightGBMTuner の自動段階探索。`n_trials` や
  `search_space` は無視され、ライブラリ内部の固定挙動になる。
- ``"optuna"``: 手書き Optuna。`search_space` で各パラメータの探索範囲を、`n_trials` で
  試行回数を、`timeout` で打ち切り秒数を指定できる。

## search_space の値の形式

各パラメータの値は以下の 3 形式で指定する:

- **数値範囲** ``[low, high]``: low/high が数値（int/float）のとき数値探索。
  `_INT_PARAMS` に含まれれば整数、`_LOG_SCALE_PARAMS` に含まれれば対数スケール。
- **カテゴリカル** ``["a", "b", "c"]``: 値が文字列のとき categorical 探索。
  3 要素以上でなくてもよい（``["gbdt", "dart"]`` 可）。

## 設定ファイル（JSON）

`load_tuning_config(path)` で以下の形式の JSON を読み込める::

    {
      "method": "optuna",
      "n_trials": 300,
      "timeout": 7200,
      "num_boost_round": 2000,
      "early_stopping_rounds": 100,
      "seed": 100,
      "search_space": {
        "num_leaves":        [8, 512],
        "max_depth":         [-1, 16],
        "learning_rate":     [0.005, 0.5],
        "n_estimators":      [100, 3000],
        "lambda_l1":         [1e-8, 10.0],
        "lambda_l2":         [1e-8, 10.0],
        "feature_fraction":  [0.3, 1.0],
        "bagging_fraction":  [0.3, 1.0],
        "bagging_freq":      [1, 10],
        "min_child_samples": [5, 300],
        "min_child_weight":  [1e-5, 100.0],
        "min_split_gain":    [0.0, 1.0],
        "path_smooth":       [0.0, 10.0],
        "extra_trees":       ["true", "false"],
        "boosting_type":     ["gbdt", "dart"]
      }
    }
"""

from __future__ import annotations

import dataclasses
import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

# 探索方式の識別子
METHOD_LIGHTGBM_TUNER = "lightgbm_tuner"
METHOD_OPTUNA = "optuna"

# log スケールで探索するパラメータ（下限 0 を渡せないため log=True）
_LOG_SCALE_PARAMS = {"learning_rate", "lambda_l1", "lambda_l2", "min_child_weight"}
# 整数で探索するパラメータ
_INT_PARAMS = {"num_leaves", "max_depth", "bagging_freq", "min_child_samples", "n_estimators"}
# bool として扱うカテゴリカルパラメータ（"true"/"false" 文字列 → bool に変換）
_BOOL_PARAMS = {"extra_trees"}

# 手書き Optuna の既定探索範囲（基本 8 種のみ。configs/tuning_config.max.json が全種）
DEFAULT_SEARCH_SPACE: dict[str, list] = {
    "num_leaves":        [8, 256],
    "learning_rate":     [0.01, 0.3],
    "lambda_l1":         [1e-8, 10.0],
    "lambda_l2":         [1e-8, 10.0],
    "feature_fraction":  [0.4, 1.0],
    "bagging_fraction":  [0.4, 1.0],
    "bagging_freq":      [1, 7],
    "min_child_samples": [5, 100],
}

# 「全パラメータ探索」設定（configs/tuning_config.max.json と同内容）
MAX_SEARCH_SPACE: dict[str, list] = {
    "boosting_type":     ["gbdt", "dart"],
    "num_leaves":        [8, 512],
    "max_depth":         [-1, 16],
    "learning_rate":     [0.005, 0.5],
    "n_estimators":      [100, 3000],
    "lambda_l1":         [1e-8, 10.0],
    "lambda_l2":         [1e-8, 10.0],
    "feature_fraction":  [0.3, 1.0],
    "bagging_fraction":  [0.3, 1.0],
    "bagging_freq":      [1, 10],
    "min_child_samples": [5, 300],
    "min_child_weight":  [1e-5, 100.0],
    "min_split_gain":    [0.0, 1.0],
    "path_smooth":       [0.0, 10.0],
    "extra_trees":       ["true", "false"],
}


def _is_categorical(bounds: list) -> bool:
    """bounds が文字列リストならカテゴリカル探索と見なす。"""
    return bool(bounds) and isinstance(bounds[0], str)


@dataclasses.dataclass(frozen=True)
class TuningConfig:
    """Optuna ハイパラ探索の設定。

    method="lightgbm_tuner" のときは n_trials / search_space は無視される。
    """

    method: str = METHOD_LIGHTGBM_TUNER
    n_trials: int = 50
    timeout: float | None = None
    num_boost_round: int = 1000
    early_stopping_rounds: int = 50
    seed: int = 100
    search_space: dict[str, list] = dataclasses.field(
        default_factory=lambda: {k: list(v) for k, v in DEFAULT_SEARCH_SPACE.items()}
    )

    @property
    def is_custom(self) -> bool:
        """手書き Optuna（探索範囲・回数を制御する方式）かどうか。"""
        return self.method == METHOD_OPTUNA

    def suggest_params(self, trial) -> dict[str, Any]:
        """trial から探索範囲に従って LightGBM パラメータ dict を生成する。"""
        params: dict[str, Any] = {
            "objective": "binary",
            "metric": "binary_logloss",
            "verbose": -1,
        }
        for name, bounds in self.search_space.items():
            if _is_categorical(bounds):
                raw = trial.suggest_categorical(name, bounds)
                # "true"/"false" → bool 変換
                if name in _BOOL_PARAMS:
                    params[name] = raw.lower() == "true"
                else:
                    params[name] = raw
            elif name in _INT_PARAMS:
                params[name] = trial.suggest_int(name, int(bounds[0]), int(bounds[1]))
            elif name in _LOG_SCALE_PARAMS:
                params[name] = trial.suggest_float(name, float(bounds[0]), float(bounds[1]), log=True)
            else:
                params[name] = trial.suggest_float(name, float(bounds[0]), float(bounds[1]))

        # dart は early_stopping が機能しないため num_boost_round を固定に
        if params.get("boosting_type") == "dart":
            params["num_boost_round"] = self.num_boost_round

        return params

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


def load_tuning_config(path: str) -> TuningConfig:
    """JSON 設定ファイルから TuningConfig を生成する。"""
    if not os.path.exists(path):
        raise FileNotFoundError(f"チューニング設定ファイルがありません: {path}")
    with open(path) as f:
        raw = json.load(f)
    return from_dict(raw)


def from_dict(raw: dict[str, Any]) -> TuningConfig:
    """dict（設定ファイル/CLI 由来）から TuningConfig を生成する。未知キーは無視。"""
    search_space = raw.get("search_space")
    if search_space is None:
        search_space = {k: list(v) for k, v in DEFAULT_SEARCH_SPACE.items()}
    else:
        # 未指定パラメータは既定範囲で補完する
        merged = {k: list(v) for k, v in DEFAULT_SEARCH_SPACE.items()}
        merged.update({k: list(v) for k, v in search_space.items()})
        search_space = merged
    return TuningConfig(
        method=raw.get("method", METHOD_OPTUNA),
        n_trials=int(raw.get("n_trials", 50)),
        timeout=raw.get("timeout"),
        num_boost_round=int(raw.get("num_boost_round", 1000)),
        early_stopping_rounds=int(raw.get("early_stopping_rounds", 50)),
        seed=int(raw.get("seed", 100)),
        search_space=search_space,
    )

