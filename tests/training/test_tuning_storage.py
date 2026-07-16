"""Optuna study 永続化（--resume-tuning）の再開＋単調改善を検証する。"""
from __future__ import annotations

import pytest


def test_study_kwargs_off_by_default(monkeypatch):
    from src.training._tuning_storage import study_kwargs, tuning_storage_url

    monkeypatch.delenv("KEIBA_TUNING_STORAGE", raising=False)
    assert tuning_storage_url() == ""
    assert study_kwargs("nn") == {}  # 未設定なら従来のメモリ内 study


def test_study_kwargs_on_when_env_set(monkeypatch):
    from src.training._tuning_storage import study_kwargs

    monkeypatch.setenv("KEIBA_TUNING_STORAGE", "sqlite:///x.db")
    assert study_kwargs("xgboost") == {
        "storage": "sqlite:///x.db",
        "study_name": "xgboost",
        "load_if_exists": True,
    }


def test_persistent_study_resumes_and_accumulates(tmp_path, monkeypatch):
    """永続 study は再実行で trial を追記し、best が単調に非悪化する（＝再開で改善）。"""
    optuna = pytest.importorskip("optuna")

    from src.training._tuning_storage import study_kwargs

    url = f"sqlite:///{tmp_path / 'studies.db'}"
    monkeypatch.setenv("KEIBA_TUNING_STORAGE", url)

    def obj(trial):
        x = trial.suggest_float("x", -1.0, 1.0)
        return (x - 0.3) ** 2

    # 1 回目
    s1 = optuna.create_study(direction="minimize", **study_kwargs("demo"))
    s1.optimize(obj, n_trials=6)
    best1 = s1.best_value
    n1 = len(s1.trials)

    # 2 回目：同名 study を load_if_exists で再開 → trial 追記
    s2 = optuna.create_study(direction="minimize", **study_kwargs("demo"))
    assert len(s2.trials) == n1  # 前回分を引き継いでいる
    s2.optimize(obj, n_trials=6)
    assert len(s2.trials) == n1 + 6  # 追記されている（新規 study ではない）
    assert s2.best_value <= best1  # best は単調非悪化（再開で改善しうる）
