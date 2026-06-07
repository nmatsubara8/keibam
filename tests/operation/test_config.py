"""OperationConfig の単体テスト。

YAML ロード・デフォルト値・バリデーション・frozen 不変性を検証する。
"""

from __future__ import annotations

import dataclasses

import pytest

from src.operation._config import (
    ADVISORY,
    FULL_AUTO,
    SEMI_AUTO,
    OperationConfig,
)


def test_default_operation_mode():
    cfg = OperationConfig()
    assert cfg.operation_mode == ADVISORY


def test_default_bankroll_positive():
    cfg = OperationConfig()
    assert cfg.bankroll > 0


def test_default_kelly_fraction_in_range():
    cfg = OperationConfig()
    assert 0 < cfg.kelly_fraction_ratio <= 1


def test_default_per_bet_cap_in_range():
    cfg = OperationConfig()
    assert 0 < cfg.per_bet_cap_ratio <= 1


def test_default_max_daily_ratio_positive():
    cfg = OperationConfig()
    assert cfg.max_daily_ratio > 0


def test_from_dict_only_mode():
    cfg = OperationConfig.from_dict({"operation_mode": SEMI_AUTO})
    assert cfg.operation_mode == SEMI_AUTO
    assert cfg.bankroll == 100000.0  # デフォルト維持


def test_from_dict_all_fields():
    cfg = OperationConfig.from_dict(
        {
            "operation_mode": FULL_AUTO,
            "bankroll": 50000.0,
            "kelly_fraction_ratio": 0.25,
            "per_bet_cap_ratio": 0.02,
            "max_daily_ratio": 0.5,
        }
    )
    assert cfg.operation_mode == FULL_AUTO
    assert cfg.bankroll == 50000.0
    assert cfg.kelly_fraction_ratio == 0.25


def test_from_dict_ignores_unknown_keys():
    cfg = OperationConfig.from_dict({"operation_mode": ADVISORY, "unknown_key": "ignored"})
    assert cfg.operation_mode == ADVISORY


def test_from_dict_empty_dict_uses_defaults():
    cfg = OperationConfig.from_dict({})
    assert cfg.operation_mode == ADVISORY


def test_invalid_operation_mode_raises():
    with pytest.raises(ValueError):
        OperationConfig(operation_mode="invalid_mode")


def test_frozen_instance_raises_on_mutation():
    cfg = OperationConfig()
    with pytest.raises(dataclasses.FrozenInstanceError):
        cfg.operation_mode = FULL_AUTO  # type: ignore[misc]


def test_load_from_yaml(tmp_path):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text("operation_mode: semi_auto\nbankroll: 200000\n")
    cfg = OperationConfig.load(str(cfg_file))
    assert cfg.operation_mode == SEMI_AUTO
    assert cfg.bankroll == 200000.0


def test_load_missing_file_raises():
    with pytest.raises((FileNotFoundError, OSError)):
        OperationConfig.load("/nonexistent/path/config.yaml")
