"""運用モード別 Executor のテスト。"""

import dataclasses

import pytest

from src.operation._bet_executor import AutoExecutor
from src.operation._bet_executor import create_bet_executor
from src.operation._config import ADVISORY
from src.operation._config import FULL_AUTO
from src.operation._config import SEMI_AUTO
from src.operation._config import OperationConfig
from src.policies._bet_candidate import BetCandidate


def _cand(stake):
    return BetCandidate("r1", "tansho", (1,), 0.5, 3.0, 1.5, confidence=0.8, stake=stake)


def test_advisory_records_recommended():
    sink = []
    ex = create_bet_executor(ADVISORY, recorder=sink.append)
    records = ex.execute([_cand(100.0), _cand(0.0)])  # stake0 は除外
    assert len(records) == 1
    assert records[0]["status"] == "recommended"
    assert sink == records


def test_semi_auto_queues():
    sink = []
    ex = create_bet_executor(SEMI_AUTO, recorder=sink.append)
    records = ex.execute([_cand(50.0)])
    assert records[0]["status"] == "queued"


def test_full_auto_disabled_by_default():
    ex = create_bet_executor(FULL_AUTO, recorder=lambda r: None)
    assert isinstance(ex, AutoExecutor)
    with pytest.raises(NotImplementedError):
        ex.execute([_cand(100.0)])


def test_full_auto_enabled_places():
    sink = []
    ex = create_bet_executor(FULL_AUTO, recorder=sink.append, enable_auto=True)
    records = ex.execute([_cand(100.0)])
    assert records[0]["status"] == "placed"


def test_invalid_mode_raises_in_config():
    with pytest.raises(ValueError):
        OperationConfig(operation_mode="invalid")


def test_config_from_dict_filters_unknown_keys():
    cfg = OperationConfig.from_dict({"operation_mode": SEMI_AUTO, "unknown": 1, "bankroll": 5000.0})
    assert cfg.operation_mode == SEMI_AUTO
    assert cfg.bankroll == 5000.0


def test_record_has_created_at_timestamp():
    sink = []
    ex = create_bet_executor(ADVISORY, recorder=sink.append)
    records = ex.execute([_cand(100.0)])
    assert "created_at" in records[0]
    # ISO 8601 としてパース可能
    import datetime as dt
    dt.datetime.fromisoformat(records[0]["created_at"])


def test_config_validates_max_daily_loss_ratio():
    with pytest.raises(ValueError):
        OperationConfig(max_daily_loss_ratio=0.0)
    with pytest.raises(ValueError):
        OperationConfig(max_daily_loss_ratio=1.5)
    # 正常値は通る
    assert OperationConfig(max_daily_loss_ratio=0.3).max_daily_loss_ratio == 0.3
