"""app/_betting_history.py のテスト。"""

import os

import pytest

from app._betting_history import append_history
from app._betting_history import calc_summary_stats
from app._betting_history import history_to_dataframe
from app._betting_history import load_history


def _sample_record(race_id="r1", stake=1000.0, payout=0.0):
    return {
        "race_id": race_id,
        "bet_type": "tansho",
        "combo": [3],
        "odds": 4.5,
        "probability": 0.3,
        "expected_value": 1.35,
        "confidence": 0.8,
        "stake": stake,
        "payout": payout,
        "status": "recommended",
    }


def test_append_and_load_roundtrip(tmp_path):
    path = str(tmp_path / "hist.jsonl")
    rec = _sample_record()
    append_history(rec, path)
    loaded = load_history(path)
    assert len(loaded) == 1
    assert loaded[0]["race_id"] == "r1"


def test_append_multiple_records(tmp_path):
    path = str(tmp_path / "hist.jsonl")
    append_history(_sample_record("r1"), path)
    append_history(_sample_record("r2"), path)
    assert len(load_history(path)) == 2


def test_load_missing_file_returns_empty(tmp_path):
    path = str(tmp_path / "nope.jsonl")
    assert load_history(path) == []


def test_history_to_dataframe_columns(tmp_path):
    path = str(tmp_path / "hist.jsonl")
    append_history(_sample_record(), path)
    df = history_to_dataframe(load_history(path))
    assert "race_id" in df.columns
    assert "combo" in df.columns
    # combo はリストから文字列に変換される
    assert df.iloc[0]["combo"] == "3"


def test_history_to_dataframe_empty():
    df = history_to_dataframe([])
    assert df.empty


def test_calc_summary_stats_no_history():
    stats = calc_summary_stats([])
    assert stats["n_bets"] == 0
    assert stats["return_rate"] is None


def test_calc_summary_stats_with_payout():
    records = [
        _sample_record(stake=1000.0, payout=4500.0),
        _sample_record(stake=500.0, payout=0.0),
    ]
    stats = calc_summary_stats(records)
    assert stats["n_bets"] == 2
    assert stats["total_stake"] == 1500.0
    assert abs(stats["total_payout"] - 4500.0) < 1e-6
    assert stats["return_rate"] == pytest.approx(3.0, rel=1e-3)


def test_calc_summary_stats_zero_stake():
    stats = calc_summary_stats([_sample_record(stake=0.0)])
    assert stats["return_rate"] is None
