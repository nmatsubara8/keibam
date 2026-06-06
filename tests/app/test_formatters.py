"""app/_formatters.py の純粋関数テスト。"""

import numpy as np
import pandas as pd
import pytest

from app._formatters import candidates_to_display_df
from app._formatters import format_combo
from app._formatters import format_ev
from app._formatters import format_odds
from app._formatters import format_prob
from app._formatters import format_stake
from app._formatters import snapshots_to_chart_df
from src.constants._bet_types import BetType
from src.policies._bet_candidate import BetCandidate


def test_format_ev_above_threshold():
    assert "✓" in format_ev(1.5, threshold=1.0)
    assert "1.50" in format_ev(1.5)


def test_format_ev_below_threshold():
    assert "✗" in format_ev(0.9, threshold=1.0)


def test_format_combo_tansho():
    assert format_combo((3,), BetType.TANSHO) == "3"


def test_format_combo_fukusho():
    assert format_combo((5,), BetType.FUKUSHO) == "5"


def test_format_combo_umaren():
    assert format_combo((1, 3), BetType.UMAREN) == "1-3"


def test_format_combo_sanrentan():
    assert format_combo((1, 2, 4), BetType.SANRENTAN) == "1-2-4"


def test_format_prob():
    assert "35.0%" in format_prob(0.35)


def test_format_odds():
    assert "x 3.5" in format_odds(3.5)


def test_format_stake_rounds_to_hundred():
    s = format_stake(1234.0)
    assert "1,200" in s


def test_candidates_to_display_df_empty():
    df = candidates_to_display_df([])
    assert df.empty


def test_candidates_to_display_df_columns():
    c = BetCandidate(
        race_id="r1", bet_type=BetType.TANSHO, combo=(1,),
        probability=0.6, odds=2.0, expected_value=1.2,
        confidence=0.8, stake=500.0,
    )
    df = candidates_to_display_df([c])
    for col in ["馬券種", "組合せ", "的中確率", "オッズ", "EV", "確信度", "推奨金額"]:
        assert col in df.columns


def test_candidates_to_display_df_sorted_by_ev():
    low = BetCandidate("r1", BetType.TANSHO, (1,), 0.3, 2.0, 1.1, stake=100.0)
    high = BetCandidate("r1", BetType.TANSHO, (2,), 0.4, 4.0, 1.6, stake=200.0)
    df = candidates_to_display_df([low, high])
    # 1行目が高い EV
    assert "1.60" in df.iloc[0]["EV"]


def test_snapshots_to_chart_df_empty():
    df = snapshots_to_chart_df([])
    assert df.empty
