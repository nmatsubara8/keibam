"""ポートフォリオシミュレーションのテスト。"""

import dataclasses

import pytest

from src.policies._bet_candidate import BetCandidate
from src.portfolio._portfolio_simulator import simulate


def _cand(combo, odds, stake, win):
    return dataclasses.replace(
        BetCandidate("r1", "tansho", combo, 0.5, odds, 0.5 * odds, stake=stake),
        # win フラグは payout_fn 側で判定するため combo に埋め込む
    ), win


def test_all_wins_increases_bankroll():
    c = BetCandidate("r1", "tansho", (1,), 0.5, 3.0, 1.5, stake=100.0)
    result = simulate([c], payout_fn=lambda x: 3.0, initial_bankroll=1000.0)
    # 返戻 300, 損益 +200
    assert result["final_bankroll"] == pytest.approx(1200.0)
    assert result["profit"] == pytest.approx(200.0)
    assert result["n_hits"] == 1


def test_all_losses_decreases_bankroll():
    c = BetCandidate("r1", "tansho", (1,), 0.5, 3.0, 1.5, stake=100.0)
    result = simulate([c], payout_fn=lambda x: 0.0, initial_bankroll=1000.0)
    assert result["final_bankroll"] == pytest.approx(900.0)
    assert result["n_hits"] == 0
    assert result["return_rate"] == 0.0


def test_zero_stake_skipped():
    c = BetCandidate("r1", "tansho", (1,), 0.5, 3.0, 1.5, stake=0.0)
    result = simulate([c], payout_fn=lambda x: 3.0, initial_bankroll=1000.0)
    assert result["n_bets"] == 0
    assert result["final_bankroll"] == 1000.0


def test_max_drawdown_tracked():
    won = BetCandidate("r1", "tansho", (1,), 0.5, 3.0, 1.5, stake=100.0)
    lost = BetCandidate("r2", "tansho", (2,), 0.5, 3.0, 1.5, stake=400.0)
    # 勝ち(+200)→負け(-400): ピーク1200から800へ、DD=400
    result = simulate([won, lost], payout_fn=lambda x: 3.0 if x.race_id == "r1" else 0.0, initial_bankroll=1000.0)
    assert result["max_drawdown"] == pytest.approx(400.0)
