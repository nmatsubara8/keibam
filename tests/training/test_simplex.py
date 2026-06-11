"""シンプレックス変換（_simplex）のテスト。"""

import numpy as np
import pandas as pd
import pytest

from src.constants._odds_phases import OddsPhase
from src.training._simplex import clr
from src.training._simplex import clr_inv
from src.training._simplex import kl_divergence
from src.training._simplex import odds_mape
from src.training._simplex import popularity_ranks
from src.training._simplex import race_share_sequences
from src.training._simplex import share_mae
from src.training._simplex import shares_from_odds


class TestSharesFromOdds:
    def test_overround_cancels(self):
        # 控除率込みオッズ（implied 和 > 1）でもシェアは Σ=1
        odds = np.array([2.0, 4.0, 8.0])
        s = shares_from_odds(odds)
        assert s.sum() == pytest.approx(1.0)
        # シェア比は 1/odds 比
        assert s[0] / s[1] == pytest.approx(2.0)

    def test_invalid_odds_are_nan(self):
        s = shares_from_odds(np.array([2.0, np.nan, 0.0, -1.0]))
        assert np.isnan(s[1]) and np.isnan(s[2]) and np.isnan(s[3])
        assert np.nansum(s) == pytest.approx(1.0)


class TestClr:
    def test_round_trip(self):
        s = np.array([0.5, 0.3, 0.15, 0.05])
        assert clr_inv(clr(s)) == pytest.approx(s)

    def test_clr_sums_to_zero(self):
        x = clr(np.array([0.7, 0.2, 0.1]))
        assert x.sum() == pytest.approx(0.0)

    def test_clr_inv_absorbs_additive_constant(self):
        x = clr(np.array([0.6, 0.3, 0.1]))
        assert clr_inv(x + 5.0) == pytest.approx(clr_inv(x))


def test_popularity_ranks():
    ranks = popularity_ranks(np.array([0.2, 0.5, 0.3]))
    assert ranks.tolist() == [3, 1, 2]


class TestRaceShareSequences:
    def _phase_table(self):
        idx = pd.MultiIndex.from_tuples(
            [("r1", "1"), ("r1", "2"), ("r1", "3")], names=["race_id", "combo"]
        )
        return pd.DataFrame(
            {
                f"odds_{OddsPhase.THIRTY_MIN}": [2.0, 4.0, 8.0],
                f"odds_{OddsPhase.T0}": [1.8, 4.5, np.nan],  # 馬 3 は直前に取消
            },
            index=idx,
        )

    def test_builds_normalized_sequences(self):
        seqs = race_share_sequences(self._phase_table())
        per_phase = seqs["r1"]
        assert per_phase[OddsPhase.THIRTY_MIN].sum() == pytest.approx(1.0)
        assert len(per_phase[OddsPhase.THIRTY_MIN]) == 3

    def test_scratched_horse_dropped_and_renormalized(self):
        seqs = race_share_sequences(self._phase_table())
        final = seqs["r1"][OddsPhase.T0]
        assert "3" not in final.index
        assert final.sum() == pytest.approx(1.0)

    def test_legacy_phase_normalized(self):
        idx = pd.MultiIndex.from_tuples([("r1", "1"), ("r1", "2")])
        table = pd.DataFrame({"odds_just_before": [2.0, 3.0]}, index=idx)
        seqs = race_share_sequences(table)
        assert OddsPhase.T10 in seqs["r1"]  # just_before → t10 に正規化

    def test_empty(self):
        assert race_share_sequences(pd.DataFrame()) == {}
        assert race_share_sequences(None) == {}


class TestMetrics:
    def test_kl_zero_for_identical(self):
        p = np.array([0.5, 0.3, 0.2])
        assert kl_divergence(p, p) == pytest.approx(0.0, abs=1e-9)
        assert kl_divergence(p, np.array([0.4, 0.4, 0.2])) > 0

    def test_share_mae(self):
        assert share_mae(np.array([0.5, 0.5]), np.array([0.4, 0.6])) == pytest.approx(0.1)

    def test_odds_mape(self):
        assert odds_mape(np.array([2.0, 4.0]), np.array([2.2, 3.6])) == pytest.approx(0.1)
