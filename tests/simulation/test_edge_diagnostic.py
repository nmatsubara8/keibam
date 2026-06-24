"""パリミュチュエル Edge/EV 診断（src.simulation._edge_diagnostic）のテスト。"""

import numpy as np
import pandas as pd
import pytest

from src.constants._results_cols import ResultsCols
from src.policies._score_policy import CURRENT_ODDS
from src.policies._score_policy import PROB
from src.simulation import _edge_diagnostic as ed


def _score_table(probs, odds, race_ids, umaban):
    return pd.DataFrame(
        {ResultsCols.UMABAN: umaban, PROB: probs, CURRENT_ODDS: odds},
        index=pd.Index(race_ids, name="race_id"),
    )


class TestMarketImpliedProb:
    def test_normalized_within_race(self):
        odds = pd.Series([2.0, 4.0, 4.0], index=["r1", "r1", "r1"])
        p = ed.market_implied_prob(odds)
        # 1/2,1/4,1/4 = 0.5,0.25,0.25 → 正規化済みで和1
        assert p.sum() == 1.0
        assert p.iloc[0] == 0.5

    def test_nonpositive_dropped(self):
        odds = pd.Series([2.0, 0.0, np.nan], index=["r1", "r1", "r1"])
        p = ed.market_implied_prob(odds)
        assert p.iloc[0] == 1.0  # 有効は1頭だけ→1.0
        assert np.isnan(p.iloc[1])


class TestBuildEdgeFrame:
    def _table(self):
        # 2レース×2頭。r1: モデルは1番を強気(0.8 vs 0.2)、市場は均等(2.0,2.0)
        return _score_table(
            probs=[0.8, 0.2, 0.5, 0.5],
            odds=[2.0, 2.0, 1.5, 3.0],
            race_ids=["r1", "r1", "r2", "r2"],
            umaban=[1, 2, 1, 2],
        )

    def test_r_hat_normalized(self):
        df = ed.build_edge_frame(self._table(), won=[1, 0, 0, 1])
        # r1: 0.8/(0.8+0.2)=0.8, 0.2 → 和1
        r1 = df.loc["r1"]
        assert r1["r_hat"].sum() == 1.0
        assert r1["r_hat"].iloc[0] == 0.8

    def test_edge_and_ev(self):
        df = ed.build_edge_frame(self._table(), won=[1, 0, 0, 1])
        row = df.loc["r1"].iloc[0]
        # p_mkt(均等)=0.5, r_hat=0.8 → edge=+0.3、EV=0.8*2.0-1=0.6
        assert row["p_mkt"] == 0.5
        assert row["edge"] == pytest.approx(0.3)
        assert row["ev"] == pytest.approx(0.6)

    def test_pop_rank(self):
        df = ed.build_edge_frame(self._table(), won=[1, 0, 0, 1])
        # r2: odds 1.5(本命) vs 3.0 → 馬1 が pop_rank=1
        r2 = df.loc["r2"]
        assert r2["pop_rank"].iloc[0] == 1.0
        assert r2["pop_rank"].iloc[1] == 2.0


class TestWinLogloss:
    def test_model_beats_market_when_better_calibrated(self):
        # モデルが勝ち馬に高い確率、市場は均等 → モデル logloss が小さい
        table = _score_table(
            probs=[0.9, 0.1, 0.9, 0.1],
            odds=[2.0, 2.0, 2.0, 2.0],  # 市場は均等(0.5,0.5)
            race_ids=["r1", "r1", "r2", "r2"],
            umaban=[1, 2, 1, 2],
        )
        df = ed.build_edge_frame(table, won=[1, 0, 1, 0])  # いつも馬1が勝つ
        s = ed.diagnostic_summary(df)
        assert s["model_win_logloss"] < s["market_win_logloss"]
        assert s["model_beats_market"] is True

    def test_nan_when_no_winner(self):
        table = _score_table([0.5, 0.5], [2.0, 2.0], ["r1", "r1"], [1, 2])
        df = ed.build_edge_frame(table, won=[0, 0])
        assert np.isnan(ed._win_logloss(df["r_hat"], df["won"]))


class TestEchoCorrelation:
    def test_high_when_model_mirrors_market(self):
        # r_hat と p_mkt がほぼ一致（モデル=市場の写し）→ 相関≈1
        table = _score_table(
            probs=[0.5, 0.25, 0.25, 0.5, 0.25, 0.25],
            odds=[2.0, 4.0, 4.0, 2.0, 4.0, 4.0],
            race_ids=["r1"] * 3 + ["r2"] * 3,
            umaban=[1, 2, 3, 1, 2, 3],
        )
        df = ed.build_edge_frame(table, won=[1, 0, 0, 1, 0, 0])
        assert ed.echo_correlation(df) == pytest.approx(1.0, abs=1e-6)


class TestCalibrationByBand:
    def test_bands_and_columns(self):
        n = 40
        rng = list(range(n))
        table = _score_table(
            probs=[0.5] * n,
            odds=[1.0 + i * 0.1 for i in rng],
            race_ids=[f"r{i // 2}" for i in rng],
            umaban=[1 + i % 2 for i in rng],
        )
        df = ed.build_edge_frame(table, won=[i % 2 for i in rng])
        calib = ed.calibration_by_band(df, n_bands=4)
        expected = {"band", "n", "p_mkt_mean", "r_hat_mean", "win_rate", "edge_mean", "ev_mean"}
        assert expected.issubset(calib.columns)
        assert calib["n"].sum() == len(df.dropna(subset=["p_mkt", "r_hat"]))
