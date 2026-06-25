"""オッズ由来特徴の除外リスト（--no-odds-features の A/B 用）の不変条件テスト。

学習は X_train.values（位置ベース・generic 特徴名）で行われるため、縮小モデルの評価では
名前ベースの reindex では整合できず、retrain/backtest 双方で **同じ列を落として位置を一致**
させる必要がある。本テストは除外リストの中身（生の単勝を残し派生のみ落とす）を固定する。
"""

import pandas as pd

from src.constants._feature_cols import MARKET_SIGNAL_FEATURE_COLS
from src.constants._feature_cols import ODDS_DERIVED_FEATURE_COLS


class TestOddsDerivedCols:
    def test_contains_log_and_market_signals(self):
        assert "単勝_log" in ODDS_DERIVED_FEATURE_COLS
        assert "単勝_log_z" in ODDS_DERIVED_FEATURE_COLS
        for c in MARKET_SIGNAL_FEATURE_COLS:
            assert c in ODDS_DERIVED_FEATURE_COLS
            assert f"{c}_z" in ODDS_DERIVED_FEATURE_COLS

    def test_excludes_raw_tansho_and_non_odds(self):
        # 生の単勝は EV/オッズ供給が列の存在を前提にするため除外リストに含めない
        assert "単勝" not in ODDS_DERIVED_FEATURE_COLS
        # 斤量比・休み明け等は対市場ではないので含めない
        assert "kinryo_per_weight" not in ODDS_DERIVED_FEATURE_COLS
        assert "is_layoff" not in ODDS_DERIVED_FEATURE_COLS


class TestDropSymmetry:
    def test_drop_keeps_tansho_removes_derived(self):
        # retrain/backtest が同じ列集合を落とすと featured から派生のみ消え、単勝・着順・
        # rank_win 等のインフラ列は残る（列の位置一致＝予測可能）。
        cols = ["単勝", "単勝_log", "単勝_log_z", "着順", "rank_win", *MARKET_SIGNAL_FEATURE_COLS, "keep"]
        df = pd.DataFrame({c: [1] for c in cols})
        present = [c for c in ODDS_DERIVED_FEATURE_COLS if c in df.columns]
        out = df.drop(columns=present, errors="ignore")
        assert "単勝" in out.columns
        assert "着順" in out.columns and "rank_win" in out.columns and "keep" in out.columns
        assert "単勝_log" not in out.columns
        for c in MARKET_SIGNAL_FEATURE_COLS:
            assert c not in out.columns
