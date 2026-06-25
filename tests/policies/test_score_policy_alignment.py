"""ExpectedValueScorePolicy.calc の列アライメント耐性（--no-odds-features 縮小モデル対応）。"""

import numpy as np
import pandas as pd

from src.constants._feature_cols import MARKET_SIGNAL_FEATURE_COLS
from src.constants._feature_cols import ODDS_DERIVED_FEATURE_COLS
from src.constants._results_cols import ResultsCols
from src.policies._score_policy import PROB
from src.policies._score_policy import ExpectedValueScorePolicy


class TestOddsDerivedCols:
    def test_contains_log_and_market_signals(self):
        assert "単勝_log" in ODDS_DERIVED_FEATURE_COLS
        for c in MARKET_SIGNAL_FEATURE_COLS:
            assert c in ODDS_DERIVED_FEATURE_COLS
            assert f"{c}_z" in ODDS_DERIVED_FEATURE_COLS

    def test_excludes_raw_tansho_and_non_odds(self):
        # 生の単勝は EV/オッズ供給が列の存在を前提にするため除外リストに含めない
        assert "単勝" not in ODDS_DERIVED_FEATURE_COLS
        # 斤量比・休み明け等は対市場ではないので含めない
        assert "kinryo_per_weight" not in ODDS_DERIVED_FEATURE_COLS
        assert "is_layoff" not in ODDS_DERIVED_FEATURE_COLS


class _ReducedModel:
    """単勝_log を学習特徴に持たない縮小モデルの模擬。

    feature_name_ に無い列が X に混ざると最初の predict_proba は失敗し、
    reindex フォールバックで縮小特徴に揃えてから成功する、という挙動を再現する。
    """

    def __init__(self, feature_name_):
        self.feature_name_ = feature_name_

    def predict_proba(self, X):
        cols = list(X.columns)
        if cols != self.feature_name_:
            raise ValueError("feature mismatch")  # 列不一致 → 呼び出し側が reindex
        p = np.linspace(0.1, 0.9, len(X))
        return np.column_stack([1 - p, p])


class TestCalcAlignment:
    def _X(self):
        # 縮小モデルは "f1","f2" のみ学習。X には余分な "単勝_log" が混ざる
        return pd.DataFrame(
            {
                ResultsCols.UMABAN: [1, 2],
                ResultsCols.TANSHO_ODDS: [2.0, 3.0],
                "f1": [0.1, 0.2],
                "f2": [0.3, 0.4],
                "単勝_log": [0.7, 1.1],  # 縮小モデルには無い列
            },
            index=pd.Index(["r1", "r1"], name="race_id"),
        )

    def test_reindex_fallback_succeeds(self):
        model = _ReducedModel(["f1", "f2"])
        table = ExpectedValueScorePolicy.calc(model, self._X())
        assert PROB in table.columns
        assert len(table) == 2
        # current_odds は元の単勝を保持
        assert list(table["current_odds"]) == [2.0, 3.0]

    def test_matching_model_no_fallback(self):
        # 余分列が無ければそのまま成功
        X = self._X().drop(columns=["単勝_log"])
        model = _ReducedModel(["f1", "f2"])
        table = ExpectedValueScorePolicy.calc(model, X)
        assert len(table) == 2
