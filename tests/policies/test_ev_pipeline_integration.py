"""Phase A の選定パイプライン結合テスト（モデル→スコア→オッズ→EV選定→確信度→配分）。

実データ・重い依存を使わず、スタブ較正モデルで一連の疎結合コンポーネントが
連携することを確認する。
"""

import numpy as np
import pandas as pd

from src.constants._bet_thresholds import BetThresholds
from src.constants._bet_types import BetType
from src.constants._results_cols import ResultsCols
from src.policies._bet_policy import ExpectedValueBetPolicy
from src.policies._odds_provider import HistoricalOddsProvider
from src.policies._score_policy import CURRENT_ODDS
from src.policies._score_policy import PROB
from src.policies._score_policy import ExpectedValueScorePolicy
from src.portfolio._confidence import CompositeConfidenceScorer
from src.portfolio._confidence import ConfidenceSignals
from src.portfolio._kelly import KellyPortfolioOptimizer
import dataclasses


class _StubCalibratedModel:
    """与えた確率列をそのまま返す較正モデルのスタブ。"""

    def __init__(self, probs):
        self._probs = np.asarray(probs)

    def predict_proba(self, x):
        return np.column_stack([1.0 - self._probs, self._probs])


def _build_X(race_id, rows):
    # rows: list of (umaban, wakuban, tansho_odds, feat)
    df = pd.DataFrame(
        [{ResultsCols.UMABAN: u, ResultsCols.WAKUBAN: w, ResultsCols.TANSHO_ODDS: o, "feat": f} for u, w, o, f in rows],
        index=[race_id] * len(rows),
    )
    return df


def test_end_to_end_selection_and_allocation():
    X = _build_X("r1", [(1, 1, 2.0, 0.1), (2, 2, 5.0, 0.2), (3, 3, 20.0, 0.3)])
    probs = [0.6, 0.25, 0.15]
    model = _StubCalibratedModel(probs)

    # 1) モデル較正勝率 + オッズのテーブル
    table = ExpectedValueScorePolicy.calc(model, X)
    assert PROB in table.columns and CURRENT_ODDS in table.columns

    # 2) オッズ供給（過去推定）
    provider = HistoricalOddsProvider.from_score_table(table, ResultsCols.UMABAN, CURRENT_ODDS)

    # 3) 期待値で全馬券種を選定
    thresholds = {
        BetType.TANSHO: 1.0,
        BetType.UMAREN: 1.0,
        BetType.SANRENPUKU: 1.0,
    }
    policy = ExpectedValueBetPolicy(provider, thresholds=thresholds)
    candidates = policy.select(table[[ResultsCols.UMABAN, PROB]])
    assert len(candidates) > 0
    assert all(c.expected_value > 1.0 for c in candidates)

    # 4) 確信度を付与
    scorer = CompositeConfidenceScorer()
    threshold_by_type = thresholds
    scored = [
        dataclasses.replace(
            c,
            confidence=scorer.score(ConfidenceSignals(ev_margin=c.expected_value - threshold_by_type[c.bet_type])),
        )
        for c in candidates
    ]
    assert all(0.0 <= c.confidence <= 1.0 for c in scored)

    # 5) ケリー配分（予算超過なし）
    optimizer = KellyPortfolioOptimizer(kelly_fraction_ratio=0.5, per_bet_cap_ratio=0.1, max_daily_ratio=1.0)
    allocated = optimizer.allocate(scored, bankroll=100000.0)
    total_stake = sum(c.stake for c in allocated)
    assert total_stake <= 100000.0
    assert all(c.stake >= 0 for c in allocated)


def test_default_thresholds_are_usable():
    # 既定閾値（KB 7.1）でも例外なく動作する
    X = _build_X("r1", [(1, 1, 3.0, 0.1), (2, 2, 4.0, 0.2)])
    model = _StubCalibratedModel([0.5, 0.5])
    table = ExpectedValueScorePolicy.calc(model, X)
    provider = HistoricalOddsProvider.from_score_table(table, ResultsCols.UMABAN, CURRENT_ODDS)
    th = BetThresholds()
    policy = ExpectedValueBetPolicy(
        provider,
        thresholds={BetType.TANSHO: th.TANSHO, BetType.UMAREN: th.UMAREN},
    )
    # 例外が出ないこと（候補数は0以上）
    assert isinstance(policy.select(table[[ResultsCols.UMABAN, PROB]]), list)
