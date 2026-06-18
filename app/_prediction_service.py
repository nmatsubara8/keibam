"""予測パイプラインの UI ブリッジ。

model → EV 選定 → 確信度 → ケリー配分 の一連の処理を
Streamlit ページから呼び出せる単一関数に集約する。
純粋に domain 層を呼び出すだけのため、単体テスト可能。
"""

from __future__ import annotations

import dataclasses

import pandas as pd

from src.constants._bet_thresholds import BetThresholds
from src.constants._bet_types import BetType
from src.constants._results_cols import ResultsCols
from src.operation._config import OperationConfig
from src.policies._bet_policy import ExpectedValueBetPolicy
from src.policies._bet_candidate import BetCandidate
from src.policies._odds_provider import HistoricalOddsProvider
from src.policies._score_policy import CURRENT_ODDS
from src.policies._score_policy import PROB
from src.policies._score_policy import ExpectedValueScorePolicy
from src.portfolio._confidence import CompositeConfidenceScorer
from src.portfolio._confidence import ConfidenceSignals
from src.portfolio._kelly import KellyPortfolioOptimizer


def default_thresholds() -> dict:
    """BetThresholds から全馬券種の EV 閾値 dict を返す。"""
    th = BetThresholds()
    return {
        BetType.TANSHO: th.TANSHO,
        BetType.FUKUSHO: th.FUKUSHO,
        BetType.UMAREN: th.UMAREN,
        BetType.UMATAN: th.UMATAN,
        BetType.WIDE: th.WIDE,
        BetType.SANRENPUKU: th.SANRENPUKU,
        BetType.SANRENTAN: th.SANRENTAN,
    }


def run_prediction(
    model,
    X: pd.DataFrame,
    op_config: OperationConfig,
    thresholds: dict | None = None,
    bet_type_params: dict | None = None,
) -> list[BetCandidate]:
    """EV 選定 → 確信度付与 → ケリー配分の全パイプラインを実行する。

    Parameters
    ----------
    model : predict_proba(X) → ndarray を持つ学習済みモデル。
    X : 対象レースの特徴量 DataFrame（race_id インデックス、TANSHO_ODDS 含む）。
    op_config : 資金・ケリー設定。
    thresholds : 馬券種 → EV 閾値（省略時は BetThresholds の既定値）。
    bet_type_params : 券種別最適化パラメータ {券種: BetTypeParams}（省略可）。
        指定券種は温度・確率較正・EV 閾値/上限を上書きする（Phase 2 最適化結果の反映）。

    Returns
    -------
    list[BetCandidate] : stake が設定された配分済み候補（EV 降順）。
    """
    thresholds = thresholds or default_thresholds()

    # 1. 較正勝率 + 現在オッズのテーブル
    table = ExpectedValueScorePolicy.calc(model, X)

    # 2. オッズ供給。既定は現在オッズ（歴史推定）。use_predicted_odds=True かつ
    #    odds_watch の最新予測（オッズ力学アンサンブル）が存在する場合は、
    #    予測確定オッズで EV を計算する（予測の無い馬は現在オッズへフォールバック）。
    from src.policies._odds_provider import AbstractOddsProvider

    provider: AbstractOddsProvider = HistoricalOddsProvider.from_score_table(
        table, ResultsCols.UMABAN, CURRENT_ODDS
    )
    if getattr(op_config, "use_predicted_odds", False):
        try:
            from src.pipeline.odds_watch import latest_final_odds_lookup
            from src.pipeline.odds_watch import load_predictions
            from src.policies._odds_provider import PredictedOddsProvider

            lookup = latest_final_odds_lookup(load_predictions())
            if lookup:
                provider = PredictedOddsProvider(lookup, fallback=provider)
        except Exception:  # noqa: BLE001 — 予測読込失敗時は現在オッズで継続
            pass

    # 3. EV 選定（券種別最適化パラメータがあれば温度・較正・閾値を反映）
    policy = ExpectedValueBetPolicy(provider, thresholds=thresholds, bet_type_params=bet_type_params)
    candidates = policy.select(table[[ResultsCols.UMABAN, PROB]])

    if not candidates:
        return []

    # 4. 確信度付与
    scorer = CompositeConfidenceScorer()
    scored = [
        dataclasses.replace(
            c,
            confidence=scorer.score(ConfidenceSignals(ev_margin=c.expected_value - thresholds.get(c.bet_type, 1.0))),
        )
        for c in candidates
    ]

    # 5. フラクショナル・ケリー配分
    optimizer = KellyPortfolioOptimizer(
        kelly_fraction_ratio=op_config.kelly_fraction_ratio,
        per_bet_cap_ratio=op_config.per_bet_cap_ratio,
        max_daily_ratio=op_config.max_daily_ratio,
    )
    return optimizer.allocate(scored, bankroll=op_config.bankroll)
