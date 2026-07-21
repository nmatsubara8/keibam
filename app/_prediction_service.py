"""予測パイプラインの UI ブリッジ。

model → EV 選定 → 確信度 → ケリー配分 の一連の処理を
Streamlit ページから呼び出せる単一関数に集約する。
純粋に domain 層を呼び出すだけのため、単体テスト可能。
"""

from __future__ import annotations

import dataclasses

import pandas as pd

from src.constants._bet_types import BetType
from src.policies._thresholds import bet_threshold_map
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
    """馬券種の EV 閾値 dict を返す（constants の単一ソースへ委譲）。"""
    return bet_threshold_map()


def _load_live_takeout(takeout):
    """ライブ選定に使う控除率を決める。

    明示指定があればそれを使う。無ければ較正済みの券種別控除率
    （models/takeout_calibration.json）を読み込み、未較正なら既定 0.2。
    """
    if takeout is not None:
        return takeout
    from src.policies._takeout_calibration import latest_takeout_map
    from src.policies._takeout_calibration import takeout_calibration_path

    calib = latest_takeout_map(takeout_calibration_path("models"))
    return calib or 0.2


def _load_ev_artifacts(models_dir: str = "models"):
    """EV 較正アーティファクト（補正Harville/r̂較正/市場合成）を models/ から読み込む。

    calibrate-ev が保存した3つの JSON を読み、(place_exponents, win_calibrator,
    blend_weights) を返す。各ファイルが無ければ該当は None（= その補正を行わない）。
    op_config.use_ev_calibration（既定 True）が真のとき呼ぶ。ファイルが無ければ全 None で
    従来挙動へフォールバックするため、既定 ON でも未 fit 環境は無害。
    """
    from src.policies._blend import load_blend_weights
    from src.policies._calibration import load_calibrator
    from src.policies._harville import load_place_exponents
    from src.simulation._calibrate import blend_weights_path
    from src.simulation._calibrate import place_exponents_path
    from src.simulation._calibrate import win_calibrator_path

    return (
        load_place_exponents(place_exponents_path(models_dir)),
        load_calibrator(win_calibrator_path(models_dir)),
        load_blend_weights(blend_weights_path(models_dir)),
    )


def run_prediction(
    model,
    X: pd.DataFrame,
    op_config: OperationConfig,
    thresholds: dict | None = None,
    bet_type_params: dict | None = None,
    takeout=None,
    win_model=None,
    pool_by_race: dict | None = None,
) -> list[BetCandidate]:
    """EV 選定 → 確信度付与 → ケリー配分の全パイプラインを実行する。

    Parameters
    ----------
    model : predict_proba(X) → ndarray を持つ学習済みモデル（Place ヘッド=top3 予測）。
    X : 対象レースの特徴量 DataFrame（race_id インデックス、TANSHO_ODDS 含む）。
    win_model : Win ヘッド（1着予測）。指定すると連系の Harville に真の勝率を供給し、
        複勝は Place ヘッド(model)の top3 出力を直接使う（Stage B）。None なら
        従来通り model 単独（top3 出力を勝率代理に流用）。
    op_config : 資金・ケリー設定。
    thresholds : 馬券種 → EV 閾値（省略時は BetThresholds の既定値）。
    bet_type_params : 券種別最適化パラメータ {券種: BetTypeParams}（省略可）。
        指定券種は温度・確率較正・EV 閾値/上限を上書きする（Phase 2 最適化結果の反映）。
    takeout : 連系推定オッズの控除率（float または {券種: 控除率}）。省略時は
        較正済み控除率（calibrate-takeout の出力）を自動読込し、無ければ 0.2。
    pool_by_race : {race_id: 復元プール（≒出来高）}。op_config.use_pool_impact=True のとき、
        自己購入によるオッズ低下でケリー stake を上限する（芦谷/ベンター）。None なら影響なし。

    Returns
    -------
    list[BetCandidate] : stake が設定された配分済み候補（EV 降順）。
    """
    thresholds = thresholds or default_thresholds()
    # 検証済み戦略: 単勝EV下限を config で上書き（None=既定 BetThresholds=1.78）。
    if op_config.tansho_ev_threshold is not None:
        thresholds = {**thresholds, BetType.TANSHO: op_config.tansho_ev_threshold}
    takeout = _load_live_takeout(takeout)

    # 0. 無効オッズ馬の除外（発走前で単勝未確定・取消馬など）。単勝が欠損/非正の馬を残すと、
    #    Harville/連系が馬番キーで KeyError になり **レース全体の予測が落ちる**。betting 対象に
    #    ならない馬なので入口で除外し、残る馬だけで予測する。backtest（全馬に確定オッズ）では
    #    除外0＝無影響。残り<2頭なら選定不能で空を返す。
    if ResultsCols.TANSHO_ODDS in X.columns:
        _odds = pd.to_numeric(X[ResultsCols.TANSHO_ODDS], errors="coerce")
        _valid = _odds > 0
        if not bool(_valid.all()):
            X = X[_valid.to_numpy()]
    if len(X) < 2:
        return []

    # 1. 較正確率 + 現在オッズのテーブル（Place ヘッド=top3 予測）
    table = ExpectedValueScorePolicy.calc(model, X)
    # Stage B: Win ヘッドがあれば、連系の Harville に渡す「勝率」テーブルを別途作る。
    # 複勝は Place(table) の top3 出力を直接使い、連系は Win(win_table) を使う。
    win_table = ExpectedValueScorePolicy.calc(win_model, X) if win_model is not None else None

    # 2. オッズ供給。既定は現在オッズ（歴史推定）。use_predicted_odds=True かつ
    #    odds_watch の最新予測（オッズ力学アンサンブル）が存在する場合は、
    #    予測確定オッズで EV を計算する（予測の無い馬は現在オッズへフォールバック）。
    #    連系の推定オッズは較正済み控除率（takeout）を反映する。
    from src.policies._odds_provider import AbstractOddsProvider

    provider: AbstractOddsProvider = HistoricalOddsProvider.from_score_table(
        table, ResultsCols.UMABAN, CURRENT_ODDS, takeout=takeout
    )
    if getattr(op_config, "use_predicted_odds", False):
        try:
            from src.pipeline.odds_watch import latest_final_odds_lookup
            from src.pipeline.odds_watch import load_predictions
            from src.policies._odds_provider import PredictedOddsProvider

            lookup = latest_final_odds_lookup(load_predictions())
            if lookup:
                provider = PredictedOddsProvider(lookup, fallback=provider, takeout=takeout)
        except Exception:  # noqa: BLE001 — 予測読込失敗時は現在オッズで継続
            pass

    # 3. EV 選定（券種別最適化パラメータがあれば温度・較正・閾値を反映）
    #    use_ev_calibration（既定 True）なら calibrate-ev の OOS 較正物を適用（無い項目は None）。
    place_exponents = win_calibrator = blend_weights = None
    if getattr(op_config, "use_ev_calibration", True):
        place_exponents, win_calibrator, blend_weights = _load_ev_artifacts()
    # 初出走馬の公衆フォールバック（ベンター §3・opt-in）。featured から初出走集合を作る。
    unratable_fallback = getattr(op_config, "use_unratable_fallback", False)
    unratable_by_race = None
    if unratable_fallback:
        from src.policies._unratable import build_unratable_by_race

        unratable_by_race = build_unratable_by_race(X)
    policy = ExpectedValueBetPolicy(
        provider, thresholds=thresholds, bet_type_params=bet_type_params,
        place_exponents=place_exponents, win_calibrator=win_calibrator,
        blend_weights=blend_weights, unratable_fallback=unratable_fallback,
    )
    place_cols = table[[ResultsCols.UMABAN, PROB]]
    if win_table is not None:
        # 連系は Win ヘッドの勝率、複勝は Place ヘッドの top3 を直接使う
        candidates = policy.select(
            win_table[[ResultsCols.UMABAN, PROB]], place_prob_table=place_cols,
            unratable_by_race=unratable_by_race,
        )
    else:
        candidates = policy.select(place_cols, unratable_by_race=unratable_by_race)

    # 検証済み戦略: オッズ上限フィルタ（既定 inf=無効）。3–15倍にエッジが集中し、
    # 15倍超は -EV な人気薄ジャンクのため除外する。
    candidates = [c for c in candidates if c.odds <= op_config.max_odds]

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

    # 5. フラクショナル・ケリー配分（任意でプール影響＝自己購入のオッズ低下を反映）
    optimizer = KellyPortfolioOptimizer(
        kelly_fraction_ratio=op_config.kelly_fraction_ratio,
        per_bet_cap_ratio=op_config.per_bet_cap_ratio,
        max_daily_ratio=op_config.max_daily_ratio,
        pool_impact=getattr(op_config, "use_pool_impact", False),
    )
    return optimizer.allocate(scored, bankroll=op_config.bankroll, pool_by_race=pool_by_race)
