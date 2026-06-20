"""券種別パラメータの最適化（Phase 2 枠組み）。

単勝勝率モデル + Harville を土台に、券種ごとの EV 選定パラメータ
（EV 閾値 / 温度 β / 確率較正）を実払戻バックテストで最適化する。
Streamlit 依存は持たない（テスト可能な計算 + ファイル I/O は呼び出し側）。

目的関数: 較正勝率と単勝オッズから `HistoricalOddsProvider` で連系の推定オッズを
作り（過去の連オッズは遡及取得不可のため）、`ExpectedValueBetPolicy` で券種別に
選定 → `Simulator` で実払戻清算 → 回収率 / シャープを最大化する。
"""

from __future__ import annotations

import itertools
import logging
from typing import Mapping

import pandas as pd

logger = logging.getLogger(__name__)

# UI 表示用ラベル（最適化対象券種）
BET_TYPE_LABELS = {
    "tansho": "単勝",
    "fukusho": "複勝",
    "umaren": "馬連",
    "umatan": "馬単",
    "wide": "ワイド",
    "sanrenpuku": "三連複",
    "sanrentan": "三連単",
}


def default_grid() -> dict:
    """EV 閾値 × 温度 × 確率較正の既定探索グリッド。"""
    return {
        "ev_thresholds": [1.0, 1.2, 1.5, 1.8, 2.2],
        "temperatures": [0.7, 1.0, 1.3, 1.6],
        "prob_scales": [1.0],
    }


def backtest_bet_type(
    ai,
    featured_slice: pd.DataFrame,
    return_processor,
    bet_type: str,
    params,
    takeout: float | Mapping[str, float] = 0.2,
) -> tuple[dict, pd.DataFrame]:
    """1 券種・1 パラメータでバックテストし (summary, per_race) を返す。

    summary は summarize_returns 出力（return_rate / hit_rate / sharpe_ratio /
    n_bets / n_races / profit / max_drawdown 等）。賭けが成立しなければ空 dict。
    """
    from src.policies._bet_policy import ExpectedValueBetPolicy
    from src.policies._odds_provider import HistoricalOddsProvider
    from src.policies._score_policy import CURRENT_ODDS
    from src.policies._score_policy import ExpectedValueScorePolicy
    from src.constants._results_cols import ResultsCols
    from src.simulation._simulator import Simulator

    score_table = ai.calc_score(featured_slice, ExpectedValueScorePolicy)
    odds_provider = HistoricalOddsProvider.from_score_table(
        score_table, ResultsCols.UMABAN, CURRENT_ODDS, takeout=takeout
    )
    policy = ExpectedValueBetPolicy(
        odds_provider,
        {bet_type: params.ev_threshold},
        bet_types=[bet_type],
        ev_max=params.ev_max,
        bet_type_params={bet_type: params},
    )
    actions = policy.judge(score_table)
    # race_id は featured も払戻テーブル(DB復元)も str。Simulator/BettingTickets 側でも
    # str に正規化して照合するため、ここも str に揃える。
    actions = {str(race_id): bets for race_id, bets in actions.items()}

    simulator = Simulator(return_processor)
    per_race = simulator.calc_returns_per_race(actions)
    summary = simulator.calc_returns(actions)
    return summary, per_race


def optimize_bet_type(
    ai,
    featured_slice: pd.DataFrame,
    return_processor,
    bet_type: str,
    *,
    grid: dict | None = None,
    objective: str = "return_rate",
    min_bets: int = 10,
    takeout: float | Mapping[str, float] = 0.2,
) -> dict:
    """1 券種のパラメータをグリッド探索で最適化する。

    Returns
    -------
    {
      "bet_type": str,
      "best_params": BetTypeParams | None,   # min_bets を満たす中で objective 最大
      "best_summary": dict,
      "results": list[{params, summary}],    # 全グリッド点（賭け成立のもの）
      "objective": str,
    }
    best_params が None のときは min_bets を満たす組合せが無かったことを示す。
    """
    from src.policies._bet_type_params import BetTypeParams

    grid = grid or default_grid()
    results = []
    for ev_th, temp, scale in itertools.product(
        grid["ev_thresholds"], grid["temperatures"], grid.get("prob_scales", [1.0])
    ):
        params = BetTypeParams(ev_threshold=ev_th, temperature=temp, prob_scale=scale)
        summary, _ = backtest_bet_type(ai, featured_slice, return_processor, bet_type, params, takeout)
        if not summary:
            continue
        results.append({"params": params, "summary": summary})

    eligible = [r for r in results if r["summary"].get("n_bets", 0) >= min_bets]
    pool = eligible or []
    best = max(pool, key=lambda r: r["summary"].get(objective, float("-inf"))) if pool else None

    return {
        "bet_type": bet_type,
        "best_params": best["params"] if best else None,
        "best_summary": best["summary"] if best else {},
        "results": results,
        "objective": objective,
    }


def optimize_all(
    ai,
    featured_slice: pd.DataFrame,
    return_processor,
    *,
    bet_types=None,
    grid: dict | None = None,
    objective: str = "return_rate",
    min_bets: int = 10,
    takeout: float | Mapping[str, float] = 0.2,
) -> tuple[dict, dict, dict]:
    """複数券種をまとめて最適化する。

    Returns
    -------
    (params_map, metrics_map, all_results) :
        params_map  — {券種: BetTypeParams}（best が無い券種は既定値）。
        metrics_map — {券種: best_summary}。
        all_results — {券種: optimize_bet_type の戻り値全体}。
    """
    from src.policies._bet_type_params import OPTIMIZABLE_BET_TYPES
    from src.policies._bet_type_params import default_params

    targets = list(bet_types) if bet_types is not None else list(OPTIMIZABLE_BET_TYPES)
    params_map: dict = {}
    metrics_map: dict = {}
    all_results: dict = {}
    for bt in targets:
        res = optimize_bet_type(
            ai, featured_slice, return_processor, bt,
            grid=grid, objective=objective, min_bets=min_bets, takeout=takeout,
        )
        all_results[bt] = res
        params_map[bt] = res["best_params"] or default_params(bt)
        metrics_map[bt] = res["best_summary"]
    return params_map, metrics_map, all_results


def results_to_frame(optimize_result: dict) -> pd.DataFrame:
    """optimize_bet_type の results を比較表に整形する（objective 降順）。"""
    rows = []
    for r in optimize_result.get("results", []):
        p = r["params"]
        s = r["summary"]
        rows.append({
            "ev_threshold": p.ev_threshold,
            "temperature": p.temperature,
            "prob_scale": p.prob_scale,
            "return_rate": s.get("return_rate"),
            "hit_rate": s.get("hit_rate"),
            "sharpe_ratio": s.get("sharpe_ratio"),
            "n_bets": s.get("n_bets"),
            "n_races": s.get("n_races"),
            "profit": s.get("profit"),
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    obj = optimize_result.get("objective", "return_rate")
    sort_col = obj if obj in df.columns else "return_rate"
    return df.sort_values(sort_col, ascending=False).reset_index(drop=True)


def compare_calibration_backtest(
    ai,
    featured_slice: pd.DataFrame,
    return_processor,
    calibrated_takeout: Mapping[str, float],
    *,
    bet_types=None,
    params_map: Mapping | None = None,
    nominal_takeout: float = 0.2,
    ev_threshold: float | None = None,
) -> pd.DataFrame:
    """券種ごとに「公称控除率」vs「較正済み控除率」のバックテストを比較する。

    控除率は連系推定オッズ（HistoricalOddsProvider）にのみ効くため、同じ EV 閾値でも
    選定される買い目が変わる（較正で順序系の推定オッズが下がる→EV 低下→買い目が絞られる）。
    実払戻での回収率がどう変わるかを A/B 比較する。単勝は実オッズ直returnで控除率の影響を
    受けないため両者同値になる。

    ev_threshold を渡すと全券種の EV 閾値をその値で上書きする（既定の BetThresholds は
    tansho 1.78〜sanrentan 10.0 と高く、較正済みモデルでは買い目が 0 になりやすいため、
    較正の効果を観察するには低めの閾値で買い目を発生させる必要がある）。

    Returns
    -------
    DataFrame[bet_type, n_nominal, return_nominal, hit_nominal,
              n_calibrated, return_calibrated, hit_calibrated, delta_return]
    """
    import dataclasses

    from src.policies._bet_type_params import OPTIMIZABLE_BET_TYPES
    from src.policies._bet_type_params import default_params

    targets = list(bet_types) if bet_types is not None else list(OPTIMIZABLE_BET_TYPES)
    rows = []
    for bt in targets:
        params = (params_map or {}).get(bt) or default_params(bt)
        if ev_threshold is not None:
            params = dataclasses.replace(params, ev_threshold=ev_threshold)
        nom, _ = backtest_bet_type(ai, featured_slice, return_processor, bt, params, nominal_takeout)
        cal, _ = backtest_bet_type(ai, featured_slice, return_processor, bt, params, calibrated_takeout)
        r_nom = nom.get("return_rate")
        r_cal = cal.get("return_rate")
        rows.append({
            "bet_type": bt,
            "n_nominal": nom.get("n_bets", 0),
            "return_nominal": r_nom,
            "hit_nominal": nom.get("hit_rate"),
            "n_calibrated": cal.get("n_bets", 0),
            "return_calibrated": r_cal,
            "hit_calibrated": cal.get("hit_rate"),
            "delta_return": (
                (r_cal - r_nom) if (r_nom is not None and r_cal is not None) else None
            ),
        })
    return pd.DataFrame(rows)
