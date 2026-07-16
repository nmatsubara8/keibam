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


def prepare_scoring(ai, featured_slice: pd.DataFrame, takeout: float | Mapping[str, float] = 0.2):
    """params 非依存の前計算（モデル予測 score_table + odds_provider）を1度だけ作る。

    score_table（各馬の勝率）と odds_provider は ev_threshold/temperature/prob_scale に依存しない
    ——パラメータは下流の judge でしか効かない——ので、**トライアル間で使い回す**ことでモデル予測の
    重複実行（最適化の主要ボトルネック）を排す。空スライスは None。
    """
    from src.constants._results_cols import ResultsCols
    from src.policies._odds_provider import HistoricalOddsProvider
    from src.policies._score_policy import CURRENT_ODDS
    from src.policies._score_policy import ExpectedValueScorePolicy

    if featured_slice is None:
        return None
    score_table = ai.calc_score(featured_slice, ExpectedValueScorePolicy)
    if score_table is None or len(score_table) == 0:      # 実データで空スライスなら score も空
        return None
    odds_provider = HistoricalOddsProvider.from_score_table(
        score_table, ResultsCols.UMABAN, CURRENT_ODDS, takeout=takeout
    )
    return {"score_table": score_table, "odds_provider": odds_provider}


def backtest_from_prepared(prepared, return_processor, bet_type: str, params) -> tuple[dict, pd.DataFrame]:
    """prepare_scoring の結果を使い 1(券種, params) をバックテスト（judge + 清算のみ・再スコアなし）。"""
    from src.policies._bet_policy import ExpectedValueBetPolicy
    from src.simulation._metrics import summarize_returns
    from src.simulation._simulator import Simulator

    if not prepared:
        return {}, None
    policy = ExpectedValueBetPolicy(
        prepared["odds_provider"],
        {bet_type: params.ev_threshold},
        bet_types=[bet_type],
        ev_max=params.ev_max,
        bet_type_params={bet_type: params},
    )
    # race_id は featured も払戻テーブル(DB復元)も str。Simulator/BettingTickets 側でも str に
    # 正規化して照合するため、ここも str に揃える。
    actions = {str(rid): bets for rid, bets in policy.judge(prepared["score_table"]).items()}
    per_race = Simulator(return_processor).calc_returns_per_race(actions)
    summary = summarize_returns(per_race) if (per_race is not None and not per_race.empty) else {}
    return summary, per_race


def backtest_bet_type(
    ai,
    featured_slice: pd.DataFrame,
    return_processor,
    bet_type: str,
    params,
    takeout: float | Mapping[str, float] = 0.2,
) -> tuple[dict, pd.DataFrame]:
    """1 券種・1 パラメータでバックテストし (summary, per_race) を返す（薄いラッパ・後方互換）。

    summary は summarize_returns 出力（return_rate / hit_rate / sharpe_ratio /
    n_bets / n_races / profit / max_drawdown 等）。賭けが成立しなければ空 dict。
    複数 params を同一スライスで評価するときは prepare_scoring + backtest_from_prepared を直接使い、
    モデル予測の再計算を避けること（optimize_* は内部でそうしている）。
    """
    prepared = prepare_scoring(ai, featured_slice, takeout)
    return backtest_from_prepared(prepared, return_processor, bet_type, params)


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
    prepared = prepare_scoring(ai, featured_slice, takeout)   # モデル予測は1度（grid 点間で再利用）
    results = []
    for ev_th, temp, scale in itertools.product(
        grid["ev_thresholds"], grid["temperatures"], grid.get("prob_scales", [1.0])
    ):
        params = BetTypeParams(ev_threshold=ev_th, temperature=temp, prob_scale=scale)
        summary, _ = backtest_from_prepared(prepared, return_processor, bet_type, params)
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


def default_bounds() -> dict:
    """Optuna(TPE) 探索の物理prior bounds（EV閾値 × 温度 × 確率較正）。grid より連続・広い。"""
    return {
        "ev_threshold": (1.0, 3.0),
        "temperature": (0.5, 2.0),
        "prob_scale": (0.5, 1.5),
    }


def time_split(featured_slice: pd.DataFrame, val_frac: float = 0.3) -> tuple[pd.DataFrame, pd.DataFrame]:
    """race_id を時系列順（YYYY…昇順）で train/val に分割（先=train, 後=val）。

    in-sample ROI 最適化は万馬券分散に過適合するため、train で最適化し val で汎化を確認する
    ための分割。race_id は日付を先頭に持ち文字列ソートで時系列になる。
    """
    ids = sorted({str(r) for r in featured_slice.index})
    if len(ids) < 2:
        return featured_slice, featured_slice.iloc[0:0]
    k = max(1, int(len(ids) * (1.0 - val_frac)))
    train_ids, val_ids = set(ids[:k]), set(ids[k:])
    idx = featured_slice.index.astype(str)
    return featured_slice[idx.isin(train_ids)], featured_slice[idx.isin(val_ids)]


def robust_metric(summary: dict, per_race: pd.DataFrame | None, objective: str) -> float:
    """万馬券分散に頑健な目的値（最大化対象）。summary 空なら -inf。

    - ``return_rate``         : 生回収率（参考・単一万馬券に過敏で過適合しやすい）。
    - ``sharpe_ratio``        : 分散調整済み（単一万馬券に鈍感）。
    - ``trimmed_return_rate`` : 最大払戻レース1本を除いた回収率（万馬券ガード・既定）。
    """
    if not summary:
        return float("-inf")
    if objective == "sharpe_ratio":
        v = summary.get("sharpe_ratio")
        return float(v) if v is not None else float("-inf")
    if objective == "trimmed_return_rate":
        if per_race is None or per_race.empty or "return_amount" not in per_race.columns:
            v = summary.get("return_rate")
            return float(v) if v is not None else float("-inf")
        pr = per_race
        if len(pr) > 1:                                   # 最大払戻レース1本を除外
            pr = pr.drop(index=pr["return_amount"].idxmax())
        tot = float(pr["bet_amount"].sum())
        return float(pr["return_amount"].sum() / tot) if tot > 0 else float("-inf")
    v = summary.get(objective)
    return float(v) if v is not None else float("-inf")


def optimize_bet_type_tpe(
    ai,
    featured_slice: pd.DataFrame,
    return_processor,
    bet_type: str,
    *,
    n_trials: int = 60,
    n_jobs: int = 1,
    bounds: dict | None = None,
    objective: str = "trimmed_return_rate",
    min_bets: int = 30,
    val_frac: float = 0.3,
    max_races: int | None = None,
    takeout: float | Mapping[str, float] = 0.2,
    seed: int = 0,
) -> dict:
    """1 券種を Optuna(TPE) で最適化する（時系列 train/val・頑健目的・prob_scale 連続探索）。

    規律: in-sample の生 ROI 最大化は万馬券に過適合するため、**train で頑健目的を最大化 →
    best を val で汎化確認**し、さらに **default params の val も併記**する。判定は
    「val_metric(最適化) が val_metric_default(既定) を out-of-sample で上回るか」で行う。
    上回らなければ最適化は過適合＝採用しない、という negative も明示できる設計。

    Returns（best が min_bets を満たさないときは best_params=None）:
      {bet_type, best_params, objective, train_metric, val_metric, val_metric_default,
       train_summary, val_summary, val_default_summary, n_trials, n_train_races, n_val_races}
    """
    import optuna

    from src.policies._bet_type_params import BetTypeParams
    from src.policies._bet_type_params import default_params

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    bd = bounds or default_bounds()
    # max_races: 探索コスト（特に3連系の組合せ爆発）を抑えるため直近 max_races レースに限定。
    if max_races and featured_slice is not None and not featured_slice.empty:
        keep = set(sorted({str(r) for r in featured_slice.index})[-max_races:])
        featured_slice = featured_slice[featured_slice.index.astype(str).isin(keep)]
    train, val = time_split(featured_slice, val_frac)
    n_train = len(set(train.index.astype(str)))
    n_val = len(set(val.index.astype(str)))
    # モデル予測(calc_score)は params 非依存 → train/val ごとに1度だけ前計算し全トライアルで再利用。
    # これが最適化の主要ボトルネック（旧: 80トライアル×再スコア）の構造的解消。
    prep_train = prepare_scoring(ai, train, takeout)
    prep_val = prepare_scoring(ai, val, takeout)

    def _eval_prepared(prepared, params):
        return backtest_from_prepared(prepared, return_processor, bet_type, params)

    def _obj(trial):
        params = BetTypeParams(
            ev_threshold=trial.suggest_float("ev_threshold", *bd["ev_threshold"]),
            temperature=trial.suggest_float("temperature", *bd["temperature"]),
            prob_scale=trial.suggest_float("prob_scale", *bd["prob_scale"]),
        )
        summary, per_race = _eval_prepared(prep_train, params)
        if not summary or summary.get("n_bets", 0) < min_bets:
            return -1e9                                   # 賭け不足は強く忌避
        return robust_metric(summary, per_race, objective)

    study = optuna.create_study(
        direction="maximize", sampler=optuna.samplers.TPESampler(seed=seed))
    # n_jobs>1 でトライアルを並列実行（judge は pandas/numpy 主体で GIL を要所で手放すため、
    # 遊休コアを使って高速化。prep_*/return_processor は read-only 共有・Simulator は各評価で新規）。
    study.optimize(_obj, n_trials=n_trials, n_jobs=n_jobs, show_progress_bar=False)

    base = {"bet_type": bet_type, "objective": objective, "n_trials": n_trials,
            "n_train_races": n_train, "n_val_races": n_val}
    if not study.best_trial or study.best_value <= -1e9:
        return {**base, "best_params": None}              # min_bets を満たす点が無かった

    bp = BetTypeParams(
        ev_threshold=study.best_params["ev_threshold"],
        temperature=study.best_params["temperature"],
        prob_scale=study.best_params["prob_scale"],
    )
    tr_s, tr_pr = _eval_prepared(prep_train, bp)
    va_s, va_pr = _eval_prepared(prep_val, bp)
    vd_s, vd_pr = _eval_prepared(prep_val, default_params(bet_type))
    return {
        **base,
        "best_params": bp,
        "train_metric": robust_metric(tr_s, tr_pr, objective),
        "val_metric": robust_metric(va_s, va_pr, objective),
        "val_metric_default": robust_metric(vd_s, vd_pr, objective),
        "train_n_bets": int(tr_s.get("n_bets", 0)),
        "val_n_bets": int(va_s.get("n_bets", 0)),        # val 買い目数（希薄なら val 指標は不信）
        "val_default_n_bets": int(vd_s.get("n_bets", 0)),
        "train_summary": tr_s,
        "val_summary": va_s,
        "val_default_summary": vd_s,
    }


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
            "sharpe_nominal": nom.get("sharpe_ratio"),
            "n_calibrated": cal.get("n_bets", 0),
            "return_calibrated": r_cal,
            "hit_calibrated": cal.get("hit_rate"),
            "sharpe_calibrated": cal.get("sharpe_ratio"),
            "delta_return": (
                (r_cal - r_nom) if (r_nom is not None and r_cal is not None) else None
            ),
        })
    return pd.DataFrame(rows)
