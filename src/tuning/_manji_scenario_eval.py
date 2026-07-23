"""①.5 Step4: シナリオ別に②を学習し OOS 回収率で選抜する（baseline/placebo 対照つき）。

流れ（シナリオ j ごと）:
  1. build_scenario_training_data で ①features ⊕ manji_score ⊕ 因子one-hot を得る
     （block_posteriors により前進安全。① は不変）。
  2. 発走日順の walk-forward で ② を学習 → 各馬の勝率 prob を予測。
  3. EV = prob × 単勝オッズ が閾値超の馬を単勝フラットで買い、OOS 回収率を集計。
  4. 対照:
     - baseline : manji 列（manji_score / manji_bkt_*）を除いた同一モデル。素の②回収率。
     - placebo  : manji_score を shuffle した config を R 回。改善が placebo を超えねばノイズ。
  5. lift = scenario 回収率 − baseline 回収率。placebo 分布での順位で有意性を見る。

② 学習器は predict_fn で差し替え可能（既定は軽量 LGBMClassifier）。KeibaAI の重い
スタッキング/較正を毎シナリオ回さず、manji_score の OOS 寄与だけを軽く測るのが狙い。
最良シナリオ確定後に、そのシナリオで本番 KeibaAI（較正込み）を1回学習する運用。

注意（名鑑の較正指針）: 妙味度は複勝回収率基準。単勝より複勝の方が分散が小さく妙味が出やすい。
本ハーネスは既定 単勝だが、payoffs を渡せば複勝決済に切替可能（settle_bet_type）。
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from src.constants._results_cols import ResultsCols

logger = logging.getLogger(__name__)

# 特徴量から必ず除外する列（目的変数・決済用オッズ・ID・日付・リーク列）。
_DROP_FEATURES = {
    ResultsCols.RANK, ResultsCols.TANSHO_ODDS, "rank", "rank_win", "date",
    "horse_id", "race_id", ResultsCols.CORNER if hasattr(ResultsCols, "CORNER") else "通過",
}
_MANJI_SCORE = "manji_score"


def numeric_feature_cols(df: pd.DataFrame, *, include_manji: bool = True) -> list[str]:
    """② に渡す数値特徴量列。manji_score / manji_bkt_* を含めるか選べる（baseline 用）。"""
    cols = []
    for c in df.columns:
        if c in _DROP_FEATURES:
            continue
        cs = str(c)
        if not include_manji and (
            c == _MANJI_SCORE or cs.startswith("manji_bkt_") or cs.startswith("manji_myoumido_")
        ):
            continue
        if pd.api.types.is_numeric_dtype(df[c]):
            cols.append(c)
    return cols


def _default_lgb_predict(train: pd.DataFrame, test: pd.DataFrame,
                         feature_cols: list[str], seed: int) -> np.ndarray:
    """軽量 LGBMClassifier で test の勝率を予測（既定の predict_fn）。"""
    import lightgbm as lgb

    y = (pd.to_numeric(train[ResultsCols.RANK], errors="coerce") == 1).astype(int).to_numpy()
    Xtr = train[feature_cols].apply(pd.to_numeric, errors="coerce").to_numpy()
    Xte = test[feature_cols].apply(pd.to_numeric, errors="coerce").to_numpy()
    if len(np.unique(y)) < 2:
        return np.full(len(test), float(y.mean()) if len(y) else 0.0)
    model = lgb.LGBMClassifier(
        n_estimators=100, num_leaves=15, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8, random_state=seed, verbose=-1,
    )
    model.fit(Xtr, y)
    return model.predict_proba(Xte)[:, 1]


def _date_folds(df: pd.DataFrame, folds: int) -> list[np.ndarray]:
    """発走日順にレース単位で folds 分割した行マスク（index=race_id 非ユニーク対応）。"""
    race_date = pd.to_datetime(df["date"], errors="coerce").groupby(level=0).first().sort_values()
    order = list(race_date.index)
    n = len(order)
    bounds = [round(i * n / folds) for i in range(folds + 1)]
    rid = df.index.astype(str).to_numpy()
    out = []
    for i in range(folds):
        rids = order[bounds[i]:bounds[i + 1]]
        out.append(np.isin(rid, np.array([str(r) for r in rids])))
    return out


def _settle(test: pd.DataFrame, prob: np.ndarray, ev_threshold: float,
            payoffs: dict | None = None) -> tuple[float, float, int, int]:
    """EV=prob×単勝オッズ が閾値超の馬を買って (stake, ret, n_bets, hit) を返す。

    payoffs=None は単勝フラット100円（着1で100×単勝）。payoffs 指定時は
    {(race_id,馬番): 払戻円}（複勝等）で決済。
    """
    odds = pd.to_numeric(test[ResultsCols.TANSHO_ODDS], errors="coerce").to_numpy()
    rank = pd.to_numeric(test[ResultsCols.RANK], errors="coerce").to_numpy()
    uma = pd.to_numeric(test[ResultsCols.UMABAN], errors="coerce").to_numpy()
    rid = test.index.astype(str).to_numpy()
    ev = prob * np.where(np.isfinite(odds), odds, 0.0)
    bet = np.isfinite(odds) & (odds > 0) & (ev > ev_threshold)
    stake = ret = 0.0
    n_bets = hit = 0
    for i in np.flatnonzero(bet):
        n_bets += 1
        stake += 100.0
        if payoffs is None:
            if rank[i] == 1:
                ret += 100.0 * float(odds[i])
                hit += 1
        else:
            pay = payoffs.get((rid[i], int(uma[i]))) if np.isfinite(uma[i]) else None
            if pay is not None:
                ret += float(pay)
                hit += 1
    return stake, ret, n_bets, hit


def walk_forward_roi(
    df: pd.DataFrame,
    *,
    feature_cols: list[str],
    folds: int = 5,
    ev_threshold: float = 1.0,
    predict_fn=_default_lgb_predict,
    payoffs: dict | None = None,
    seed: int = 0,
) -> dict:
    """発走日順 walk-forward で ② を学習し OOS 単勝回収率を返す。

    Returns: {roi, stake, ret, n_bets, hit}
    """
    masks = _date_folds(df, folds)
    stake = ret = 0.0
    n_bets = hit = 0
    for k in range(1, folds):
        train_mask = np.logical_or.reduce(masks[:k])
        test_mask = masks[k]
        if not train_mask.any() or not test_mask.any():
            continue
        train, test = df[train_mask], df[test_mask]
        prob = predict_fn(train, test, feature_cols, seed)
        s, r, nb, h = _settle(test, np.asarray(prob, dtype=float), ev_threshold, payoffs)
        stake += s
        ret += r
        n_bets += nb
        hit += h
    roi = ret / stake if stake else 0.0
    return {"roi": roi, "stake": stake, "ret": ret, "n_bets": n_bets, "hit": hit}


def posterior_ready(block_posteriors, *, min_adopted: int = 20) -> bool:
    """①.5 のベイズ更新が「十分に進んだ」か（最新ブロックの採用バケット数で判定）。

    ② の Optuna は事後が十分に貯まってから回す。最新（最も証拠が多い）ブロックの採用
    (factor,bucket) 総数が min_adopted 以上なら ready。
    """
    if not block_posteriors:
        return False
    _, pts = block_posteriors[-1]
    n_adopted = sum(len(bmap) for bmap in pts.values())
    return n_adopted >= min_adopted


def default_lgb_search_space() -> dict:
    """② LightGBM の Optuna 探索空間（回収率向けの穏当な範囲）。"""
    return {
        "learning_rate": ("loguniform", 0.01, 0.2),
        "num_leaves": ("int", 15, 127),
        "max_depth": ("int", 3, 12),
        "min_child_samples": ("int", 10, 200),
        "feature_fraction": ("uniform", 0.5, 1.0),
        "bagging_fraction": ("uniform", 0.5, 1.0),
        "lambda_l1": ("loguniform", 1e-3, 10.0),
        "lambda_l2": ("loguniform", 1e-3, 10.0),
    }


def _suggest_lgb(trial, space: dict) -> dict:
    out = {}
    for name, spec in space.items():
        kind = spec[0]
        if kind == "int":
            out[name] = trial.suggest_int(name, int(spec[1]), int(spec[2]))
        elif kind == "loguniform":
            out[name] = trial.suggest_float(name, float(spec[1]), float(spec[2]), log=True)
        else:  # uniform
            out[name] = trial.suggest_float(name, float(spec[1]), float(spec[2]))
    return out


def make_lgb_predict(params: dict, *, early_stopping_rounds: int = 50,
                     valid_frac: float = 0.2, num_boost_round: int = 1000):
    """指定ハイパーパラメータで学習する predict_fn を作る（**早期終了つき**）。

    train を発走日順に inner/valid 分割し、valid の binary_logloss が改善しなくなったら
    early_stopping_rounds で boosting を打ち切る（＝Early termination）。
    """
    import lightgbm as lgb

    def predict_fn(train, test, feature_cols, seed):
        y = (pd.to_numeric(train[ResultsCols.RANK], errors="coerce") == 1).astype(int).to_numpy()
        if len(np.unique(y)) < 2:
            return np.full(len(test), float(y.mean()) if len(y) else 0.0)
        order = np.argsort(pd.to_datetime(train["date"], errors="coerce").to_numpy(), kind="stable")
        cut = max(1, int(len(order) * (1.0 - valid_frac)))
        tr_idx, va_idx = order[:cut], order[cut:]
        Xall = train[feature_cols].apply(pd.to_numeric, errors="coerce")
        dtrain = lgb.Dataset(Xall.iloc[tr_idx].to_numpy(), label=y[tr_idx])
        p = {**params, "objective": "binary", "verbose": -1, "seed": int(seed),
             "feature_pre_filter": False}
        callbacks, valid_sets = [], None
        if len(va_idx) > 0 and len(np.unique(y[va_idx])) > 0:
            dvalid = lgb.Dataset(Xall.iloc[va_idx].to_numpy(), label=y[va_idx])
            valid_sets = [dvalid]
            callbacks = [lgb.early_stopping(early_stopping_rounds, verbose=False),
                         lgb.log_evaluation(0)]
        booster = lgb.train(p, dtrain, num_boost_round=num_boost_round,
                            valid_sets=valid_sets, callbacks=callbacks)
        Xte = test[feature_cols].apply(pd.to_numeric, errors="coerce").to_numpy()
        return booster.predict(Xte)

    return predict_fn


def tune_lgb_optuna(
    scenario_df: pd.DataFrame,
    *,
    feature_cols: list[str] | None = None,
    n_trials: int = 100,
    early_stopping_rounds: int = 50,
    num_boost_round: int = 1000,
    valid_frac: float = 0.2,
    folds: int = 5,
    ev_threshold: float = 1.0,
    payoffs: dict | None = None,
    search_space: dict | None = None,
    seed: int = 0,
) -> dict:
    """①.5 妙味度列つきデータで ② の LightGBM ハイパーパラメータを Optuna 探索する。

    - n_trials 回（既定100=『100epoch』）。各 trial は LightGBM 早期停止つきで学習。
    - **Early termination 2段**: (1) 各 trial 内で LightGBM early_stopping（boosting 打切り）、
      (2) Optuna MedianPruner で見込みの薄い trial を fold 途中で枝刈り。
    - 目的 = 発走日順 walk-forward の OOS 回収率（最大化）。前進安全は①.5事後＋fold分割が担う。

    Returns: {best_params, value(OOS回収率), n_trials, n_pruned, predict_fn}
    """
    import optuna

    feature_cols = feature_cols or numeric_feature_cols(scenario_df, include_manji=True)
    space = search_space or default_lgb_search_space()
    masks = _date_folds(scenario_df, folds)

    def objective(trial):
        params = _suggest_lgb(trial, space)
        pf = make_lgb_predict(params, early_stopping_rounds=early_stopping_rounds,
                              valid_frac=valid_frac, num_boost_round=num_boost_round)
        stake = ret = 0.0
        for step, k in enumerate(range(1, folds)):
            train_mask = np.logical_or.reduce(masks[:k])
            test_mask = masks[k]
            if not train_mask.any() or not test_mask.any():
                continue
            prob = pf(scenario_df[train_mask], scenario_df[test_mask], feature_cols, seed)
            s, r, _, _ = _settle(scenario_df[test_mask], np.asarray(prob, dtype=float),
                                 ev_threshold, payoffs)
            stake += s
            ret += r
            roi = ret / stake if stake else 0.0
            trial.report(roi, step)             # fold 途中経過を Optuna に報告
            if trial.should_prune():             # 見込み薄なら早期終了（枝刈り）
                raise optuna.TrialPruned()
        return ret / stake if stake else 0.0

    study = optuna.create_study(
        direction="maximize",
        sampler=optuna.samplers.TPESampler(seed=seed),
        pruner=optuna.pruners.MedianPruner(n_warmup_steps=1, n_startup_trials=5),
    )
    study.optimize(objective, n_trials=n_trials)

    completed = [t for t in study.trials if t.state.name == "COMPLETE"]
    n_pruned = sum(1 for t in study.trials if t.state.name == "PRUNED")
    best_params = dict(study.best_params) if completed else {}
    value = float(study.best_value) if completed else 0.0
    predict_fn = make_lgb_predict(best_params, early_stopping_rounds=early_stopping_rounds,
                                  valid_frac=valid_frac, num_boost_round=num_boost_round) \
        if best_params else _default_lgb_predict
    return {"best_params": best_params, "value": value, "n_trials": n_trials,
            "n_pruned": n_pruned, "predict_fn": predict_fn}


def evaluate_scenario(
    scenario_df: pd.DataFrame,
    *,
    folds: int = 5,
    ev_threshold: float = 1.0,
    predict_fn=_default_lgb_predict,
    payoffs: dict | None = None,
    n_placebo: int = 20,
    seed: int = 0,
) -> dict:
    """1 シナリオの学習データを OOS 評価（scenario / baseline / placebo）。

    Returns: {roi, baseline_roi, lift, placebo_pct, n_bets, ...}
    """
    feat_full = numeric_feature_cols(scenario_df, include_manji=True)
    feat_base = numeric_feature_cols(scenario_df, include_manji=False)

    full = walk_forward_roi(scenario_df, feature_cols=feat_full, folds=folds,
                            ev_threshold=ev_threshold, predict_fn=predict_fn,
                            payoffs=payoffs, seed=seed)
    base = walk_forward_roi(scenario_df, feature_cols=feat_base, folds=folds,
                            ev_threshold=ev_threshold, predict_fn=predict_fn,
                            payoffs=payoffs, seed=seed)
    lift = full["roi"] - base["roi"]

    # placebo: manji_score を shuffle（因子構造は同じ、寄与の有無だけ壊す）
    placebo_lifts = []
    if n_placebo and _MANJI_SCORE in scenario_df.columns:
        rng = np.random.default_rng(seed)
        for _ in range(n_placebo):
            perm = scenario_df.copy()
            perm[_MANJI_SCORE] = rng.permutation(perm[_MANJI_SCORE].to_numpy())
            pf = walk_forward_roi(perm, feature_cols=feat_full, folds=folds,
                                  ev_threshold=ev_threshold, predict_fn=predict_fn,
                                  payoffs=payoffs, seed=seed)
            placebo_lifts.append(pf["roi"] - base["roi"])
    placebo_lifts = np.array(placebo_lifts) if placebo_lifts else np.array([])
    placebo_pct = float((placebo_lifts < lift).mean()) if placebo_lifts.size else float("nan")

    return {
        "roi": full["roi"], "baseline_roi": base["roi"], "lift": lift,
        "placebo_pct": placebo_pct, "placebo_median": (float(np.median(placebo_lifts))
                                                        if placebo_lifts.size else float("nan")),
        "n_bets": full["n_bets"], "hit": full["hit"], "stake": full["stake"], "ret": full["ret"],
    }


def evaluate_scenarios(
    featured: pd.DataFrame,
    scenario_names: list[str] | None = None,
    *,
    shared=None,
    n_blocks: int = 8,
    folds: int = 5,
    ev_threshold: float = 1.0,
    predict_fn=_default_lgb_predict,
    payoffs: dict | None = None,
    n_placebo: int = 20,
    cfg=None,
    factor_half_life: dict | None = None,
    seed: int = 0,
) -> pd.DataFrame:
    """複数シナリオを OOS 評価し lift 降順で返す（最良シナリオ選抜）。

    共有成果物（factor_table・block_posteriors）は1回だけ作って全シナリオで使い回す。
    """
    from src.tuning._manji_scenario import (
        SCENARIOS,
        build_scenario_training_data,
        prepare_shared,
        scenario_factor_union,
    )

    scenario_names = scenario_names or list(SCENARIOS)
    if shared is None:
        shared = prepare_shared(featured, factor_names=scenario_factor_union(scenario_names),
                                n_blocks=n_blocks, cfg=cfg, factor_half_life=factor_half_life)
    factor_table, block_posteriors = shared

    rows = []
    for name in scenario_names:
        scn = SCENARIOS[name]
        sdf = build_scenario_training_data(
            featured, scn, factor_table=factor_table, block_posteriors=block_posteriors,
        )
        res = evaluate_scenario(sdf, folds=folds, ev_threshold=ev_threshold,
                                predict_fn=predict_fn, payoffs=payoffs,
                                n_placebo=n_placebo, seed=seed)
        res["scenario"] = name
        rows.append(res)
        logger.info("[scenario-eval] %-14s roi=%.3f base=%.3f lift=%+.3f placebo%%=%.2f n=%d",
                    name, res["roi"], res["baseline_roi"], res["lift"],
                    res["placebo_pct"], res["n_bets"])
    cols = ["scenario", "roi", "baseline_roi", "lift", "placebo_pct", "placebo_median",
            "n_bets", "hit", "stake", "ret"]
    out = pd.DataFrame(rows)[cols].sort_values("lift", ascending=False).reset_index(drop=True)
    return out
