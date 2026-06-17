"""XGBoost / CatBoost 向け Optuna ハイパーパラメータチューニング。

_model_wrapper.py の __tune_custom パターンに倣い、
各モデルに対して TPE ベースの study を実行して最良パラメータを返す。
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_XGB_LOG_PARAMS = {"learning_rate", "reg_alpha", "reg_lambda"}
_XGB_INT_PARAMS = {"n_estimators", "max_depth", "min_child_weight"}
_CAT_LOG_PARAMS = {"learning_rate", "l2_leaf_reg", "random_strength"}
_CAT_INT_PARAMS = {"iterations", "depth"}


def _suggest(trial, name, bounds, log_params, int_params):
    lo, hi = bounds
    if name in int_params:
        return trial.suggest_int(name, int(lo), int(hi))
    if name in log_params:
        return trial.suggest_float(name, lo, hi, log=True)
    return trial.suggest_float(name, lo, hi)


def _objective(trial, model_name, x_tr, y_tr, x_val, y_val, search_space, scale_pos_weight):
    import optuna

    if model_name == "xgboost":
        try:
            import xgboost as xgb
        except ImportError:
            raise optuna.TrialPruned()

        params = {
            k: _suggest(trial, k, v, _XGB_LOG_PARAMS, _XGB_INT_PARAMS)
            for k, v in search_space.items()
        }
        params["objective"] = "binary:logistic"
        params["eval_metric"] = "logloss"
        params["tree_method"] = "hist"
        params["seed"] = 100
        params["scale_pos_weight"] = scale_pos_weight

        model = xgb.XGBClassifier(**params)
        model.fit(
            x_tr, y_tr,
            eval_set=[(x_val, y_val)],
            early_stopping_rounds=50,
            verbose=False,
        )
        preds = model.predict_proba(x_val)[:, 1]

    elif model_name == "catboost":
        try:
            from catboost import CatBoostClassifier
        except ImportError:
            raise optuna.TrialPruned()

        params = {
            k: _suggest(trial, k, v, _CAT_LOG_PARAMS, _CAT_INT_PARAMS)
            for k, v in search_space.items()
        }
        params["verbose"] = 0
        params["random_seed"] = 100
        params["class_weights"] = [1.0, scale_pos_weight]

        model = CatBoostClassifier(**params)
        model.fit(
            x_tr, y_tr,
            eval_set=(x_val, y_val),
            early_stopping_rounds=50,
        )
        preds = model.predict_proba(x_val)[:, 1]

    else:
        raise ValueError(f"未対応のモデル名: {model_name}")

    from sklearn.metrics import log_loss
    return log_loss(y_val, preds)


def tune_model(
    model_name: str,
    x_tr,
    y_tr,
    x_val,
    y_val,
    search_space: dict,
    n_trials: int = 50,
    timeout: float | None = None,
    seed: int = 100,
    scale_pos_weight: float = 1.0,
) -> dict:
    """XGBoost または CatBoost の最良パラメータを Optuna で探索して返す。

    Parameters
    ----------
    model_name : "xgboost" | "catboost"
    x_tr, y_tr : 学習データ
    x_val, y_val : 検証データ（early stopping / objective 評価用）
    search_space : パラメータ名 → [lo, hi] の辞書
    n_trials : Optuna trial 数
    timeout : 秒単位のタイムアウト（None なら n_trials まで）
    seed : TPESampler のシード
    scale_pos_weight : クラス不均衡補正係数

    Returns
    -------
    dict : study.best_params
    """
    import optuna

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    sampler = optuna.samplers.TPESampler(seed=seed)
    study = optuna.create_study(direction="minimize", sampler=sampler)
    study.optimize(
        lambda trial: _objective(
            trial, model_name, x_tr, y_tr, x_val, y_val, search_space, scale_pos_weight
        ),
        n_trials=n_trials,
        timeout=timeout,
    )
    logger.info(
        "[tune_model] %s best_value=%.4f params=%s",
        model_name, study.best_value, study.best_params,
    )
    return study.best_params
