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
        except ImportError as e:
            raise optuna.TrialPruned() from e

        params = {
            k: _suggest(trial, k, v, _XGB_LOG_PARAMS, _XGB_INT_PARAMS)
            for k, v in search_space.items()
        }
        from src.training._gpu_config import xgb_gpu_params

        params["objective"] = "binary:logistic"
        params["eval_metric"] = "logloss"
        params["tree_method"] = "hist"
        params["seed"] = 100
        params["scale_pos_weight"] = scale_pos_weight
        params["early_stopping_rounds"] = 50  # XGBoost>=2.0 は __init__ で指定
        params.update(xgb_gpu_params())        # --gpu 時のみ device=cuda

        model = xgb.XGBClassifier(**params)
        model.fit(
            x_tr, y_tr,
            eval_set=[(x_val, y_val)],
            verbose=False,
        )
        preds = model.predict_proba(x_val)[:, 1]

    elif model_name == "catboost":
        try:
            from catboost import CatBoostClassifier
        except ImportError as e:
            raise optuna.TrialPruned() from e

        params = {
            k: _suggest(trial, k, v, _CAT_LOG_PARAMS, _CAT_INT_PARAMS)
            for k, v in search_space.items()
        }
        from src.training._gpu_config import catboost_gpu_params

        params["verbose"] = 0
        params["random_seed"] = 100
        params["class_weights"] = [1.0, scale_pos_weight]
        params.update(catboost_gpu_params())   # --gpu 時のみ task_type=GPU

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


def _suggest_nn_params(trial, search_space: dict) -> dict:
    """NN の構造・学習パラメータを 1 trial 分サンプリングする。

    search_space のキー（範囲指定）:
      lr [lo, hi] (log), dropout [lo, hi], batch_size [候補...],
      arch [候補...], pre_norm [候補...],
      n_layers [lo, hi], layer_width [候補...],   # mlp 用
      n_conv [lo, hi], conv_width [候補...], kernel_size [候補...]  # cnn 用
    """
    ss = search_space
    arch = trial.suggest_categorical("arch", ss.get("arch", ["mlp", "cnn"]))
    params: dict = {
        "arch": arch,
        "lr": trial.suggest_float("lr", *ss.get("lr", [1e-4, 5e-3]), log=True),
        "dropout": trial.suggest_float("dropout", *ss.get("dropout", [0.1, 0.5])),
        "batch_size": trial.suggest_categorical("batch_size", ss.get("batch_size", [256, 512])),
        "pre_norm": trial.suggest_categorical("pre_norm", ss.get("pre_norm", ["layer_norm", "none"])),
    }
    # weight_decay（Adam L2）は search_space にキーがある時だけ探索する（後方互換：
    # 旧 config・空間指定なしでは 0.0 のまま＝従来挙動）。過学習抑制の主ノブ。
    if "weight_decay" in ss:
        params["weight_decay"] = trial.suggest_float("weight_decay", *ss["weight_decay"], log=True)
    if params["pre_norm"] == "none":
        params["pre_norm"] = None
    if arch == "cnn":
        n_conv = trial.suggest_int("n_conv", *ss.get("n_conv", [1, 3]))
        width = trial.suggest_categorical("conv_width", ss.get("conv_width", [16, 32, 64]))
        # 段階的にチャネルを増やす（width, 2*width, ...）
        params["conv_channels"] = [width * (2**i) for i in range(n_conv)]
        params["kernel_size"] = trial.suggest_categorical("kernel_size", ss.get("kernel_size", [3, 5]))
    else:
        n_layers = trial.suggest_int("n_layers", *ss.get("n_layers", [1, 3]))
        width = trial.suggest_categorical("layer_width", ss.get("layer_width", [64, 128, 256]))
        # 段階的に幅を絞る（width, width/2, ...、最小 32）
        params["hidden_dims"] = [max(32, width // (2**i)) for i in range(n_layers)]
    return params


def tune_nn(
    x_tr,
    y_tr,
    x_val,
    y_val,
    search_space: dict,
    categorical_cardinalities: dict,
    n_numeric: int,
    n_trials: int = 25,
    timeout: float | None = None,
    seed: int = 100,
    scale_pos_weight: float = 1.0,
    epochs: int = 15,
    max_train_rows: int | None = 120000,
) -> dict:
    """NnWinModel の最良構造・学習パラメータを Optuna で探索して返す（AUC 最大化）。

    x_tr/x_val は NN ストリーム形式（derive_nn_input 済みの float 配列）。
    各 trial は短め epochs・部分標本で素早く評価し、検証 AUC を最大化する。

    Returns
    -------
    dict : best な nn_params（arch, lr, dropout, hidden_dims/conv_channels 等）
    """
    import optuna
    import optuna.logging  # 明示 submodule import（環境により optuna.logging が自動公開されない）
    from sklearn.metrics import roc_auc_score

    from ._nn_win_model import NnWinModel

    def objective(trial):
        params = _suggest_nn_params(trial, search_space)
        model = NnWinModel(
            categorical_cardinalities=categorical_cardinalities,
            n_numeric=n_numeric,
            pos_weight=scale_pos_weight,
            epochs=epochs,
            max_train_rows=max_train_rows,
            seed=seed,
            **params,
        )
        try:
            model.fit(x_tr, y_tr)
            preds = model.predict_proba(x_val)[:, 1]
        except Exception as e:  # noqa: BLE001
            logger.warning("[tune_nn] trial 失敗のため pruned: %s", e)
            raise optuna.TrialPruned() from e
        auc = roc_auc_score(y_val, preds)
        # 構造を user_attr に保存（hidden_dims/conv_channels は suggest 名と別管理のため）
        trial.set_user_attr("nn_params", params)
        return auc

    from ._tuning_storage import study_kwargs

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    sampler = optuna.samplers.TPESampler(seed=seed)
    # --resume-tuning 時は永続 study を再開（trial 追記＝best 単調改善）。
    study = optuna.create_study(direction="maximize", sampler=sampler, **study_kwargs("nn"))
    study.optimize(objective, n_trials=n_trials, timeout=timeout)
    best = study.best_trial.user_attrs.get("nn_params", {})
    logger.info(
        "[tune_nn] best_auc=%.4f params=%s", study.best_value, best,
    )
    return best


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
    import optuna.logging  # 明示 submodule import（環境により optuna.logging が自動公開されない）

    from ._tuning_storage import study_kwargs

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    sampler = optuna.samplers.TPESampler(seed=seed)
    # --resume-tuning 時はモデル別の永続 study を再開（trial 追記＝best 単調改善）。
    study = optuna.create_study(
        direction="minimize", sampler=sampler, **study_kwargs(model_name)
    )
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
