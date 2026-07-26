import logging

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

from src.constants._bet_thresholds import TrainingWeights
from src.constants._results_cols import ResultsCols

from ._data_splitter import DataSplitter
from ._lgb_optuna import lgb_o

logger = logging.getLogger(__name__)


class ModelWrapper:
    """
    モデルのハイパーパラメータチューニング・学習の処理が記述されたクラス。

    KB 不均衡データ対応（§2）: `scale_pos_weight`（クラスレベルのマクロ補正）を
    LGBMClassifier に注入する。EV sigmoid 重み（§2 / §2h）は `train()` の
    `sample_weight` 引数（サンプルレベルのミクロ補正）として直交合成する。
    `is_unbalance` は使わない（EV sigmoid との二重重み付けを避けるため）。
    """

    def __init__(self, scale_pos_weight: float = TrainingWeights.SCALE_POS_WEIGHT):
        self.__lgb_model = lgb.LGBMClassifier(
            objective="binary", scale_pos_weight=scale_pos_weight
        )
        self.__feature_importance = None

    def tune_hyper_params(self, datasets: DataSplitter, study=None, tuning_config=None):
        """optunaによるチューニングを実行。

        tuning_config が None または method="lightgbm_tuner" の場合は従来どおり
        LightGBMTuner（自動段階探索・範囲/回数は固定）を使う。method="optuna" の
        場合は手書き Optuna で探索範囲（search_space）・試行回数（n_trials）・
        打ち切り（timeout）を制御する。

        study を明示的に生成して渡すことで、終了後に全 trial（パラメータと成績）を
        回収できる（`last_study_` に保持。RetrainJob が tuning_history.json へ保存する）。
        """
        from ._tuning_config import TuningConfig

        cfg = tuning_config if tuning_config is not None else TuningConfig()
        if cfg.is_custom:
            return self.__tune_custom(datasets, cfg, study)
        return self.__tune_lightgbm_tuner(datasets, study)

    def __tune_lightgbm_tuner(self, datasets: DataSplitter, study=None):
        """LightGBMTuner の自動段階探索（探索種類・範囲・回数はライブラリ固定）。"""
        import optuna  # noqa: PLC0415

        if study is None:
            study = optuna.create_study(direction="minimize")
        self.last_study_ = study

        from src.training._gpu_config import lgb_gpu_params

        params = {"objective": "binary", "verbose": -1}
        params.update(lgb_gpu_params())  # KEIBA_LGB_GPU 時のみ device_type=gpu/cuda

        # チューニング実行
        lgb_clf_o = lgb_o.train(
            params,
            datasets.lgb_train_optuna,
            valid_sets=[datasets.lgb_valid_optuna],
            # verbose_eval=100,
            # early_stopping_rounds=10,
            study=study,
            optuna_seed=100,  # optunaのseed固定
        )

        # num_iterations/early_stopping は使わないので削除。device 系は環境属性なので stored params
        # には残さない（build 時に lgb_gpu_params で再付与＝CPU 実行時に GPU 設定が焼き付かない）。
        _drop = {"num_iterations", "early_stopping_round", "device_type", "device",
                 "gpu_platform_id", "gpu_device_id"}
        tunedParams = {k: v for k, v in lgb_clf_o.params.items() if k not in _drop}

        self.__lgb_model.set_params(**tunedParams)
        return study

    def __tune_custom(self, datasets: DataSplitter, cfg, study=None):
        """手書き Optuna 探索（search_space / n_trials / timeout を設定で制御）。"""
        import json  # noqa: PLC0415

        import optuna  # noqa: PLC0415

        # tuning_history.json が読む system_attrs と同じキー（完全パラメータの記録先）
        from ._tuning_history import _LGBM_PARAMS_ATTR
        from ._gpu_config import lgb_gpu_params

        if study is None:
            from ._tuning_storage import study_kwargs

            # --resume-tuning 時は永続 study を再開（手書き TPE 探索のみ。best 単調改善）。
            study = optuna.create_study(
                direction="minimize",
                sampler=optuna.samplers.TPESampler(seed=cfg.seed),
                **study_kwargs("lightgbm"),
            )
        self.last_study_ = study

        train_set = datasets.lgb_train_optuna
        valid_set = datasets.lgb_valid_optuna

        def objective(trial):
            params = cfg.suggest_params(trial)
            # min_child_samples を複数 trial で変化させると feature_pre_filter=true
            # （既定）が競合してエラーになるため、動的変更を許可する。
            params["feature_pre_filter"] = False
            # 再学習時にそのまま使えるよう、完全パラメータを user_attr に記録する
            # （trials_to_records が system_attrs / user_attrs の両方を参照する）。
            trial.set_user_attr(_LGBM_PARAMS_ATTR, json.dumps(params))
            # dart は early_stopping 非対応なので固定ラウンド数で学習する
            num_boost_round = params.pop("num_boost_round", cfg.num_boost_round)
            is_dart = params.get("boosting_type") == "dart"
            callbacks = [] if is_dart else [lgb.early_stopping(cfg.early_stopping_rounds, verbose=False)]
            # device は stored params（user_attr / best）に残さず、この学習だけ GPU にする。
            booster = lgb.train(
                {**params, **lgb_gpu_params()},
                train_set,
                valid_sets=[valid_set],
                num_boost_round=num_boost_round,
                callbacks=callbacks,
            )
            score_key = "valid_0" if not is_dart else list(booster.best_score.keys())[0]
            return booster.best_score[score_key]["binary_logloss"]

        logger.info(
            "[tune] 手書き Optuna 探索: n_trials=%d timeout=%s 探索対象=%s",
            cfg.n_trials, cfg.timeout, list(cfg.search_space.keys()),
        )
        study.optimize(objective, n_trials=cfg.n_trials, timeout=cfg.timeout)

        best = dict(study.best_trial.params)
        best.setdefault("objective", "binary")
        self.__lgb_model.set_params(**best)
        logger.info("[tune] best binary_logloss=%.5f params=%s", study.best_value, best)
        return study

    @property
    def params(self):
        return self.__lgb_model.get_params()

    def set_params(self, ex_params):
        """
        外部からハイパーパラメータを設定する場合。
        """
        self.__lgb_model.set_params(**ex_params)

    def train(self, datasets: DataSplitter, sample_weight=None):
        """LightGBM を学習する。

        sample_weight: §2 EV境界 sigmoid 重み（レース内正規化済み）を渡すと、
        EV>1 領域へ学習をフォーカスさせる。None なら従来どおり等重み。
        `scale_pos_weight`（コンストラクタ注入）と sample_weight は独立に合成される。
        """
        # 学習
        self.__lgb_model.fit(
            datasets.X_train.values, datasets.y_train.values, sample_weight=sample_weight
        )
        # AUCを計算して出力（predict_proba は np.ndarray|list を返し得るため asarray で正規化）
        auc_train = roc_auc_score(
            datasets.y_train, np.asarray(self.__lgb_model.predict_proba(datasets.X_train))[:, 1]
        )
        auc_test = roc_auc_score(
            datasets.y_test,
            np.asarray(
                self.__lgb_model.predict_proba(datasets.X_test.drop([ResultsCols.TANSHO_ODDS], axis=1))
            )[:, 1],
        )
        # 特徴量の重要度を記憶しておく
        self.__feature_importance = pd.DataFrame(
            {"features": datasets.X_train.columns, "importance": self.__lgb_model.feature_importances_}
        ).sort_values("importance", ascending=False)
        logger.info("AUC: %.3f(train), %.3f(test)", auc_train, auc_test)

    @property
    def feature_importance(self):
        return self.__feature_importance

    @feature_importance.setter
    def feature_importance(self, df):
        self.__feature_importance = df

    @property
    def lgb_model(self):
        return self.__lgb_model

    @lgb_model.setter
    def lgb_model(self, loaded):
        self.__lgb_model = loaded
