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

    def tune_hyper_params(self, datasets: DataSplitter, study=None):
        """
        optunaによるチューニングを実行。

        study を明示的に生成して渡すことで、終了後に全 trial（パラメータと成績）を
        回収できる（`last_study_` に保持。RetrainJob が tuning_history.json へ保存する）。
        """
        import optuna  # noqa: PLC0415

        if study is None:
            study = optuna.create_study(direction="minimize")
        self.last_study_ = study

        params = {"objective": "binary", "verbose": -1}

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

        # num_iterationsとearly_stopping_roundは今は使わないので削除
        tunedParams = {k: v for k, v in lgb_clf_o.params.items() if k not in ["num_iterations", "early_stopping_round"]}

        self.__lgb_model.set_params(**tunedParams)
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
        # AUCを計算して出力
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

    @property
    def lgb_model(self):
        return self.__lgb_model

    @lgb_model.setter
    def lgb_model(self, loaded):
        self.__lgb_model = loaded
