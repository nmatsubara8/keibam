import lightgbm as lgb
import optuna.integration.lightgbm as lgb_o
import pandas as pd
from sklearn.metrics import roc_auc_score

from ._data_splitter import DataSplitter


class ModelWrapper:
    """
    モデルのハイパーパラメータチューニング・学習の処理が記述されたクラス。
    """

    def __init__(self):
        self.__lgb_model = lgb.LGBMClassifier(objective="binary")
        self.__feature_importance = None

    def tune_hyper_params(self, datasets: DataSplitter):
        """
        optunaによるチューニングを実行。
        """

        params = {"objective": "binary", "verbose": -1}

        # チューニング実行
        lgb_clf_o = lgb_o.train(
            params,
            datasets.lgb_train_optuna,
            valid_sets=datasets.lgb_valid_optuna,
            # verbose_eval=100,
            # early_stopping_rounds=10,
            optuna_seed=100,  # optunaのseed固定
        )

        # num_iterationsとearly_stopping_roundは今は使わないので削除
        tunedParams = {k: v for k, v in lgb_clf_o.params.items() if k not in ["num_iterations", "early_stopping_round"]}

        self.__lgb_model.set_params(**tunedParams)

    @property
    def params(self):
        return self.__lgb_model.get_params()

    def set_params(self, ex_params):
        """
        外部からハイパーパラメータを設定する場合。
        """
        self.__lgb_model.set_params(**ex_params)

    def train(self, datasets: DataSplitter):
        # 学習
        self.__lgb_model.fit(datasets.X_train, datasets.y_train)
        # AUCを計算して出力
        auc_train = roc_auc_score(datasets.y_train, self.__lgb_model.predict_proba(datasets.X_train)[:, 1])
        auc_test = roc_auc_score(
            datasets.y_test,
            self.__lgb_model.predict_proba(datasets.X_test)[:, 1],
        )
        # 特徴量の重要度を記憶しておく
        # NumPy配列からDataFrameに変換する
        X_train_df = pd.DataFrame(datasets.X_train)
        self.__feature_importance = pd.DataFrame(
            {"features": X_train_df.columns, "importance": self.__lgb_model.feature_importances_}
        ).sort_values("importance", ascending=False)
        print("AUC: {:.3f}(train), {:.3f}(test)".format(auc_train, auc_test))

    @property
    def feature_importance(self):
        return self.__feature_importance

    @property
    def lgb_model(self):
        return self.__lgb_model

    @lgb_model.setter
    def lgb_model(self, loaded):
        self.__lgb_model = loaded
