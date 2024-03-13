import lightgbm as lgb
import optuna
import pandas as pd
from sklearn.metrics import mean_squared_error

from ._data_splitter import DataSplitter


class ModelWrapper:
    """
    モデルのハイパーパラメータチューニング・学習の処理が記述されたクラス。
    """

    def __init__(self):
        self.__lgb_model = lgb.LGBMRegressor(objective="regression")
        self.__feature_importance = None

    def objective(self, trial):
        learning_rate = trial.suggest_float("learning_rate", 0.1, 1.0)
        num_leaves = trial.suggest_int("num_leaves", 5, 50)
        tree_learner = trial.suggest_categorical("tree_learner", ["serial", "feature", "data", "voting"])

        params = {
            "task": "train",  # タスクを訓練に設定
            "boosting_type": "gbdt",  # GBDTを指定
            "objective": "regression",  # 回帰を指定
            "metric": {"rmse"},  # 回帰の損失（誤差）
            "learning_rate": learning_rate,  # 学習率
            "num_leaves": num_leaves,
            "tree_learner": tree_learner,
            "seed": 100,
            "verbose_eval": -1,  # シード値
        }
        # lgb_results = {}
        verbose_eval = -1
        self.__lgb_model = lgb.train(
            params=params,  # ハイパーパラメータをセット
            train_set=self.datasets.lgb_train_optuna,
            valid_sets=[
                self.datasets.lgb_valid_optuna
            ],  # 訓練データとテストデータをセット# データセットの名前をそれぞれ設定
            num_boost_round=10000,  # 計算回数
            callbacks=[lgb.early_stopping(stopping_rounds=10, verbose=True), lgb.log_evaluation(verbose_eval)],
            # ログを最後の1つだけ表示
        )
        y_pred = self.__lgb_model.predict(self.datasets.lgb_train_optuna.data)
        rmse = mean_squared_error(self.datasets.lgb_train_optuna.label, y_pred, squared=False)
        return rmse

    def tune_hyper_params(self, datasets: DataSplitter):
        """
        optunaによるチューニングを実行。
        """
        self.datasets = datasets  # データセットを保存
        self.study = optuna.create_study(direction="minimize")
        self.study.optimize(self.objective, n_trials=1000, show_progress_bar=True)
        _params = self.study.best_params

        # num_iterationsとearly_stopping_roundは今は使わないので削除
        tuned_params = {k: v for k, v in _params.items()}

        self.__lgb_model.set_params(**tuned_params)
        y_pred = self.__lgb_model.predict(self.datasets.lgb_valid_optuna.data)

        # 評価
        rmse = mean_squared_error(self.datasets.lgb_valid_optuna.label, y_pred, squared=False)
        print(f"rmse of test data: {rmse }")

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
        self.__lgb_model.fit(datasets.X_train.values, datasets.y_train.values)
        # AUCを計算して出力
        # auc_train = roc_auc_score(datasets.y_train, self.__lgb_model.predict_proba(datasets.X_train)[:, 1])
        # auc_test = roc_auc_score(
        #    datasets.y_test,
        #    self.__lgb_model.predict_proba(datasets.X_test.drop([ResultsCols.TANSHO_ODDS], axis=1))[:, 1],
        # )
        # 特徴量の重要度を記憶しておく
        self.__feature_importance = pd.DataFrame(
            {"features": datasets.X_train.columns, "importance": self.__lgb_model.feature_importances_}
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
