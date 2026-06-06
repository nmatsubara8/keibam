import pandas as pd

from src.policies._bet_policy import AbstractBetPolicy
from src.policies._score_policy import AbstractScorePolicy

from ._data_splitter import DataSplitter
from ._model_wrapper import ModelWrapper


class KeibaAI:
    """
    モデルの訓練や読み込み、実際に賭けるなどの処理を実行するクラス。
    """

    def __init__(self, datasets: DataSplitter):
        self.__datasets = datasets
        self.__model_wrapper = ModelWrapper()
        self._calibrated_model = None  # train_with_stacking 後に設定される
        self.peds_processor = None  # PedsProcessor with fitted encoders (serialized with model for inference)
        self.nn_scaler = None  # NnFeatureScaler with fitted StandardScaler (serialized with model for inference)

    @property
    def datasets(self):
        return self.__datasets

    def train_with_tuning(self):
        """
        optunaでのチューニング後、訓練させる。
        """
        self.__model_wrapper.tune_hyper_params(self.__datasets)
        self.__model_wrapper.train(self.__datasets)

    def train_without_tuning(self):
        """
        ハイパーパラメータチューニングをスキップして訓練させる。
        """
        self.__model_wrapper.train(self.__datasets)

    def train_with_stacking(self, meta_ratio: float = 0.3, with_tuning: bool = True) -> None:
        """スタッキング+Isotonic 較正の Layer1 パイプラインを実行する。

        1. make_stacking_splits で base_train / meta_train / calib_holdout に分割
        2. with_tuning=True なら base_train 上で Optuna ハイパラ探索
        3. base_train で初期 LightGBM を学習し、ブートストラップ予測から
           §2 EV境界 sigmoid 重み（§2h レース内正規化）を算出
        4. StackingModel (LightGBM + NN) を base_train で学習。LightGBM base には
           EV 重み、NN base には pos_weight（class imbalance 補正）を適用
        5. meta_train で meta 特徴量を生成し LogisticRegression meta 学習器を学習
        6. calib_holdout で Isotonic 較正し self._calibrated_model に保存
        """
        import lightgbm as lgb
        from sklearn.linear_model import LogisticRegression

        from src.constants._bet_thresholds import TrainingWeights

        from ._calibrated_model import CalibratedModel
        from ._stacking_model import StackingModel

        self.__datasets.make_stacking_splits(meta_ratio=meta_ratio)
        if with_tuning:
            self.__model_wrapper.tune_hyper_params(self.__datasets)

        x_base = self.__datasets.X_base_train.values
        y_base = self.__datasets.y_base_train.values

        # §2: ブートストラップ予測 → EV境界 sigmoid 重み（レース内正規化）
        ev_weights = self.__compute_base_ev_weights(x_base, y_base)

        # base①: LightGBM（scale_pos_weight でクラス不均衡補正、EV 重みは sample_weight）
        params = dict(self.__model_wrapper.params)
        params.setdefault("scale_pos_weight", TrainingWeights.SCALE_POS_WEIGHT)
        lgb_base = lgb.LGBMClassifier(**params)
        base_models = [lgb_base]
        base_sample_weights = [ev_weights]

        # base②: NN（pos_weight でクラス不均衡補正）
        try:
            from ._nn_win_model import NnWinModel

            base_models.append(
                NnWinModel(
                    n_numeric=self.__datasets.X_base_train.shape[1],
                    pos_weight=TrainingWeights.SCALE_POS_WEIGHT,
                )
            )
            base_sample_weights.append(None)  # NN は pos_weight で補正、EV 重みは未適用
        except Exception:
            pass  # torch 未インストールの場合は LightGBM のみで動作

        stacking = StackingModel(base_models, LogisticRegression(max_iter=1000, random_state=100))
        stacking.fit(
            x_base,
            y_base,
            self.__datasets.X_meta_train.values,
            self.__datasets.y_meta_train.values,
            base_sample_weights=base_sample_weights,
        )
        self._calibrated_model = CalibratedModel.fit(
            stacking,
            self.__datasets.X_calib.values,
            self.__datasets.y_calib.values,
        )

    def __compute_base_ev_weights(self, x_base, y_base):
        """base_train でブートストラップ LightGBM を学習し EV境界重みを返す。

        odds（TANSHO_ODDS）と race_id（index）が base_train_data から取れない場合は
        None（等重み）にフォールバックする。
        """
        import lightgbm as lgb

        from src.constants._bet_thresholds import TrainingWeights
        from src.constants._results_cols import ResultsCols

        from ._sample_weights import compute_ev_weights

        try:
            base_df = self.__datasets.base_train_data
            odds = base_df[ResultsCols.TANSHO_ODDS].values
            race_ids = base_df.index.values
        except Exception:
            return None

        boot = lgb.LGBMClassifier(scale_pos_weight=TrainingWeights.SCALE_POS_WEIGHT, objective="binary")
        boot.fit(x_base, y_base)
        pred = boot.predict_proba(x_base)[:, 1]
        return compute_ev_weights(pred, odds, race_ids)

    @property
    def effective_model(self):
        """較正済みモデルがあればそれを、なければ LightGBM 単体モデルを返す。"""
        return self._calibrated_model if self._calibrated_model is not None else self.__model_wrapper.lgb_model

    def get_params(self):
        """
        ハイパーパラメータを取得
        """
        return self.__model_wrapper.params

    def set_params(self, params):
        """
        ハイパーパラメータを外部から設定。
        """
        self.__model_wrapper.set_params(params)

    def feature_importance(self, num_features=20):
        return self.__model_wrapper.feature_importance[:num_features]

    def decide_action(self, score_table: pd.DataFrame, bet_policy: AbstractBetPolicy, **params) -> dict:
        """
        bet_policyを元に、賭ける馬券を決定する。paramsにthresholdを入れる。
        """
        actions = bet_policy.judge(score_table, **params)

        return actions

    def calc_score(self, X: pd.DataFrame, score_policy: AbstractScorePolicy):
        """
        score_policyを元に、馬の「勝ちやすさスコア」を計算する。
        train_with_stacking 済みの場合は較正済みスタッキングモデルを使用する。
        """
        model = self._calibrated_model if self._calibrated_model is not None else self.__model_wrapper.lgb_model
        return score_policy.calc(model, X)
