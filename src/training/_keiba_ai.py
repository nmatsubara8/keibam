from __future__ import annotations

from typing import Any

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
        self._calibrated_model: Any = None  # train_with_stacking 後に設定される
        self.peds_processor = None  # PedsProcessor with fitted encoders (serialized with model for inference)
        self.nn_scaler = None  # NnFeatureScaler with fitted StandardScaler (serialized with model for inference)
        self.feature_names_: list[str] | None = None  # 学習時の列順序（推論時の列整合用）
        self._tuned_base_models_config: Any = None  # tune_per_model 探索後の完成 config（書き戻し用）

    @property
    def datasets(self):
        return self.__datasets

    @property
    def tuning_study_(self):
        """直近の Optuna 探索 study（未実行なら None）。全 trial の成績・パラメータを持つ。"""
        return getattr(self.__model_wrapper, "last_study_", None)

    @property
    def tuned_base_models_config(self):
        """tune_per_model 探索後の完成 BaseModelsConfig（xgboost/catboost/nn の best 反映済み）。

        探索を伴わない学習では None。retrain 側がこれを JSON 保存し、そのまま固定運用の
        base_models config に書き戻せるようにする（§5⑤ の「書き戻し」を機械化）。
        """
        return self._tuned_base_models_config

    def set_lgb_params(self, params: dict) -> None:
        """LightGBM ハイパーパラメータを外部から注入する。

        保存済みのチューニング履歴（tuning_history.json）から選んだパラメータで
        学習する場合に、train_without_tuning / train_with_stacking(with_tuning=False)
        の前に呼ぶ。
        """
        self.__model_wrapper.set_params(params)

    def train_with_tuning(self, tuning_config=None):
        """
        optunaでのチューニング後、訓練させる。

        tuning_config で探索範囲・回数を制御できる（None なら LightGBMTuner）。
        """
        self.__model_wrapper.tune_hyper_params(self.__datasets, tuning_config=tuning_config)
        self.__model_wrapper.train(self.__datasets)

    def train_without_tuning(self):
        """
        ハイパーパラメータチューニングをスキップして訓練させる。
        """
        self.__model_wrapper.train(self.__datasets)

    def train_with_stacking(self, meta_ratio=0.3, with_tuning=True, tuning_config=None, base_models_config=None):
        """スタッキング+Isotonic 較正の Layer1 パイプラインを実行する。

        1. make_stacking_splits で base_train / meta_train / calib_holdout に分割
        2. with_tuning=True なら base_train 上で Optuna ハイパラ探索
        3. base_train で初期 LightGBM を学習し、ブートストラップ予測から
           §2 EV境界 sigmoid 重み（§2h レース内正規化）を算出
        4. StackingModel (LightGBM + NN) を base_train で学習。LightGBM base には
           EV 重み、NN base には pos_weight（class imbalance 補正）を適用
        5. meta_train で meta 特徴量を生成し meta 学習器を学習
           （base_models_config.meta_model="logistic"=LogisticRegression /
           "lightgbm"=浅い GBDT meta。build_meta_model で構築）
        6. calib_holdout で Isotonic 較正し self._calibrated_model に保存
        """
        import dataclasses

        from src.constants._bet_thresholds import TrainingWeights

        from ._base_model_factory import build_base_models, build_meta_model
        from ._base_models_config import BaseModelsConfig
        from ._calibrated_model import CalibratedModel
        from ._multi_model_tuner import tune_model, tune_nn
        from ._stacking_model import StackingModel, derive_nn_input

        self.__datasets.make_stacking_splits(meta_ratio=meta_ratio, build_optuna_datasets=with_tuning)
        if with_tuning:
            self.__model_wrapper.tune_hyper_params(self.__datasets, tuning_config=tuning_config)

        x_base = self.__datasets.X_base_train.values
        y_base = self.__datasets.y_base_train.values

        # §2: ブートストラップ予測 → EV境界 sigmoid 重み（レース内正規化）
        ev_weights = self.__compute_base_ev_weights(x_base, y_base)

        bm_cfg = base_models_config or BaseModelsConfig()

        params = dict(self.__model_wrapper.params)
        params.setdefault("scale_pos_weight", TrainingWeights.SCALE_POS_WEIGHT)

        # XGB/CatBoost の per-model Optuna チューニング
        extra_tuned = {}
        if bm_cfg.tune_per_model:
            n = len(x_base)
            split = int(n * 0.8)
            x_bt, y_bt = x_base[:split], y_base[:split]
            x_bv, y_bv = x_base[split:], y_base[split:]
            for mname in bm_cfg.models:
                if mname in ("xgboost", "catboost"):
                    space = (
                        bm_cfg.xgboost_search_space
                        if mname == "xgboost"
                        else bm_cfg.catboost_search_space
                    )
                    best = tune_model(
                        mname, x_bt, y_bt, x_bv, y_bv, space,
                        n_trials=bm_cfg.n_trials,
                        timeout=bm_cfg.timeout,
                        scale_pos_weight=TrainingWeights.SCALE_POS_WEIGHT,
                    )
                    extra_tuned[mname] = best

        # NN の per-model Optuna 探索（構造・学習率・正規化を最適化）
        if bm_cfg.tune_per_model and "nn" in bm_cfg.models and self.__datasets.has_nn_stream:
            try:
                scaler = self.__datasets.nn_scaler
                cards = self.__datasets.nn_categorical_cardinalities or {}
                # NN ストリーム形式（derive 済み float 配列）を base_train から導出し 80/20 分割
                nn_arr = derive_nn_input(scaler, self.__datasets.X_base_train)
                nsplit = int(len(nn_arr) * 0.8)
                best_nn = tune_nn(
                    nn_arr[:nsplit], y_base[:nsplit],
                    nn_arr[nsplit:], y_base[nsplit:],
                    bm_cfg.nn_search_space,
                    categorical_cardinalities=cards,
                    n_numeric=len(scaler.numeric_cols),
                    n_trials=bm_cfg.nn_tune_trials,
                    timeout=bm_cfg.timeout,
                    scale_pos_weight=TrainingWeights.SCALE_POS_WEIGHT,
                    epochs=bm_cfg.nn_tune_epochs,
                    max_train_rows=bm_cfg.nn_tune_max_rows,
                )
                if best_nn:
                    # 探索した構造を nn_params に反映（epochs/batch 等の既存設定は残す）
                    merged_nn = dict(bm_cfg.nn_params)
                    merged_nn.update(best_nn)
                    bm_cfg = dataclasses.replace(bm_cfg, nn_params=merged_nn)
            except Exception as _e:
                import logging as _l

                _l.getLogger(__name__).warning("NN チューニング失敗のためスキップ: %s", _e)

        if extra_tuned:
            kw = {}
            if "xgboost" in extra_tuned:
                merged = dict(bm_cfg.xgboost_params)
                merged.update(extra_tuned["xgboost"])
                kw["xgboost_params"] = merged
            if "catboost" in extra_tuned:
                merged = dict(bm_cfg.catboost_params)
                merged.update(extra_tuned["catboost"])
                kw["catboost_params"] = merged
            if kw:
                bm_cfg = dataclasses.replace(bm_cfg, **kw)

        # 探索済みの完成 config（xgboost/catboost/nn の best を反映済み）を公開する。
        # tune_per_model 時のみ意味を持ち、retrain が JSON 保存＝固定運用へ書き戻せる。
        if getattr(bm_cfg, "tune_per_model", False):
            self._tuned_base_models_config = bm_cfg

        specs = build_base_models(bm_cfg, params, TrainingWeights.SCALE_POS_WEIGHT)
        self.base_model_names_ = [s.name for s in specs]
        base_models: list[Any] = [s.model for s in specs]
        base_sample_weights = [ev_weights if s.weight == "ev" else None for s in specs]
        base_streams = [s.stream for s in specs]

        # NN base（Phase 2）: entity+numeric を専用ストリームとして消費する。
        # entity/numeric 列は gbdt DataFrame 内に共存するため、StackingModel が
        # nn_scaler で内部導出する（推論時も gbdt 1 枚から再構成でき契約は不変）。
        if "nn" in bm_cfg.models and self.__datasets.has_nn_stream:
            try:
                from ._nn_win_model import NnWinModel

                cards = self.__datasets.nn_categorical_cardinalities or {}
                scaler = self.__datasets.nn_scaler
                nn_kwargs = {
                    k: v for k, v in dict(bm_cfg.nn_params).items()
                    if k in (
                        "hidden_dims", "epochs", "lr", "batch_size", "max_train_rows",
                        "arch", "dropout", "conv_channels", "kernel_size", "pre_norm",
                    )
                }
                base_models.append(
                    NnWinModel(
                        categorical_cardinalities=cards,
                        n_numeric=len(scaler.numeric_cols),
                        pos_weight=TrainingWeights.SCALE_POS_WEIGHT,
                        **nn_kwargs,
                    )
                )
                base_sample_weights.append(None)
                base_streams.append("nn")
                self.base_model_names_.append("NN")
            except Exception as _e:
                import logging as _l

                _l.getLogger(__name__).warning("NN base 構築失敗のためスキップ: %s", _e)

        meta_model = build_meta_model(bm_cfg, scale_pos_weight=TrainingWeights.SCALE_POS_WEIGHT)
        stacking = StackingModel(
            base_models,
            meta_model,
            base_streams=base_streams,
            nn_scaler=self.__datasets.nn_scaler,
            nn_cat_cardinalities=self.__datasets.nn_categorical_cardinalities,
        )
        stacking.fit(
            self.__datasets.X_base_train,
            y_base,
            self.__datasets.X_meta_train,
            self.__datasets.y_meta_train.values,
            base_sample_weights=base_sample_weights,
        )
        self._calibrated_model = CalibratedModel.fit(
            stacking,
            self.__datasets.X_calib,
            self.__datasets.y_calib.values,
        )
        self.feature_names_ = list(self.__datasets.X_base_train.columns)

        # base LightGBM の特徴量重要度を ModelWrapper に反映（特徴量重要度ページ用）
        try:
            lgb_spec = next((s for s in specs if s.name == "LightGBM"), None)
            if lgb_spec is not None:
                fi_vals = lgb_spec.model.feature_importances_
                fi_cols = self.__datasets.X_base_train.columns
                self.__model_wrapper.feature_importance = (
                    pd.DataFrame({"features": fi_cols, "importance": fi_vals})
                    .sort_values("importance", ascending=False)
                )
        except Exception:
            pass  # 重要度取得失敗は致命的でないため無視

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
        fi = self.__model_wrapper.feature_importance
        if fi is None:
            return None
        return fi[:num_features]

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

        feature_names_ が保存されている場合はライブ推論時の列不一致を自動修正する:
        - 不足列は 0 埋め、余分な列は無視。
        - score_policy が必要とする 枠番・馬番・単勝 等の非特徴量列は X から保持。
        """
        import logging as _log
        _logger = _log.getLogger(__name__)
        model = self._calibrated_model if self._calibrated_model is not None else self.__model_wrapper.lgb_model

        # feature_names_ が保存済みでない旧モデルは datasets から補完する
        feature_names: list[str] | None = getattr(self, "feature_names_", None)
        if feature_names is None:
            try:
                feature_names = list(self.__datasets.X_base_train.columns)
                self.feature_names_ = feature_names
            except Exception:
                pass

        # feature_names_ が保存済みの場合: 列を学習時の順序・セットに揃える
        if feature_names is not None:
            from src.policies._score_policy import META_COLS
            # score_policy が参照する非特徴量列（枠番・馬番・単勝など）は X に残す必要がある
            meta_cols = [c for c in META_COLS if c in X.columns]
            feat_cols = [c for c in feature_names if c not in meta_cols]
            X_feat = X.reindex(columns=feat_cols, fill_value=0)
            missing = [c for c in feat_cols if c not in X.columns]
            if missing:
                _logger.warning("calc_score: %d 列が X に存在しないため 0 で補完: %s ...", len(missing), missing[:5])
            X = pd.concat([X[[c for c in meta_cols if c in X.columns]], X_feat], axis=1)

        return score_policy.calc(model, X)
