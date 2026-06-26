"""retrain コマンド（全データ再学習）と設定ビルダ。"""

from __future__ import annotations

import argparse
import logging
import os

from src.pipeline._cli_common import _auto_migrate_db

logger = logging.getLogger(__name__)


def _build_base_models_config(args):
    """args から BaseModelsConfig を組み立てる（指定なければ None）。"""
    from src.training._base_models_config import from_dict, load_base_models_config

    if hasattr(args, "base_models_config") and args.base_models_config:
        return load_base_models_config(args.base_models_config)
    if hasattr(args, "base_models") and args.base_models:
        models = tuple(m.strip() for m in args.base_models.split(","))
        return from_dict({"models": list(models)})
    return None


def _retrain(args: argparse.Namespace) -> None:
    """再学習ジョブを実行する（optuna が with_tuning=True 時に必要）。"""
    import pandas as pd

    from src.constants._local_paths import LocalPaths
    from src.pipeline._retrain import RetrainConfig
    from src.pipeline._retrain import RetrainJob
    from src.training._keiba_ai_factory import KeibaAIFactory

    # Phase 1: pickle → DB の自動移行（DB が空の場合のみ実行される）
    _auto_migrate_db()

    cfg = RetrainConfig(
        use_stacking=not args.no_stacking,
        train_win_head=not getattr(args, "no_win_head", False),
    )

    featured_path = LocalPaths.FEATURED_DATA_PATH
    if not os.path.exists(featured_path):
        logger.info("[retrain] featured_data.pkl が見つからないため自動生成します")
        from src.pipeline._ingestion import IngestConfig
        cfg_ing = IngestConfig()

        class _Builder:
            def build(self, config):
                from src.preprocessing._data_merger import DataMerger
                from src.preprocessing._feature_engineering import FeatureEngineering
                from src.preprocessing._horse_info_processor import HorseInfoProcessor
                from src.preprocessing._horse_results_processor import HorseResultsProcessor
                from src.preprocessing._peds_processor import PedsProcessor
                from src.preprocessing._race_info_processor import RaceInfoProcessor
                from src.preprocessing._results_processor import ResultsProcessor
                from src.constants._feature_cols import AGG_TARGET_COLS

                merger = DataMerger(
                    ResultsProcessor(config.raw_results_path),
                    RaceInfoProcessor(config.raw_race_info_path),
                    HorseResultsProcessor(config.raw_horse_results_path),
                    HorseInfoProcessor(config.raw_horse_info_path),
                    PedsProcessor(config.raw_peds_path),
                    target_cols=AGG_TARGET_COLS,
                    group_cols=["騎手"],
                )
                merger.merge()
                return (
                    FeatureEngineering(merger)
                    .add_interval().add_agedays()
                    .add_derived_features()
                    .add_date_cyclical()
                    .add_interaction_features()
                    .add_race_level_zscore()
                    .dumminize_kaisai().dumminize_sex().dumminize_weather()
                    .dumminize_race_type().dumminize_ground_state1().dumminize_ground_state2()
                    .dumminize_around().add_race_class_level().dumminize_race_class()
                    .encode_horse_id()
                    .encode_jockey_id().encode_trainer_id().encode_owner_id().encode_breeder_id()
                ).featured_data

        featured_data = _Builder().build(cfg_ing)
        featured_data.to_pickle(featured_path)
        logger.info("[retrain] featured_data.pkl を生成しました shape=%s", featured_data.shape)
    else:
        featured_data = pd.read_pickle(featured_path)

    # --no-odds-features: オッズ由来の派生特徴（単勝_log・市場歪み overlay 等）を学習から
    # 除外する。マーケット・エコー検証（r̂ が市場の写しでないかの A/B）用。Place/Win 両ヘッドに
    # 適用される（この時点では DataFrame なので prepared_from_gbdt 変換の前に落とす）。
    if getattr(args, "no_odds_features", False):
        from src.constants._feature_cols import ODDS_DERIVED_FEATURE_COLS

        present = [c for c in ODDS_DERIVED_FEATURE_COLS if c in featured_data.columns]
        featured_data = featured_data.drop(columns=present, errors="ignore")
        logger.info("[retrain] --no-odds-features: オッズ由来 %d 列を除外: %s", len(present), present)

    # --params-rank: 保存済みチューニング履歴（成績順）から指定 rank のパラメータで学習。
    # --use-selected-params: UI（モデルラボ）で保存した選択（models/selected_params.json）を使う。
    lgb_params = None
    params_rank = getattr(args, "params_rank", None)
    if params_rank is not None:
        from src.training._tuning_history import get_params_by_rank
        from src.training._tuning_history import load_tuning_history
        from src.training._tuning_history import tuning_history_path

        history = load_tuning_history(tuning_history_path(cfg.models_dir))
        lgb_params = get_params_by_rank(history, params_rank)
        logger.info("[retrain] tuning_history rank=%d のパラメータで学習します", params_rank)
    elif getattr(args, "use_selected_params", False):
        import json

        selected_path = os.path.join(cfg.models_dir, "selected_params.json")
        if not os.path.exists(selected_path):
            raise FileNotFoundError(
                f"{selected_path} がありません（UI のモデルラボでパラメータを選択してください）"
            )
        with open(selected_path) as f:
            selected = json.load(f)
        lgb_params = selected["params"]
        params_rank = selected.get("rank")
        logger.info(
            "[retrain] selected_params.json（version=%s rank=%s）のパラメータで学習します",
            selected.get("version"), params_rank,
        )

    # 手書き Optuna 探索の設定（--tuning-config / --n-trials / --tuning-timeout）。
    # いずれかが指定された場合は method="optuna" の探索（範囲・回数を制御）を使う。
    tuning_config = _build_tuning_config(args)
    if tuning_config is not None and not args.with_tuning:
        # 探索設定を渡すなら自動的に探索を有効化する（指定漏れ防止）
        args.with_tuning = True
        logger.info("[retrain] 探索設定が指定されたため --with-tuning を有効化します")

    base_models_config = _build_base_models_config(args)

    # NN base を使う場合は 2 系統（gbdt+nn）の PreparedFeatures を構成する。
    # entity/numeric 列は gbdt 内に共存するため列選択のみで導出でき、キャッシュ済み
    # featured_data からも特徴量エンジニアリング再実行なしで NN ストリームを作れる。
    if base_models_config is not None and "nn" in base_models_config.models:
        from src.preprocessing._prepared_features import prepared_from_gbdt

        featured_data = prepared_from_gbdt(featured_data)
        logger.info("[retrain] NN base 用に 2 系統 PreparedFeatures を構成しました")

    # KeibaAIFactory は create/save が staticmethod のためインスタンスを渡す（AIFactory Protocol 適合）
    job = RetrainJob(KeibaAIFactory(), cfg)
    result = job.run(
        featured_data,
        vname=args.version_name,
        with_tuning=args.with_tuning,
        lgb_params=lgb_params,
        params_rank=params_rank,
        tuning_config=tuning_config,
        base_models_config=base_models_config,
    )
    logger.info("[retrain] %s", result)


def _build_tuning_config(args: argparse.Namespace):
    """CLI 引数から TuningConfig を構築する（未指定なら None=LightGBMTuner）。"""
    config_path = getattr(args, "tuning_config", None)
    n_trials = getattr(args, "n_trials", None)
    timeout = getattr(args, "tuning_timeout", None)

    if config_path is None and n_trials is None and timeout is None:
        return None

    from src.training._tuning_config import METHOD_OPTUNA
    from src.training._tuning_config import TuningConfig
    from src.training._tuning_config import load_tuning_config

    if config_path is not None:
        cfg = load_tuning_config(config_path)
    else:
        cfg = TuningConfig(method=METHOD_OPTUNA)

    # CLI の --n-trials / --tuning-timeout は設定ファイルより優先する
    overrides: dict = {"method": METHOD_OPTUNA}
    if n_trials is not None:
        overrides["n_trials"] = n_trials
    if timeout is not None:
        overrides["timeout"] = timeout
    import dataclasses
    cfg = dataclasses.replace(cfg, **overrides)
    logger.info(
        "[retrain] 探索設定: method=%s n_trials=%d timeout=%s",
        cfg.method, cfg.n_trials, cfg.timeout,
    )
    return cfg
