"""継続学習パイプライン CLI エントリ。

使用例（cron から実行）:
    # 日次: 前日の終了レースを取込
    python -m src.pipeline.run_pipeline --job ingest \
        --race-id 202401010101 202401010102 --post-date 2024-01-01

    # 週次: 全データで再学習
    python -m src.pipeline.run_pipeline --job retrain

selenium / optuna 等は実行時にのみ必要。コマンド解析と設定組立はそれらなしで動作する。
"""

from __future__ import annotations

import argparse
import logging
from typing import Sequence

logger = logging.getLogger(__name__)


def _ingest(args: argparse.Namespace) -> None:
    """取込ジョブを実行する（selenium / bs4 が実行時に必要）。"""
    from src.pipeline._ingestion import IngestConfig
    from src.pipeline._ingestion import IngestJob

    cfg = IngestConfig()

    # I/O アダプタ: preparing を遅延 import して DI
    class _ScrapingFetcher:
        """netkeiba から実データを取得する実 adapter（bs4/selenium 依存）。"""

        def fetch_results(self, race_ids):
            from src.preprocessing._abstract_data_processor import AbstractDataProcessor
            from src.preprocessing._results_processor import ResultsProcessor

            return ResultsProcessor(cfg.raw_results_path).preprocessed_data

        def fetch_race_info(self, race_ids):
            from src.preprocessing._race_info_processor import RaceInfoProcessor

            return RaceInfoProcessor(cfg.raw_race_info_path).preprocessed_data

        def fetch_return_tables(self, race_ids):
            from src.preprocessing._return_processor import ReturnProcessor

            return ReturnProcessor(cfg.raw_return_tables_path).preprocessed_data

    class _FullPipelineBuilder:
        """raw pickles から FeatureEngineering を実行する実 adapter。"""

        def build(self, config):
            from src.preprocessing._data_merger import DataMerger
            from src.preprocessing._feature_engineering import FeatureEngineering
            from src.preprocessing._horse_info_processor import HorseInfoProcessor
            from src.preprocessing._horse_results_processor import HorseResultsProcessor
            from src.preprocessing._peds_processor import PedsProcessor
            from src.preprocessing._race_info_processor import RaceInfoProcessor
            from src.preprocessing._results_processor import ResultsProcessor

            merger = DataMerger(
                ResultsProcessor(config.raw_results_path),
                RaceInfoProcessor(config.raw_race_info_path),
                HorseResultsProcessor(config.raw_horse_results_path),
                HorseInfoProcessor(config.raw_horse_info_path),
                PedsProcessor(config.raw_peds_path),
                target_cols=["着順"],
                group_cols=["騎手"],
            )
            merger.merge()
            fe = (
                FeatureEngineering(merger)
                .add_interval()
                .add_agedays()
                .add_interaction_features()  # §2b: before dummification
                .add_race_level_zscore()     # §2g: after all aggregate features
                .dumminize_kaisai()
            )
            return fe.featured_data

    job = IngestJob(_ScrapingFetcher(), _FullPipelineBuilder(), cfg)
    result = job.run(args.race_ids)
    logger.info("[ingest] %s", result)


def _retrain(args: argparse.Namespace) -> None:
    """再学習ジョブを実行する（optuna が with_tuning=True 時に必要）。"""
    import pandas as pd

    from src.constants._local_paths import LocalPaths
    from src.pipeline._retrain import RetrainConfig
    from src.pipeline._retrain import RetrainJob
    from src.training._keiba_ai_factory import KeibaAIFactory

    cfg = RetrainConfig(use_stacking=not args.no_stacking)

    featured_path = LocalPaths.FEATURED_DATA_PATH
    featured_data = pd.read_pickle(featured_path)

    job = RetrainJob(KeibaAIFactory, cfg)
    result = job.run(featured_data, vname=args.version_name, with_tuning=args.with_tuning)
    logger.info("[retrain] %s", result)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="継続学習パイプライン")
    sub = parser.add_subparsers(dest="job", required=True)

    # ingest サブコマンド
    ingest_p = sub.add_parser("ingest", help="終了レースを日次取込")
    ingest_p.add_argument("--race-id", dest="race_ids", nargs="+", required=True, help="対象 race_id")

    # retrain サブコマンド
    retrain_p = sub.add_parser("retrain", help="全データで週次再学習")
    retrain_p.add_argument("--version-name", default=None, help="バージョン名（省略時は日付自動生成）")
    retrain_p.add_argument("--no-stacking", action="store_true", help="スタッキングを使わない（LightGBM のみ）")
    retrain_p.add_argument("--with-tuning", action="store_true", help="Optuna ハイパラ探索を実行する")

    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    from src.constants._logging_config import setup_logging

    setup_logging()
    args = _parse_args(argv)
    if args.job == "ingest":
        _ingest(args)
    elif args.job == "retrain":
        _retrain(args)


if __name__ == "__main__":
    main()
