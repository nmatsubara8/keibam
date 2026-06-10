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
import os
from typing import Sequence

logger = logging.getLogger(__name__)


def _ingest(args: argparse.Namespace) -> None:
    """取込ジョブを実行する（selenium / bs4 が実行時に必要）。"""
    from src.pipeline._ingestion import IngestConfig
    from src.pipeline._ingestion import IngestJob

    # Phase 1: --force フラグを IngestConfig に伝搬（DB 行の事前 DELETE を有効化）
    cfg = IngestConfig(force=getattr(args, "force", False))

    # I/O アダプタ: preparing を遅延 import して DI
    class _ScrapingFetcher:
        """netkeiba から実データを取得する実 adapter（bs4/selenium 依存）。"""

        @staticmethod
        def _load_indexed(path: str) -> "pd.DataFrame":
            """pickle を読み込み race_id をインデックスに正規化して返す。

            ResultsProcessor を経由すると _preprocess() が走り pickle の状態に
            依存した例外が発生するため、load_raw で直接読む。
            """
            from src.pipeline._ingestion import load_raw
            df = load_raw(path)
            if df.empty:
                return df
            if "race_id" in df.columns and df.index.name != "race_id":
                df = df.set_index("race_id")
            return df

        def fetch_results(self, race_ids):
            return _ScrapingFetcher._load_indexed(cfg.raw_results_path)

        def fetch_race_info(self, race_ids):
            return _ScrapingFetcher._load_indexed(cfg.raw_race_info_path)

        def fetch_return_tables(self, race_ids):
            return _ScrapingFetcher._load_indexed(cfg.raw_return_tables_path)

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
                .dumminize_sex()
                .dumminize_weather()
                .dumminize_race_type()
                .dumminize_ground_state1()
                .dumminize_ground_state2()
                .dumminize_around()
                .dumminize_race_class()
                .encode_jockey_id()
                .encode_trainer_id()
                .encode_owner_id()
                .encode_breeder_id()
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
                return (
                    FeatureEngineering(merger)
                    .add_interval().add_agedays()
                    .add_interaction_features()
                    .add_race_level_zscore()
                    .dumminize_kaisai().dumminize_sex().dumminize_weather()
                    .dumminize_race_type().dumminize_ground_state1().dumminize_ground_state2()
                    .dumminize_around().dumminize_race_class()
                    .encode_jockey_id().encode_trainer_id().encode_owner_id().encode_breeder_id()
                ).featured_data

        featured_data = _Builder().build(cfg_ing)
        featured_data.to_pickle(featured_path)
        logger.info("[retrain] featured_data.pkl を生成しました shape=%s", featured_data.shape)
    else:
        featured_data = pd.read_pickle(featured_path)

    job = RetrainJob(KeibaAIFactory, cfg)
    result = job.run(featured_data, vname=args.version_name, with_tuning=args.with_tuning)
    logger.info("[retrain] %s", result)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="継続学習パイプライン")
    sub = parser.add_subparsers(dest="job", required=True)

    # ingest サブコマンド
    ingest_p = sub.add_parser("ingest", help="終了レースを日次取込")
    ingest_p.add_argument("--race-id", dest="race_ids", nargs="+", type=int, required=True, help="対象 race_id")
    # Phase 1: 誤情報修正時に既存 DB 行を削除してから再取込するためのフラグ
    ingest_p.add_argument(
        "--force",
        action="store_true",
        help="既存 DB 行を削除してから再取込（誤情報修正時に使用）",
    )

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
