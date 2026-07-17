"""継続学習パイプライン CLI エントリ（薄いファサード）。

サブコマンドの実装は ``src/pipeline/commands/`` に、引数定義は ``_cli_parser`` に、横断
インフラ（DB 優先読込・ジョブ計測/記録/通知）は ``_cli_common`` に分離した。本モジュールは
パーサ→ディスパッチの配線（HANDLERS）と main だけを持つ。

使用例（cron から実行）:
    # 日次: 前日の終了レースを取込
    python -m src.pipeline.run_pipeline ingest --race-id 202401010101 202401010102
    # 週次: 全データで再学習
    python -m src.pipeline.run_pipeline retrain
"""

from __future__ import annotations

import argparse
import logging
from typing import Sequence

from src.pipeline._cli_common import _run_job
from src.pipeline.commands._backfill import (
    _backfill_horses,
    _backfill_notes,
    _backfill_peds,
    _backfill_persons,
    _backfill_yoso,
    _backfill_yoso_predictors,
)
from src.pipeline.commands._calibrate import _calibrate_ev, _calibrate_takeout
from src.pipeline.commands._evaluate import (
    _backtest,
    _doctor,
    _evaluate_odds_dynamics,
    _fetch_final_odds,
)
from src.pipeline.commands._combine import _build_combined
from src.pipeline.commands._ingest import _ingest, _rebuild_featured
from src.pipeline.commands._train import _retrain

logger = logging.getLogger(__name__)

# サブコマンド名 → handler。build_parser の subcommand と一致すること（test_cli_smoke が検証）。
HANDLERS = {
    "ingest": _ingest,
    "rebuild-featured": _rebuild_featured,
    "backfill-notes": _backfill_notes,
    "backfill-yoso": _backfill_yoso,
    "backfill-yoso-predictors": _backfill_yoso_predictors,
    "backfill-persons": _backfill_persons,
    "backfill-horses": _backfill_horses,
    "backfill-peds": _backfill_peds,
    "evaluate-odds-dynamics": _evaluate_odds_dynamics,
    "fetch-final-odds": _fetch_final_odds,
    "calibrate-takeout": _calibrate_takeout,
    "calibrate-ev": _calibrate_ev,
    "retrain": _retrain,
    "backtest": _backtest,
    "build-combined": _build_combined,
    "doctor": _doctor,
}


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """CLI 引数を解析する（サブコマンド定義は _cli_parser.build_parser に分離）。"""
    from src.pipeline._cli_parser import build_parser

    return build_parser().parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    from src.constants._logging_config import setup_logging

    setup_logging()
    args = _parse_args(argv)
    handler = HANDLERS.get(args.job)
    if handler is not None:
        _run_job(args.job, handler, args)


if __name__ == "__main__":
    main()
