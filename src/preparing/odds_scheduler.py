"""段階オッズ取得スケジューラ（CLI エントリ）。

前日 / 数時間前 / 30分前 / 直前 の各タイミングで cron から起動し、対象レースの
オッズを取得して `OddsSnapshot` を集約 pickle に冪等追記する。VPS の cron 運用を想定
（KB 9.2）。I/O（スクレイピング・ファイル永続化）はここに閉じ込め、ドメイン純粋層
（_odds_snapshot の DTO/マージ/整形）を呼び出す。

使用例:
    python -m src.preparing.odds_scheduler --phase just_before \
        --race-id 202401010101 --post-time 2024-01-01T15:40 --bet-type tansho

post-time（発走時刻）はフェーズ判定に用いる minutes_to_post の基準。複数 race を
渡す場合は --race-id を繰り返し指定する。
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import pickle
from typing import Sequence

from src.constants._bet_types import BetType
from src.constants._local_paths import LocalPaths
from src.constants._odds_phases import OddsPhase
from src.preparing._odds_snapshot import OddsSnapshot
from src.preparing._odds_snapshot import OddsSnapshotScraper
from src.preparing._odds_snapshot import merge_snapshots

_VALID_PHASES = (
    OddsPhase.PREV_DAY,
    OddsPhase.HOURS_BEFORE,
    OddsPhase.THIRTY_MIN,
    OddsPhase.JUST_BEFORE,
)


def load_snapshots(path: str) -> list[OddsSnapshot]:
    """集約 pickle を読み込む（無ければ空リスト）。"""
    if not os.path.exists(path):
        return []
    with open(path, "rb") as f:
        return pickle.load(f)


def save_snapshots(snapshots: Sequence[OddsSnapshot], path: str) -> None:
    """集約 pickle を保存する（ディレクトリは自動作成）。"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        pickle.dump(list(snapshots), f)


def persist(new: Sequence[OddsSnapshot], path: str) -> list[OddsSnapshot]:
    """新規スナップショットを既存に冪等マージして保存し、マージ結果を返す。"""
    existing = load_snapshots(path)
    merged = merge_snapshots(existing, new)
    save_snapshots(merged, path)
    return merged


def run(
    race_ids: Sequence[str],
    post_time: dt.datetime,
    bet_types: Sequence[str],
    scraper: OddsSnapshotScraper,
    path: str = LocalPaths.RAW_ODDS_SNAPSHOT_PATH,
    captured_at: dt.datetime | None = None,
) -> list[OddsSnapshot]:
    """指定レース・馬券種のオッズを取得し冪等追記する（DI で scraper を受け取る）。"""
    captured_at = captured_at or dt.datetime.now()
    collected: list[OddsSnapshot] = []
    for race_id in race_ids:
        for bet_type in bet_types:
            try:
                collected.extend(scraper.capture(race_id, bet_type, post_time, captured_at))
            except Exception as e:  # 1 レースの失敗で全体を止めない（リジューム前提）
                print(f"capture failed race_id={race_id} bet_type={bet_type}: {e}")
    return persist(collected, path)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="段階オッズ取得スケジューラ")
    parser.add_argument("--phase", required=True, choices=_VALID_PHASES, help="取得フェーズ")
    parser.add_argument("--race-id", dest="race_ids", action="append", required=True, help="対象 race_id（複数可）")
    parser.add_argument("--post-time", required=True, help="発走時刻 ISO8601（例 2024-01-01T15:40）")
    parser.add_argument(
        "--bet-type", dest="bet_types", action="append", default=None, help="馬券種（既定: tansho。複数可）"
    )
    parser.add_argument("--path", default=LocalPaths.RAW_ODDS_SNAPSHOT_PATH, help="集約 pickle の保存先")
    # --waiting-time は Playwright 移行で不要（wait_for_selector で描画完了を判定）。
    # 既存 cron コマンド互換のため引数は受け付けるが内部では使用しない。
    parser.add_argument("--waiting-time", type=int, default=10, help="（廃止予定・無視される）描画待ち秒数")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)
    post_time = dt.datetime.fromisoformat(args.post_time)
    bet_types = args.bet_types or [BetType.TANSHO]
    scraper = OddsSnapshotScraper()
    merged = run(args.race_ids, post_time, bet_types, scraper, path=args.path)
    print(f"phase={args.phase} captured races={len(args.race_ids)} total_snapshots={len(merged)}")


if __name__ == "__main__":
    main()
