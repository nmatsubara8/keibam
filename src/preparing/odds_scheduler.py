"""段階オッズ取得スケジューラ（CLI エントリ）。

前日 / 数時間前 / 30分前 / 直前 の各タイミングで cron から起動し、対象レースの
オッズを取得して `OddsSnapshot` を集約 pickle に冪等追記する。VPS の cron 運用を想定
（KB 9.2）。I/O（スクレイピング・ファイル永続化）はここに閉じ込め、ドメイン純粋層
（_odds_snapshot の DTO/マージ/整形）を呼び出す。

使用例:
    # 手動指定（race_id と発走時刻を明示）
    python -m src.preparing.odds_scheduler --phase just_before \
        --race-id 202401010101 --post-time 2024-01-01T15:40 --bet-type tansho

    # 自動検出（当日の開催レースと発走時刻をスクレイプし、window 内のレースを取得）
    python -m src.preparing.odds_scheduler --auto --window-minutes 60

post-time（発走時刻）はフェーズ判定に用いる minutes_to_post の基準。複数 race を
渡す場合は --race-id を繰り返し指定する。--auto では race_id・発走時刻・フェーズが
すべて自動決定されるため cron からの定期実行に適する（scripts/odds_snapshot.sh）。
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import pickle
import time
from typing import Sequence

from src.constants._bet_types import BetType
from src.constants._local_paths import LocalPaths
from src.constants._logging_config import setup_logging
from src.constants._odds_phases import OddsPhase
from src.preparing._odds_snapshot import OddsSnapshot
from src.preparing._odds_snapshot import OddsSnapshotScraper
from src.preparing._odds_snapshot import merge_snapshots
from src.preparing._rate_limiter import polite_interval

logger = logging.getLogger(__name__)

_VALID_PHASES = (
    OddsPhase.PREV_DAY,
    OddsPhase.HOURS_BEFORE,
    OddsPhase.THIRTY_MIN,
    OddsPhase.T10,
    OddsPhase.T5,
    OddsPhase.T0,
    OddsPhase.JUST_BEFORE,  # 旧名（後方互換）
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


def _upsert_db(new: Sequence[OddsSnapshot]) -> None:
    """SQLite `raw_odds_snapshots` へ冪等 upsert する（pickle 揮発時の保険、非致命）。

    主キーは (race_id, captured_at, bet_type, combo)。取得時刻ごとに行が積まれるため、
    pickle 側のフェーズ単位の dedup とは異なり DB には時系列の全スナップショットが残る。
    失敗しても pickle 永続化は完了している前提で warning のみ吐いて続行する。
    """
    if not new:
        return
    try:
        import pandas as pd  # noqa: PLC0415

        from src.preparing._odds_snapshot import snapshots_to_records
        from src.storage import RawDataRepo

        df = pd.DataFrame(snapshots_to_records(new))
        inserted = RawDataRepo().upsert("raw_odds_snapshots", df)
        logger.info("odds_scheduler: DB upsert raw_odds_snapshots: %d rows inserted", inserted)
    except Exception as e:  # noqa: BLE001
        logger.warning("odds_scheduler: DB upsert 失敗 (non-fatal): %s", e)


def persist(new: Sequence[OddsSnapshot], path: str) -> list[OddsSnapshot]:
    """新規スナップショットを既存に冪等マージして保存し、マージ結果を返す。"""
    existing = load_snapshots(path)
    merged = merge_snapshots(existing, new)
    save_snapshots(merged, path)
    _upsert_db(new)
    return merged


def run(
    race_ids: Sequence[str],
    post_time: dt.datetime,
    bet_types: Sequence[str],
    scraper: OddsSnapshotScraper,
    path: str = LocalPaths.RAW_ODDS_SNAPSHOT_PATH,
    captured_at: dt.datetime | None = None,
    request_delay: float = 0.0,
    persist_every: int = 0,
) -> list[OddsSnapshot]:
    """指定レース・馬券種のオッズを取得し冪等追記する（DI で scraper を受け取る）。

    request_delay>0 のときは各リクエスト間に polite_interval（最低 1 秒+揺らぎ）の
    間隔を挟む（過去レースの大量取得向け。単一 fetch 経路は時間上限のみで間隔を
    持たないため）。既定 0.0 はライブ取得（odds_watch）の従来挙動を保持する。

    persist_every>0 のときは N レースごとに途中保存し進捗ログを出す（大量バックフィルで
    途中中断しても取得済み分を失わない＋生存確認のため）。既定 0 は最後に 1 回だけ保存
    （ライブ取得の従来挙動）。
    """
    captured_at = captured_at or dt.datetime.now()
    collected: list[OddsSnapshot] = []
    first = True
    total = len(race_ids)
    empty_races: list[str] = []
    batch_added = 0
    for i, race_id in enumerate(race_ids, 1):
        race_count = 0
        for bet_type in bet_types:
            if not first and request_delay > 0:
                time.sleep(polite_interval(request_delay))
            first = False
            try:
                snaps = scraper.capture(race_id, bet_type, post_time, captured_at)
                collected.extend(snaps)
                race_count += len(snaps)
            except Exception as e:  # 1 レースの失敗で全体を止めない（リジューム前提）
                logger.warning("capture failed race_id=%s bet_type=%s: %s", race_id, bet_type, e)
        batch_added += race_count
        if race_count == 0:
            empty_races.append(str(race_id))
        if persist_every and i % persist_every == 0:
            merged = persist(collected, path)
            logger.info(
                "[odds capture] 進捗 %d/%d レース完了（本バッチ +%d 件 / 累計 %d 件）",
                i, total, batch_added, len(merged),
            )
            collected = []
            batch_added = 0
    final = persist(collected, path)
    if empty_races and persist_every:
        logger.warning(
            "[odds capture] %d/%d レースが 0 件（確定オッズ未配信 or 描画失敗の可能性）。例: %s",
            len(empty_races), total, empty_races[:5],
        )
    return final


def build_race_post_times(
    race_ids: Sequence[str], race_times: Sequence[str], date_str: str
) -> list[tuple[str, dt.datetime]]:
    """(race_id, "HH:MM") の対から (race_id, 発走 datetime) を構築する（純粋関数）。

    発走時刻が取得できなかったレース（空文字）はスキップする。
    """
    base = dt.datetime.strptime(date_str, "%Y%m%d")
    out: list[tuple[str, dt.datetime]] = []
    for race_id, time_str in zip(race_ids, race_times, strict=True):
        if not time_str:
            continue
        try:
            hh, mm = time_str.split(":")
            out.append((race_id, base.replace(hour=int(hh), minute=int(mm))))
        except ValueError:
            logger.warning("発走時刻を解釈できません: race_id=%s time=%r", race_id, time_str)
    return out


def select_checkpoint_races(
    pairs: Sequence[tuple[str, dt.datetime]],
    now: dt.datetime,
) -> list[tuple[str, dt.datetime, str]]:
    """いま取得すべきレースを返す（締切までの残り時間ベースの取得スケジュール）。

    - 締切直前（残り ≤ DENSE_WINDOW_MIN 分）: 毎ティック取得して推移を密に記録する。
    - それより前: SPARSE_CHECKPOINT_MINUTES（発走 N 分前 ±許容幅）でのみ取得（疎）。

    Returns
    -------
    list[(race_id, post_time, phase)] : phase は minutes_to_post から分類した識別子
        （メタデータ。実保存時に make_snapshot が classify_phase で再付与する）。
    無駄な全レース取得を避け、タイマー実行（cron */2 分等）のたびに「いま取るべき
    レース」だけを返す純粋関数。
    """
    from src.constants._odds_dynamics import CHECKPOINT_TOLERANCE_MIN
    from src.constants._odds_dynamics import DENSE_WINDOW_MIN
    from src.constants._odds_dynamics import SPARSE_CHECKPOINT_MINUTES
    from src.constants._odds_phases import classify_phase

    out: list[tuple[str, dt.datetime, str]] = []
    for race_id, post in pairs:
        mtp = (post - now).total_seconds() / 60
        if mtp < 0:
            continue  # 発走済みは対象外
        take = mtp <= DENSE_WINDOW_MIN  # 締切直前は毎ティック密に取得
        if not take:
            take = any(abs(mtp - m) <= CHECKPOINT_TOLERANCE_MIN for m in SPARSE_CHECKPOINT_MINUTES)
        if take:
            out.append((race_id, post, classify_phase(int(round(mtp)))))
    return out


def select_target_races(
    pairs: Sequence[tuple[str, dt.datetime]],
    now: dt.datetime,
    window_minutes: int,
) -> list[tuple[str, dt.datetime]]:
    """発走まで window_minutes 以内（かつ未発走）のレースだけ選ぶ（純粋関数）。

    フェーズは capture 時に minutes_to_post から自動分類されるため、ここでは
    「いま取得すべきか」だけを判定する。
    """
    return [(rid, post) for rid, post in pairs if 0 <= (post - now).total_seconds() / 60 <= window_minutes]


def run_auto(
    scraper: OddsSnapshotScraper,
    bet_types: Sequence[str],
    window_minutes: int = 60,
    date_str: str | None = None,
    path: str = LocalPaths.RAW_ODDS_SNAPSHOT_PATH,
    now: dt.datetime | None = None,
    race_id_time_fetcher=None,
) -> list[OddsSnapshot]:
    """当日の開催レースを自動検出し、発走が近いレースのオッズを取得・冪等追記する。

    cron 定期実行用エントリ。race_id_time_fetcher は DI（テスト時はスタブ、
    既定は netkeiba の開催一覧スクレイパ）。
    """
    now = now or dt.datetime.now()
    date_str = date_str or now.strftime("%Y%m%d")
    if race_id_time_fetcher is None:
        from src.preparing._scrape_shutuba import scrape_race_id_race_time_list  # noqa: PLC0415

        race_id_time_fetcher = scrape_race_id_race_time_list

    race_ids, race_times = race_id_time_fetcher(date_str)
    pairs = build_race_post_times(race_ids, race_times, date_str)
    targets = select_target_races(pairs, now, window_minutes)
    logger.info(
        "auto: date=%s 開催=%d レース, window=%d 分以内=%d レース",
        date_str, len(pairs), window_minutes, len(targets),
    )
    if not targets:
        return load_snapshots(path)

    captured_at = now
    collected: list[OddsSnapshot] = []
    for race_id, post_time in targets:
        for bet_type in bet_types:
            try:
                collected.extend(scraper.capture(race_id, bet_type, post_time, captured_at))
            except Exception as e:  # 1 レースの失敗で全体を止めない（リジューム前提）
                logger.warning("capture failed race_id=%s bet_type=%s: %s", race_id, bet_type, e)
    return persist(collected, path)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="段階オッズ取得スケジューラ")
    parser.add_argument("--auto", action="store_true", help="当日の開催レースを自動検出して取得する")
    parser.add_argument("--date", default=None, help="--auto 時の対象日 YYYYMMDD（既定: 今日）")
    parser.add_argument(
        "--window-minutes", type=int, default=60, help="--auto 時: 発走まで何分以内のレースを取得するか"
    )
    parser.add_argument("--phase", choices=_VALID_PHASES, help="取得フェーズ（手動モード時に必須）")
    parser.add_argument(
        "--race-id", dest="race_ids", action="append", help="対象 race_id（複数可、手動モード時に必須）"
    )
    parser.add_argument("--post-time", help="発走時刻 ISO8601（例 2024-01-01T15:40、手動モード時に必須）")
    parser.add_argument(
        "--bet-type", dest="bet_types", action="append", default=None, help="馬券種（既定: tansho。複数可）"
    )
    parser.add_argument("--path", default=LocalPaths.RAW_ODDS_SNAPSHOT_PATH, help="集約 pickle の保存先")
    # --waiting-time は Playwright 移行で不要（wait_for_selector で描画完了を判定）。
    # 既存 cron コマンド互換のため引数は受け付けるが内部では使用しない。
    parser.add_argument("--waiting-time", type=int, default=10, help="（廃止予定・無視される）描画待ち秒数")
    args = parser.parse_args(argv)
    if not args.auto and (not args.phase or not args.race_ids or not args.post_time):
        parser.error("--auto を付けない場合は --phase / --race-id / --post-time が必須です")
    return args


def main(argv: Sequence[str] | None = None) -> None:
    setup_logging()
    args = _parse_args(argv)
    bet_types = args.bet_types or [BetType.TANSHO]
    scraper = OddsSnapshotScraper()

    if args.auto:
        merged = run_auto(
            scraper,
            bet_types,
            window_minutes=args.window_minutes,
            date_str=args.date,
            path=args.path,
        )
        logger.info("auto captured total_snapshots=%d", len(merged))
        return

    post_time = dt.datetime.fromisoformat(args.post_time)
    merged = run(args.race_ids, post_time, bet_types, scraper, path=args.path)
    logger.info(
        "phase=%s captured races=%d total_snapshots=%d", args.phase, len(args.race_ids), len(merged)
    )


if __name__ == "__main__":
    main()
