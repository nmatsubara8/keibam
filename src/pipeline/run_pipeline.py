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
from typing import TYPE_CHECKING
from typing import Sequence

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)


def _resolve_race_ids(post_date: str) -> list[int]:
    """--post-date YYYYMMDD から当日の race_id リストを取得する（Playwright 必須）。"""
    from src.preparing._scrape_shutuba import scrape_race_id_race_time_list

    race_ids_str, _ = scrape_race_id_race_time_list(post_date)
    if not race_ids_str:
        raise RuntimeError(f"--post-date {post_date}: race_id を取得できませんでした（ネットワーク・HTML 構造を確認）")
    logger.info("[ingest] --post-date %s: %d レースを取得: %s", post_date, len(race_ids_str), race_ids_str[:3])
    return [int(r) for r in race_ids_str]


def _auto_migrate_db() -> None:
    """既存 pickle を SQLite へ自動移行する（DB が空のテーブルのみ、non-fatal）。

    pickle 揮発時の保険である DB が空のまま運用されるのを防ぐため、
    ingest / retrain の起動時に毎回呼ぶ（移行済みなら has_rows チェックのみで安価）。
    """
    try:
        from src.storage import RawDataRepo

        migrated = RawDataRepo().auto_migrate_all()
        if migrated:
            logger.info("[pipeline] DB auto-migrate: %s", migrated)
    except Exception as e:  # noqa: BLE001
        logger.warning("[pipeline] DB auto-migrate 失敗 (non-fatal): %s", e)


def _resolve_data_source(args) -> str:
    """使用するデータソース名を決める（CLI --source > UI 選択保存 > 既定 netkeiba）。"""
    from src.preparing._data_source import load_selected_source

    cli = getattr(args, "source", None)
    if cli:
        return cli
    return load_selected_source()


def _scrape_new_race_data(race_ids: list, source=None) -> list:
    """新規 race_id のレースデータを取得し、raw テーブルを増分更新する。

    既存 results.pkl に存在する race_id はスキップする（dedup は pipeline 側）。
    実際の取得（スクレイプ/ファイル受信）は DataSource（既定 netkeiba）に委譲する。

    Returns
    -------
    list : 今回新たに取得した race_id（pickle へマージ済み）。
    """
    from src.constants._local_paths import LocalPaths
    from src.pipeline._ingestion import existing_race_ids
    from src.pipeline._ingestion import find_new_race_ids
    from src.pipeline._ingestion import load_raw

    if source is None:
        from src.preparing._data_source import NetkeibaDataSource

        source = NetkeibaDataSource()

    existing = load_raw(LocalPaths.RAW_RESULTS_PATH)
    if not existing.empty and "race_id" in existing.columns and existing.index.name != "race_id":
        existing = existing.set_index("race_id")
    new_ids = find_new_race_ids(existing_race_ids(existing), [int(r) for r in race_ids])
    if not new_ids:
        logger.info("[ingest] 新規 race_id なし（取得不要）")
        return []

    logger.info("[ingest] 新規レース %d 件を %s から取得します", len(new_ids), source.name)
    source.acquire_races([str(r) for r in new_ids])
    return new_ids


def _scrape_new_horse_data(source=None) -> int:
    """results に存在するが horse_info に無い馬データを取得し増分更新する。

    新規レースの取込で初出走馬・地方/海外からの転入馬が現れると、horse_results /
    horse_info / peds に欠落が生じ、特徴量の馬履歴・血統が NaN になる。
    実際の取得は DataSource（既定 netkeiba）に委譲する。

    Returns
    -------
    int : 新たに取得した馬の数。
    """
    from src.constants._local_paths import LocalPaths
    from src.pipeline._ingestion import load_raw

    # KEIBA_SKIP_HORSES=1: 馬ページのインライン取得をスキップする。過去データの
    # 大量バックフィルでは ingest を「レース取込のみ」に絞り、馬ページ・血統は後段の
    # backfill-horses / backfill-peds で別途・低速に取得する方が安全（レート制限/
    # ブロック時に毎回 未取得馬を再試行して時間を浪費するのを防ぐ）。
    if os.environ.get("KEIBA_SKIP_HORSES", "").strip().lower() in ("1", "true", "yes"):
        logger.info("[ingest] KEIBA_SKIP_HORSES=1: 馬ページ取得をスキップ（後で backfill-horses / backfill-peds）")
        return 0

    if source is None:
        from src.preparing._data_source import NetkeibaDataSource

        source = NetkeibaDataSource()

    res = load_raw(LocalPaths.RAW_RESULTS_PATH)
    hi = load_raw(LocalPaths.RAW_HORSE_INFO_PATH)
    if res.empty or "horse_id" not in res.columns:
        return 0
    known = (
        set(hi["horse_id"].astype(str))
        if "horse_id" in hi.columns
        else set(hi.index.astype(str))
    )
    missing = sorted(set(res["horse_id"].astype(str)) - known)
    if not missing:
        logger.info("[ingest] 馬データの欠落なし（馬ページの取得不要）")
        return 0

    logger.info("[ingest] 未取得の馬 %d 頭を %s から取得します", len(missing), source.name)
    source.acquire_horses(missing)

    # Phase 1: 更新された馬系 pickle を DB へ冪等 upsert（保険、non-fatal）
    try:
        from src.storage import RawDataRepo

        repo = RawDataRepo()
        for alias, path in (
            ("raw_horse_results", LocalPaths.RAW_HORSE_RESULTS_PATH),
            ("raw_horse_info", LocalPaths.RAW_HORSE_INFO_PATH),
            ("raw_peds", LocalPaths.RAW_PEDS_PATH),
        ):
            inserted = repo.upsert(alias, load_raw(path))
            logger.info("[ingest] DB upsert %s: %d rows inserted", alias, inserted)
    except Exception as e:  # noqa: BLE001
        logger.warning("[ingest] 馬データ DB upsert 失敗 (non-fatal): %s", e)
    return len(missing)


def _build_odds_signal_frame(config):
    """券種別確定オッズ（odds_snapshots）から市場歪み特徴の (race_id,馬番) フレームを作る。

    複勝/三連複/三連単の確定オッズと単勝（results）由来 Harville の差分（overlay）を
    馬単位で算出する。スナップショット未取得 or 単勝列が無ければ None（マージは no-op）。
    fetch-final-odds でオッズを貯めた後に効く（リーク無し＝発走前確定値）。
    """
    import os

    import pandas as pd

    from src.constants._local_paths import LocalPaths

    path = LocalPaths.RAW_ODDS_SNAPSHOT_PATH
    if not path or not os.path.isfile(path):
        return None
    try:
        from src.pipeline._ingestion import load_raw
        from src.preparing._odds_snapshot import build_final_odds_lookup
        from src.preparing.odds_scheduler import load_snapshots
        from src.preprocessing._market_signals import build_market_signal_frame

        snapshots = load_snapshots(path)
        if not snapshots:
            return None
        lookup = build_final_odds_lookup(snapshots)
        # 単勝オッズ {race_id: {馬番: 単勝}} を results 生データから構成
        res = load_raw(config.raw_results_path)
        if res.empty or "単勝" not in res.columns or "馬番" not in res.columns:
            return None
        res = res.reset_index()
        rid_col = "race_id" if "race_id" in res.columns else res.columns[0]
        res["_rid"] = res[rid_col].astype(str).str.replace(r"\.0$", "", regex=True)
        res["_um"] = pd.to_numeric(res["馬番"], errors="coerce")
        res["_tan"] = pd.to_numeric(res["単勝"], errors="coerce")
        win_by_race: dict = {}
        for rid, g in res.dropna(subset=["_um", "_tan"]).groupby("_rid"):
            win_by_race[rid] = {
                int(u): float(o) for u, o in zip(g["_um"], g["_tan"], strict=False) if o > 0
            }
        frame = build_market_signal_frame(lookup, win_by_race)
        return frame if not frame.empty else None
    except Exception as e:  # noqa: BLE001 — オッズ特徴は任意。失敗してもパイプラインは継続
        logger.warning("[featured] 市場歪み特徴の構築に失敗（スキップ）: %s", e)
        return None


def _build_featured_data(config):
    """raw pickle 群から DataMerger + FeatureEngineering で featured_data を生成する。

    スクレイプ・取得は行わず、現在の data/raw/*.pkl から特徴量を再構築する。
    ingest（IngestJob 経由）と rebuild-featured で共用する単一の生成ロジック。
    """
    from src.preprocessing._data_merger import DataMerger
    from src.preprocessing._feature_engineering import FeatureEngineering
    from src.preprocessing._horse_info_processor import HorseInfoProcessor
    from src.preprocessing._horse_results_processor import HorseResultsProcessor
    from src.preprocessing._peds_processor import PedsProcessor
    from src.preprocessing._race_info_processor import RaceInfoProcessor
    from src.preprocessing._results_processor import ResultsProcessor
    from src.constants._feature_cols import AGG_TARGET_COLS
    from src.constants._local_paths import LocalPaths

    def _read_optional_pickle(path):
        """存在すれば DataFrame を読む。無ければ None（マージは no-op）。"""
        import os

        import pandas as pd

        if path and os.path.isfile(path):
            try:
                return pd.read_pickle(path)
            except Exception:  # noqa: BLE001 — 壊れていてもパイプライン全体は止めない
                return None
        return None

    merger = DataMerger(
        ResultsProcessor(config.raw_results_path),
        RaceInfoProcessor(config.raw_race_info_path),
        HorseResultsProcessor(config.raw_horse_results_path),
        HorseInfoProcessor(config.raw_horse_info_path),
        PedsProcessor(config.raw_peds_path),
        target_cols=AGG_TARGET_COLS,
        group_cols=["騎手"],
        training_df=_read_optional_pickle(LocalPaths.RAW_TRAINING_PATH),
        paddock_df=_read_optional_pickle(LocalPaths.RAW_PADDOCK_PATH),
        comment_df=_read_optional_pickle(LocalPaths.RAW_COMMENT_PATH),
        yoso_marks_df=_read_optional_pickle(LocalPaths.RAW_YOSO_MARKS_PATH),
        person_yearly_df=_read_optional_pickle(LocalPaths.RAW_PERSON_YEARLY_PATH),
        yoso_predictor_df=_read_optional_pickle(LocalPaths.RAW_YOSO_PREDICTOR_PATH),
        odds_signals_df=_build_odds_signal_frame(config),
    )
    merger.merge()
    fe = (
        FeatureEngineering(merger)
        .add_interval()
        .add_agedays()
        .add_derived_features()      # §2m(Batch A): log odds / 斤量比 / 休み明け
        .add_date_cyclical()         # 開催日の周期性（sin/cos・うるう年込み）
        .add_interaction_features()  # §2b: before dummification
        .add_race_level_zscore()     # §2g: after all aggregate features
        .dumminize_kaisai()
        .dumminize_sex()
        .dumminize_weather()
        .dumminize_race_type()
        .dumminize_ground_state1()
        .dumminize_ground_state2()
        .dumminize_around()
        .add_race_class_level()      # 現レースの格を順序値化（dumminize 前。one-hot と併用）
        .dumminize_race_class()
        .dumminize_paddock_eval()    # 当日ノート: パドック評価 A/B/穴 → one-hot
        .encode_video_grade()        # 当日ノート: 映像グレード A/B/C → 順序
        .encode_training_eval()      # 当日ノート: 調教評価 → ベストエフォート順序
        .drop_text_note_columns()    # コメント類はモデル特徴から除外（raw は保持）
        .encode_horse_id()
        .encode_jockey_id()
        .encode_trainer_id()
        .encode_owner_id()
        .encode_breeder_id()
    )
    return fe.featured_data


def _rebuild_featured(args: argparse.Namespace) -> None:
    """raw pickle から featured_data を再生成する（スクレイプ・取得なし）。

    DB から raw pickle を復元した後など、raw は更新済みだが featured が古い場合に使う。
    """
    from src.pipeline._ingestion import IngestConfig, _save_featured_phase2, save_raw

    cfg = IngestConfig()
    featured = _build_featured_data(cfg)
    save_raw(featured, cfg.featured_data_path)
    _save_featured_phase2(featured, cfg)
    n_races = featured.index.nunique() if hasattr(featured, "index") else 0
    logger.info(
        "[rebuild-featured] featured_data 再生成完了: %d 行 / %d レース → %s",
        len(featured), n_races, cfg.featured_data_path,
    )


def _backfill_notes(args: argparse.Namespace) -> None:
    """既存 raw の全 race_id に対し当日ノート（調教/パドック/コメント）だけを取得する。

    コア取得（results/horse/peds）とは独立。バルク再取得の後に走らせ、年代ゲート
    （KEIBA_RACE_DAY_NOTES_MIN_YEAR・既定2010）で近年のみに絞る。ノートは race_id 単位で
    冪等に総入替されるため、何度でも中断・再開できる。完了後は rebuild-featured で反映。
    """
    from src.pipeline._ingestion import IngestConfig
    from src.pipeline._ingestion import existing_race_ids
    from src.pipeline._ingestion import load_raw
    from src.preparing._data_source import create_data_source

    cfg = IngestConfig()
    results = load_raw(cfg.raw_results_path)
    # raw_results は race_id を列に持つ（RangeIndex）ことがある。index に揃えてから列挙する
    if not results.empty and "race_id" in results.columns and results.index.name != "race_id":
        results = results.set_index("race_id")
    ids = sorted(str(r) for r in existing_race_ids(results))
    if getattr(args, "min_year", None):
        ids = [r for r in ids if r[:4].isdigit() and int(r[:4]) >= args.min_year]
    # 中断・再開: 既に training ノートを取得済みの race_id を除外（--no-skip-existing で無効）
    if not getattr(args, "no_skip_existing", False):
        from src.constants._local_paths import LocalPaths

        done = existing_race_ids(load_raw(LocalPaths.RAW_TRAINING_PATH))
        done = {str(r) for r in done}
        before = len(ids)
        ids = [r for r in ids if r not in done]
        if before != len(ids):
            logger.info("[backfill-notes] 取得済み %d レースをスキップ（再開）", before - len(ids))
    if getattr(args, "limit", None):
        ids = ids[: args.limit]
    if not ids:
        logger.info("[backfill-notes] 対象 race_id なし（全件取得済み or raw_results が空）")
        return
    source = create_data_source(_resolve_data_source(args))
    logger.info("[backfill-notes] %s で %d レースの当日ノートを取得します", source.name, len(ids))
    source.acquire_race_day_notes(ids)
    logger.info("[backfill-notes] 完了。rebuild-featured で featured に反映してください")


def _backfill_yoso(args: argparse.Namespace) -> None:
    """既存 raw の全 race_id に対し予想印（コンセンサス用ロング）だけを取得する。

    コア取得と独立。年代ゲート（KEIBA_YOSO_MARKS_MIN_YEAR・既定2010）で近年に絞る。
    race_id 単位で冪等総入替のため、取得済みを除外して中断・再開できる。完了後 rebuild-featured。
    """
    from src.constants._local_paths import LocalPaths
    from src.pipeline._ingestion import existing_race_ids
    from src.pipeline._ingestion import load_raw
    from src.preparing._data_source import create_data_source

    results = load_raw(LocalPaths.RAW_RESULTS_PATH)
    if not results.empty and "race_id" in results.columns and results.index.name != "race_id":
        results = results.set_index("race_id")
    ids = sorted(str(r) for r in existing_race_ids(results))
    if getattr(args, "min_year", None):
        ids = [r for r in ids if r[:4].isdigit() and int(r[:4]) >= args.min_year]
    # 中断・再開: 既に予想印を取得済みの race_id を除外（--no-skip-existing で無効）
    if not getattr(args, "no_skip_existing", False):
        done = existing_race_ids(load_raw(LocalPaths.RAW_YOSO_MARKS_PATH))
        done = {str(r) for r in done}
        before = len(ids)
        ids = [r for r in ids if r not in done]
        if before != len(ids):
            logger.info("[backfill-yoso] 取得済み %d レースをスキップ（再開）", before - len(ids))
    if getattr(args, "limit", None):
        ids = ids[: args.limit]
    if not ids:
        logger.info("[backfill-yoso] 対象 race_id なし（全件取得済み or raw_results が空）")
        return
    source = create_data_source(_resolve_data_source(args))
    logger.info("[backfill-yoso] %s で %d レースの予想印を取得します", source.name, len(ids))
    source.acquire_yoso_marks(ids)
    logger.info("[backfill-yoso] 完了。rebuild-featured で featured に反映してください")


def _backfill_yoso_predictors(args: argparse.Namespace) -> None:
    """raw_yoso_marks の predictor_yid に対し予想家スキル prior だけを取得する（独立）。

    予想家プールは小さい。取得済み predictor_yid は除外して中断・再開できる。
    完了後 rebuild-featured で profile-skill 加重に反映される。
    """
    from src.constants._local_paths import LocalPaths
    from src.pipeline._ingestion import load_raw
    from src.preparing._data_source import create_data_source

    marks = load_raw(LocalPaths.RAW_YOSO_MARKS_PATH)
    if marks.empty or "predictor_yid" not in marks.columns:
        logger.info("[backfill-yoso-predictors] yoso_marks が空 or predictor_yid 列なし")
        return
    yids = sorted(set(marks["predictor_yid"].dropna().astype(str)))
    if not getattr(args, "no_skip_existing", False):
        done = load_raw(LocalPaths.RAW_YOSO_PREDICTOR_PATH)
        done_set = {str(y) for y in done.index} if not done.empty else set()
        before = len(yids)
        yids = [y for y in yids if y not in done_set]
        if before != len(yids):
            logger.info("[backfill-yoso-predictors] 取得済み %d 人をスキップ（再開）", before - len(yids))
    if getattr(args, "limit", None):
        yids = yids[: args.limit]
    if not yids:
        logger.info("[backfill-yoso-predictors] 対象なし（全件取得済み）")
        return
    source = create_data_source(_resolve_data_source(args))
    logger.info("[backfill-yoso-predictors] %s で %d 人の予想家スキルを取得します", source.name, len(yids))
    source.acquire_yoso_predictors(yids)
    logger.info("[backfill-yoso-predictors] 完了。rebuild-featured で featured に反映してください")


def _backfill_persons(args: argparse.Namespace) -> None:
    """results の jockey_id/trainer_id に対し人物の年度別成績だけを取得する（コア取得と独立）。

    (entity_type, entity_id) 単位で冪等総入替。取得済み entity_id は除外して中断・再開できる。
    完了後 rebuild-featured で as-of 結合される。
    """
    from src.constants._local_paths import LocalPaths
    from src.pipeline._ingestion import load_raw
    from src.preparing._data_source import create_data_source
    from src.preparing._person_yearly import canon_person_id

    res = load_raw(LocalPaths.RAW_RESULTS_PATH)
    hi = load_raw(LocalPaths.RAW_HORSE_INFO_PATH)
    if res.empty:
        logger.info("[backfill-persons] results が空")
        return
    types = (args.types.split(",") if getattr(args, "types", None)
             else ["jockey", "trainer", "owner", "breeder"])
    pairs: list[tuple] = []
    for etype in types:
        col = f"{etype}_id"
        # jockey/trainer/owner は results、breeder は horse_info から id を引く
        src = res if col in res.columns else (hi if col in hi.columns else None)
        if src is None:
            logger.warning("[backfill-persons] %s 列が results/horse_info に無し。スキップ", col)
            continue
        # 正準化（jockey/trainer は5桁ゼロ埋め、owner/breeder は素通し）。重複排除。
        for eid in sorted({canon_person_id(etype, v) for v in src[col].dropna()}):
            pairs.append((etype, eid))
    # 中断・再開: 既に取得済み（person_yearly の (entity_type, entity_id)）を除外
    if not getattr(args, "no_skip_existing", False):
        done = load_raw(LocalPaths.RAW_PERSON_YEARLY_PATH)
        if not done.empty and "entity_type" in done.columns:
            eid_series = (done.index.astype(str) if done.index.name == "entity_id"
                          else done["entity_id"].astype(str))
            done_set = set(zip(done["entity_type"].astype(str), eid_series, strict=False))
            before = len(pairs)
            pairs = [p for p in pairs if p not in done_set]
            if before != len(pairs):
                logger.info("[backfill-persons] 取得済み %d 人をスキップ（再開）", before - len(pairs))
    if getattr(args, "limit", None):
        pairs = pairs[: args.limit]
    if not pairs:
        logger.info("[backfill-persons] 対象なし（全件取得済み or 列なし）")
        return
    source = create_data_source(_resolve_data_source(args))
    logger.info("[backfill-persons] %s で %d 人の年度別成績を取得します", source.name, len(pairs))
    source.acquire_persons(pairs)
    logger.info("[backfill-persons] 完了。rebuild-featured で featured に反映してください")


def _backfill_horses(args: argparse.Namespace) -> None:
    """既存 raw(results) の全 horse_id に対し馬ページ(horse_results/horse_info)を取得する。

    fetch_history.py でレース層(results/race_info/return)を揃えた後、本ジョブで馬層を
    網羅取得する。血統(peds)はブロックされやすく大量なので KEIBA_SKIP_PEDS=1 で分離し、
    後段の backfill-peds に委ねるのが安全（既定でも acquire_horses が env を尊重）。
    horse_results.pkl は horse_id 単位で冪等なため、取得済みを除外して中断・再開できる。
    完了後 backfill-peds → rebuild-featured。
    """
    from src.constants._local_paths import LocalPaths
    from src.pipeline._ingestion import load_raw
    from src.preparing._data_source import create_data_source

    res = load_raw(LocalPaths.RAW_RESULTS_PATH)
    if res.empty or "horse_id" not in res.columns:
        logger.info("[backfill-horses] 対象 horse_id なし（raw_results が空 or horse_id 列なし）")
        return
    ids = sorted(set(res["horse_id"].astype(str)))
    # 中断・再開: 既に horse_results 取得済み（index に居る horse_id）を除外
    if not getattr(args, "no_skip_existing", False):
        hr = load_raw(LocalPaths.RAW_HORSE_RESULTS_PATH)
        done = {str(h) for h in hr.index} if not hr.empty else set()
        before = len(ids)
        ids = [h for h in ids if h not in done]
        if before != len(ids):
            logger.info("[backfill-horses] 取得済み %d 頭をスキップ（再開）", before - len(ids))
    if getattr(args, "limit", None):
        ids = ids[: args.limit]
    if not ids:
        logger.info("[backfill-horses] 対象 horse_id なし（全件取得済み）")
        return
    source = create_data_source(_resolve_data_source(args))
    logger.info("[backfill-horses] %s で %d 頭の馬ページを取得します", source.name, len(ids))
    source.acquire_horses(ids)
    logger.info(
        "[backfill-horses] 完了。KEIBA_SKIP_PEDS=1 で回した場合は backfill-peds、"
        "その後 rebuild-featured で反映してください"
    )


def _backfill_peds(args: argparse.Namespace) -> None:
    """既存 raw の全 horse_id に対し血統(peds)だけを取得する（馬ページ取得と独立）。

    KEIBA_SKIP_PEDS=1 で馬ページ(horse_results/horse_info)を先に網羅取得した後、
    本ジョブで血統を低レートで後追いする。peds.pkl は horse_id 単位で冪等総入替の
    ため、取得済みを除外して中断・再開できる。完了後 rebuild-featured で反映。
    """
    from src.constants._local_paths import LocalPaths
    from src.pipeline._ingestion import load_raw
    from src.preparing._data_source import create_data_source

    res = load_raw(LocalPaths.RAW_RESULTS_PATH)
    if res.empty or "horse_id" not in res.columns:
        logger.info("[backfill-peds] 対象 horse_id なし（raw_results が空 or horse_id 列なし）")
        return
    ids = sorted(set(res["horse_id"].astype(str)))
    # 中断・再開: 既に peds 取得済み（peds.pkl の index）を除外
    if not getattr(args, "no_skip_existing", False):
        peds = load_raw(LocalPaths.RAW_PEDS_PATH)
        done = {str(h) for h in peds.index} if not peds.empty else set()
        before = len(ids)
        ids = [h for h in ids if h not in done]
        if before != len(ids):
            logger.info("[backfill-peds] 取得済み %d 頭をスキップ（再開）", before - len(ids))
    if getattr(args, "limit", None):
        ids = ids[: args.limit]
    if not ids:
        logger.info("[backfill-peds] 対象 horse_id なし（全件取得済み）")
        return
    source = create_data_source(_resolve_data_source(args))
    logger.info("[backfill-peds] %s で %d 頭の血統を取得します", source.name, len(ids))
    source.acquire_peds(ids)
    logger.info("[backfill-peds] 完了。rebuild-featured で featured に反映してください")


def _ingest(args: argparse.Namespace) -> None:
    """取込ジョブを実行する（selenium / bs4 が実行時に必要）。"""
    from src.pipeline._ingestion import IngestConfig
    from src.pipeline._ingestion import IngestJob

    # Phase 1: pickle → DB の自動移行（DB が空の場合のみ実行される）
    _auto_migrate_db()

    # データソース（既定 netkeiba / UI で選択した jravan 等）を生成
    from src.preparing._data_source import create_data_source

    source = create_data_source(_resolve_data_source(args))
    logger.info("[ingest] データソース: %s", source.name)

    # --post-date が指定された場合は race_id をソースから取得する
    if getattr(args, "post_date", None):
        args.race_ids = source.resolve_race_ids(args.post_date)
        logger.info("[ingest] --post-date %s: %d レース", args.post_date, len(args.race_ids))

    # 新規レースのデータ取得 → raw テーブル増分更新。
    # 取得失敗時は既存 pickle のみで継続する（冪等・リジューム前提）。
    scraped_new: list = []
    try:
        scraped_new = _scrape_new_race_data(list(args.race_ids), source=source)
    except Exception as e:  # noqa: BLE001
        logger.warning("[ingest] 新規レースの取得に失敗 (既存データのみで継続): %s", e)

    # 新規レースに含まれる未知の馬（初出走・転入）の馬ページ・血統を取得。
    # 失敗しても既存の馬データで featured 再生成は可能なため non-fatal。
    scraped_horses = 0
    try:
        scraped_horses = _scrape_new_horse_data(source=source)
    except Exception as e:  # noqa: BLE001
        logger.warning("[ingest] 馬データの取得に失敗 (既存データのみで継続): %s", e)
    finally:
        source.close()

    # Phase 1: --force フラグを IngestConfig に伝搬（DB 行の事前 DELETE を有効化）。
    # スクレイプで新規レース・新規馬を取り込んだ場合も force 扱いにする: 新規行は
    # 既に pickle へマージ済みのため、IngestJob の dedup をスキップしないと
    # 「新規なし」と誤判定され featured 再生成と DB upsert が走らない。
    cfg = IngestConfig(
        force=getattr(args, "force", False) or bool(scraped_new) or scraped_horses > 0
    )

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
            return _build_featured_data(config)

    job = IngestJob(_ScrapingFetcher(), _FullPipelineBuilder(), cfg)
    result = job.run(args.race_ids)
    logger.info("[ingest] %s", result)


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

    job = RetrainJob(KeibaAIFactory, cfg)
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


def _evaluate_odds_dynamics(args: argparse.Namespace) -> None:
    """オッズ力学モデル（Dirichlet/Kalman/Particle/Ensemble）の比較評価ジョブ。

    蓄積スナップショットを時系列 holdout で分割し、各モデルの精度を比較して
    models/odds_dynamics_eval.json と models/odds_gravity.json を更新する。
    結果はモデルラボの「オッズ力学モデル」タブに表示される。
    """
    from src.constants._bet_types import BetType
    from src.constants._local_paths import LocalPaths
    from src.pipeline._ingestion import load_raw
    from src.preparing.odds_scheduler import load_snapshots
    from src.training._odds_dynamics_eval import dynamics_eval_path
    from src.training._odds_dynamics_eval import evaluate_dynamics_models
    from src.training._odds_dynamics_eval import race_winners
    from src.training._odds_dynamics_eval import save_dynamics_eval
    from src.training._odds_feature_builder import snapshots_to_phase_table
    from src.training._odds_gravity import gravity_path
    from src.training._odds_gravity import save_gravity
    from src.training._simplex import race_share_sequences

    snapshots = load_snapshots(LocalPaths.RAW_ODDS_SNAPSHOT_PATH)
    if not snapshots:
        logger.warning("[odds-dynamics] スナップショットがありません（odds_watch の蓄積待ち）")
        return
    table = snapshots_to_phase_table(snapshots, BetType.TANSHO)
    sequences = race_share_sequences(table)
    if len(sequences) < 5:
        logger.warning("[odds-dynamics] 評価には 5 レース以上の系列が必要です（現在 %d）", len(sequences))
        return

    # 勝ち馬 log-loss 指標のため results から race_id → 勝ち馬番を導出（無ければ NaN のまま）
    winners = race_winners(load_raw(LocalPaths.RAW_RESULTS_PATH))
    if not winners:
        logger.info("[odds-dynamics] 勝ち馬データが未取得のため winner_logloss は NaN になります")
    evaluation = evaluate_dynamics_models(sequences, holdout_frac=args.holdout_frac, winners=winners)
    save_dynamics_eval(evaluation, dynamics_eval_path("models"))
    save_gravity(evaluation["gravity"], gravity_path("models"))
    for name, metrics in evaluation["results"].items():
        logger.info("[odds-dynamics] %s: KL=%.4f mae=%.4f mape=%.3f",
                    name, metrics["kl_mean"], metrics["share_mae"], metrics["odds_mape"])


def _filter_final_odds_race_ids(race_ids, *, done=None, years=None, force=False, limit=None) -> list[str]:
    """確定オッズ取得の対象 race_id を 年フィルタ・resume・件数上限で絞り込む（純粋関数）。

    - years: race_id 先頭 4 桁が一致するものだけ残す。
    - done（取得済み集合）: force=False のとき done に含まれる race_id を除外（resume）。
    - limit: 先頭から limit 件に制限。
    """
    out = [str(r) for r in race_ids]
    if years:
        yrs = {str(y) for y in years}
        out = [r for r in out if r[:4] in yrs]
    if not force and done:
        done_set = {str(d) for d in done}
        out = [r for r in out if r not in done_set]
    if limit:
        out = out[: int(limit)]
    return out


def _race_ids_from_results() -> list[str]:
    """取込済みの results.pkl から全 race_id を昇順で返す（確定オッズのバックフィル元）。"""
    from src.constants._local_paths import LocalPaths
    from src.pipeline._ingestion import existing_race_ids, load_raw

    from src.storage._repo import _to_db_str

    res = load_raw(LocalPaths.RAW_RESULTS_PATH)
    if res.empty:
        return []
    if "race_id" in res.columns and res.index.name != "race_id":
        res = res.set_index("race_id")
    # race_id は int64/float64 由来があり得るため正準文字列化（"...0.0" を防ぐ）
    ids = {s for r in existing_race_ids(res) if (s := _to_db_str(r))}
    return sorted(ids)


def _fetch_final_odds(args: argparse.Namespace) -> None:
    """過去レースの最終確定オッズを全券種で取得・永続化する。

    確定後の netkeiba オッズページ（race.netkeiba.com/odds/）を券種別に取得し、
    OddsSnapshot として `data/raw/odds_snapshots.pkl` + `raw_odds_snapshots` に
    冪等永続化する。post_time=now（取得=確定後）なので phase は t0（確定オッズの代理）。
    バルク取得のためリクエスト間隔（KEIBA_SCRAPE_DELAY、既定 1 秒+揺らぎ）を挟む。

    レース選択（いずれか）:
    - ``--race-id``      個別指定。
    - ``--post-date``    当日開催の全レース（1 日分）。
    - ``--from-results`` 取込済み results.pkl の全 race_id（過去全レースのバックフィル）。

    既定では既に取得済み（snapshots にある）レースをスキップして再開可能（resume）。
    ``--force`` で再取得、``--years`` で年で絞り込み、``--limit`` で 1 回の件数を制限する。
    """
    import datetime as dt

    from src.constants._bet_types import BetType
    from src.constants._local_paths import LocalPaths
    from src.preparing import odds_scheduler
    from src.preparing._odds_snapshot import OddsSnapshotScraper

    # OddsCapturer が対応する全 8 券種（payout 側と揃える）
    default_bet_types = [
        BetType.TANSHO, BetType.FUKUSHO, BetType.WAKUREN, BetType.UMAREN,
        BetType.UMATAN, BetType.WIDE, BetType.SANRENPUKU, BetType.SANRENTAN,
    ]

    if getattr(args, "from_results", False):
        race_ids = _race_ids_from_results()
        logger.info("[fetch-final-odds] results.pkl から %d レースを対象に取得", len(race_ids))
    elif getattr(args, "post_date", None):
        race_ids = [str(r) for r in _resolve_race_ids(args.post_date)]
    else:
        race_ids = [str(r) for r in args.race_ids]

    force = getattr(args, "force", False)
    done = (
        set()
        if force
        else {str(s.race_id) for s in odds_scheduler.load_snapshots(LocalPaths.RAW_ODDS_SNAPSHOT_PATH)}
    )
    before = len(race_ids)
    race_ids = _filter_final_odds_race_ids(
        race_ids, done=done, years=getattr(args, "years", None), force=force,
        limit=getattr(args, "limit", None),
    )
    if before != len(race_ids):
        logger.info(
            "[fetch-final-odds] 絞り込み: %d → %d レース（resume/年/上限）", before, len(race_ids)
        )

    if not race_ids:
        logger.warning("[fetch-final-odds] 対象レースがありません（全て取得済み or 条件に合致せず）")
        return

    bet_types = list(args.bet_types) if getattr(args, "bet_types", None) else default_bet_types
    delay = float(os.environ.get("KEIBA_SCRAPE_DELAY", "1.0"))
    scraper = OddsSnapshotScraper()
    now = dt.datetime.now()
    # 進捗・所要見込み（1 リクエスト ~4 秒の実測値で概算）。5 レースごとに途中保存する。
    n_requests = len(race_ids) * len(bet_types)
    est_min = n_requests * 4.0 / 60.0
    logger.info(
        "[fetch-final-odds] %d レース × %d 券種 = 約 %d リクエストを取得します"
        "（間隔 ~%.1f 秒 / 推定 ~%.0f 分 / 5 レースごとに途中保存）",
        len(race_ids), len(bet_types), n_requests, max(delay, 1.0), est_min,
    )
    merged = odds_scheduler.run(
        race_ids, post_time=now, bet_types=bet_types, scraper=scraper,
        captured_at=now, request_delay=delay, persist_every=5,
    )
    logger.info("[fetch-final-odds] 完了。永続化済みスナップショット累計 %d 件", len(merged))


def _load_raw_db_first(alias: str, pickle_path: str):
    """raw データを DB 優先で読む（DB が source of truth。stale な pickle を回避）。

    DB（RawDataRepo）に行があれば DB を、無ければ pickle を返す。``(df, source)`` を返す。
    DB 復元後に pickle が古いまま（merge バグ等で縮小）でも最新データを使えるようにする。
    """
    import pandas as pd

    from src.pipeline._ingestion import load_raw

    try:
        from src.storage import RawDataRepo

        repo = RawDataRepo()
        if repo.has_rows(alias):
            df = repo.read(alias)
            if df is not None and not df.empty:
                return df, "db"
    except Exception as e:  # noqa: BLE001 — DB 不可時は pickle にフォールバック
        logger.warning("[calibrate-takeout] DB(%s) 読込失敗、pickle にフォールバック: %s", alias, e)
    df = load_raw(pickle_path)
    return (df if df is not None else pd.DataFrame()), "pickle"


def _return_processor_db_first():
    """ReturnProcessor を DB 優先で構築する（DB にあれば一時 pickle 経由で読む）。"""
    import tempfile

    from src.constants._local_paths import LocalPaths
    from src.preprocessing._return_processor import ReturnProcessor

    df, source = _load_raw_db_first("raw_return_tables", LocalPaths.RAW_RETURN_TABLES_PATH)
    if source == "db" and not df.empty:
        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tf:
            tmp = tf.name
        try:
            df.to_pickle(tmp)
            return ReturnProcessor(tmp), "db"
        finally:
            os.unlink(tmp)
    return ReturnProcessor(LocalPaths.RAW_RETURN_TABLES_PATH), "pickle"


def _calibrate_takeout(args: argparse.Namespace) -> None:
    """払戻実績 × 単勝勝率から券種別の実効控除率を逆算し永続化する。

    的中組の確定オッズ（払戻金/100）と単勝由来の Harville 確率から
    ``1 - t_eff = 確定オッズ × P_harville(的中組)`` を集計し、券種別の実効控除率を
    models/takeout_calibration.json に保存する。連系推定オッズ（HistoricalOddsProvider）
    の控除率に反映され、EV バックテスト/ライブ選定の精度を上げる。

    単勝勝率・払戻実績はいずれも DB（source of truth）優先で読む。pickle が DB 復元後に
    古いまま（merge バグで縮小等）でも、最新の全レースで較正できる。
    """
    from src.constants._results_cols import ResultsCols
    from src.policies._takeout_calibration import calibrate_takeout_from_payouts
    from src.policies._takeout_calibration import payout_lookup_from_return_processor
    from src.policies._takeout_calibration import save_takeout_calibration
    from src.policies._takeout_calibration import takeout_calibration_path
    from src.policies._takeout_calibration import tansho_odds_by_race_from_table

    # pickle のみ存在し DB が空なら移行（DB を source of truth に揃える）
    _auto_migrate_db()

    from src.constants._local_paths import LocalPaths

    results, res_src = _load_raw_db_first("raw_results", LocalPaths.RAW_RESULTS_PATH)
    if results is None or results.empty:
        logger.warning("[calibrate-takeout] results が空です。先に ingest してください")
        return
    tansho_map = tansho_odds_by_race_from_table(
        results, ResultsCols.UMABAN, ResultsCols.TANSHO_ODDS
    )
    if not tansho_map:
        logger.warning(
            "[calibrate-takeout] 単勝オッズを results から構築できませんでした"
            "（列 '%s'/'%s' を確認）", ResultsCols.UMABAN, ResultsCols.TANSHO_ODDS,
        )
        return

    rp, ret_src = _return_processor_db_first()
    payout_lookup = payout_lookup_from_return_processor(rp)
    min_samples = int(getattr(args, "min_samples", 20))
    calib = calibrate_takeout_from_payouts(tansho_map, payout_lookup, min_samples=min_samples)

    # カバレッジ診断（単勝レースと払戻レースの重なりが較正サンプル数を決める）
    payout_races = {k[0] for k in payout_lookup}
    overlap = set(tansho_map) & payout_races
    logger.info(
        "[calibrate-takeout] 単勝 %d レース(%s) / 払戻 %d 件・%d レース(%s) / 重なり %d レース",
        len(tansho_map), res_src, len(payout_lookup), len(payout_races), ret_src, len(overlap),
    )
    if len(overlap) < min_samples:
        logger.warning(
            "[calibrate-takeout] 単勝×払戻の重なりレースが %d 件と少なく、多くの券種が公称値に"
            "フォールバックします。results（単勝の元）の取得範囲を払戻と揃えてください。",
            len(overlap),
        )
    for bt, info in calib.items():
        ci = (
            f" 95%CI[{info['ci_low']:.4f},{info['ci_high']:.4f}]"
            if info.get("ci_low") is not None else ""
        )
        logger.info(
            "[calibrate-takeout] %-11s takeout=%.4f%s (n=%d, %s)",
            bt, info["takeout"], ci, info["n"], info["source"],
        )

    if getattr(args, "dry_run", False):
        logger.info("[calibrate-takeout] --dry-run 指定のため保存しません")
        return
    path = takeout_calibration_path("models")
    save_takeout_calibration(calib, path)
    logger.info("[calibrate-takeout] 保存しました → %s", path)


def _calibrate_ev(args: argparse.Namespace) -> None:
    """OOS データで補正Harville(γ,δ)/r̂較正/市場合成(α,β)を fit し models/*.json に保存する。

    Win ヘッド（<version>__win.pickle）の OOS 勝率予測から、観測着順・確定単勝オッズを
    使って3つの後段アーティファクトを最尤推定する。リーク回避のため **必ず学習年より後の年**
    を --years で指定すること（in-sample は等値写像・過学習評価に退化する。Benter §5）。
    保存物は backtest の --corrected-harville/--calibrate/--blend で読み込んで使う。
    """
    from app._data_loader import load_model_from_path, load_win_head_for
    from src.constants._local_paths import LocalPaths
    from src.pipeline._ingestion import load_raw
    from src.simulation._calibrate import fit_all

    place_path = _resolve_backtest_model_path(getattr(args, "version", None))
    win_ai = load_win_head_for(place_path)
    if win_ai is None:
        logger.error(
            "[calibrate-ev] Win ヘッド(%s__win.pickle)がありません。retrain で生成してください",
            os.path.splitext(os.path.basename(place_path))[0],
        )
        # Place ヘッドで代替（複勝モデルの勝率近似）。較正は依然 OOS で行うこと。
        win_ai = load_model_from_path(place_path)
    logger.info("[calibrate-ev] モデル: %s", os.path.basename(place_path))

    featured = load_raw(LocalPaths.FEATURED_DATA_PATH)
    if featured is None or featured.empty:
        logger.error("[calibrate-ev] featured_data がありません。先に rebuild-featured を実行してください")
        return
    years = getattr(args, "years", None)
    if years:
        yset = {str(y) for y in years}
        rid = featured.index.astype(str)
        featured = featured[rid.str[:4].isin(yset)]
        logger.info("[calibrate-ev] OOS 年 %s に絞り込み: %d 行", sorted(yset), len(featured))
    else:
        logger.warning(
            "[calibrate-ev] --years 未指定。学習年を含むと楽観バイアスになります（OOS 推奨）"
        )
    if featured.empty:
        logger.error("[calibrate-ev] 対象レースがありません（年フィルタが厳しすぎる可能性）")
        return

    if getattr(args, "dry_run", False):
        from src.simulation._calibrate import build_calibration_inputs

        inputs = build_calibration_inputs(win_ai.effective_model, featured)
        logger.info(
            "[calibrate-ev] --dry-run: レース=%d 着順揃い place=%d blend=%d 較正標本=%d（保存しません）",
            inputs.n_races, len(inputs.place_races), len(inputs.blend_races),
            int(inputs.raw_probs.size),
        )
        return

    summary = fit_all(
        win_ai.effective_model, featured,
        models_dir="models", which=tuple(args.which),
    )
    logger.info(
        "[calibrate-ev] レース=%d place=%d blend=%d 較正標本=%d",
        summary["n_races"], summary["n_place_races"], summary["n_blend_races"],
        summary["n_calib_samples"],
    )
    if "exponents" in summary:
        e = summary["exponents"]
        logger.info("[calibrate-ev] (γ,δ)=(%.4f,%.4f) → %s", e["gamma"], e["delta"], e["path"])
    if "calibrator" in summary:
        c = summary["calibrator"]
        logger.info("[calibrate-ev] r̂較正 閾値%d点 → %s", c["n_thresholds"], c["path"])
    if "blend" in summary:
        b = summary["blend"]
        logger.info("[calibrate-ev] (α,β)=(%.4f,%.4f) → %s", b["alpha"], b["beta"], b["path"])


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="継続学習パイプライン")
    sub = parser.add_subparsers(dest="job", required=True)

    # ingest サブコマンド
    ingest_p = sub.add_parser("ingest", help="終了レースを日次取込")
    race_id_group = ingest_p.add_mutually_exclusive_group(required=True)
    race_id_group.add_argument("--race-id", dest="race_ids", nargs="+", type=int, help="対象 race_id（個別指定）")
    race_id_group.add_argument(
        "--post-date",
        dest="post_date",
        metavar="YYYYMMDD",
        help="開催日を指定して当日の全 race_id を自動取得（cron 用）",
    )
    # Phase 1: 誤情報修正時に既存 DB 行を削除してから再取込するためのフラグ
    ingest_p.add_argument(
        "--force",
        action="store_true",
        help="既存 DB 行を削除してから再取込（誤情報修正時に使用）",
    )
    ingest_p.add_argument(
        "--source",
        default=None,
        help="データ取得元（netkeiba / jravan）。省略時は UI 選択 or 既定 netkeiba",
    )

    # retrain サブコマンド
    retrain_p = sub.add_parser("retrain", help="全データで週次再学習")
    retrain_p.add_argument("--version-name", default=None, help="バージョン名（省略時は日付自動生成）")
    retrain_p.add_argument("--no-stacking", action="store_true", help="スタッキングを使わない（LightGBM のみ）")
    retrain_p.add_argument(
        "--no-odds-features", action="store_true",
        help="オッズ由来の派生特徴(単勝_log・市場歪み overlay 等)を除外して学習（対市場エッジの A/B 検証用）",
    )
    retrain_p.add_argument(
        "--no-win-head", action="store_true",
        help="Win ヘッド(1着予測, <version>__win.pickle)の併行学習を行わない（Place ヘッドのみ）",
    )
    retrain_p.add_argument("--with-tuning", action="store_true", help="Optuna ハイパラ探索を実行する")
    retrain_p.add_argument(
        "--params-rank",
        type=int,
        default=None,
        help="保存済みチューニング履歴（成績順）の指定 rank のパラメータで学習する（--with-tuning と排他）",
    )
    retrain_p.add_argument(
        "--use-selected-params",
        action="store_true",
        help="UI（モデルラボ）で選択・保存したパラメータ（models/selected_params.json）で学習する",
    )
    # 手書き Optuna 探索（探索範囲・回数を制御）。いずれか指定で method="optuna" に切替。
    retrain_p.add_argument(
        "--n-trials",
        type=int,
        default=None,
        help="手書き Optuna 探索の試行回数（指定すると探索範囲を制御する optuna 方式に切替）",
    )
    retrain_p.add_argument(
        "--tuning-timeout",
        type=float,
        default=None,
        metavar="SECONDS",
        help="手書き Optuna 探索の打ち切り秒数（任意）",
    )
    retrain_p.add_argument(
        "--tuning-config",
        default=None,
        metavar="PATH",
        help="探索範囲・回数を定義した JSON 設定ファイル（src/training/_tuning_config.py 参照）",
    )
    retrain_p.add_argument(
        "--base-models",
        type=str,
        default=None,
        help="カンマ区切りの base 学習器リスト (例: lightgbm,xgboost,catboost)",
    )
    retrain_p.add_argument(
        "--base-models-config",
        type=str,
        default=None,
        help="BaseModelsConfig JSON ファイルパス",
    )

    # rebuild-featured サブコマンド（raw から featured を再生成。取得なし）
    sub.add_parser(
        "rebuild-featured",
        help="raw pickle から featured_data を再生成する（スクレイプなし。DB 復元後等に使用）",
    )

    # backfill-notes サブコマンド（既存 race_id の当日ノートのみを取得。コア取得と独立）
    bn_p = sub.add_parser(
        "backfill-notes",
        help="既存 raw の race_id に当日ノート(調教/パドック/コメント)のみ取得。後で rebuild-featured",
    )
    bn_p.add_argument("--min-year", type=int, default=None, help="この開催年以降のみ対象（既定は年代ゲートに委譲）")
    bn_p.add_argument("--limit", type=int, default=None, help="先頭 N レースのみ（動作確認用）")
    bn_p.add_argument("--source", type=str, default=None, help="データソース名（既定: 選択保存 > netkeiba）")
    bn_p.add_argument("--no-skip-existing", action="store_true", help="取得済み race_id も再取得する（既定はスキップ）")

    # backfill-yoso サブコマンド（既存 race_id の予想印のみを取得。コア取得と独立）
    by_p = sub.add_parser(
        "backfill-yoso",
        help="既存 raw の race_id に予想印(無料+premium)のみ取得。後で rebuild-featured",
    )
    by_p.add_argument("--min-year", type=int, default=None, help="この開催年以降のみ対象")
    by_p.add_argument("--limit", type=int, default=None, help="先頭 N レースのみ（動作確認用）")
    by_p.add_argument("--source", type=str, default=None, help="データソース名（既定: 選択保存 > netkeiba）")
    by_p.add_argument("--no-skip-existing", action="store_true", help="取得済み race_id も再取得する")

    # backfill-yoso-predictors サブコマンド（予想家スキル prior のみ取得）
    byp = sub.add_parser(
        "backfill-yoso-predictors",
        help="yoso_marks の predictor_yid に予想家スキル prior のみ取得。後で rebuild-featured",
    )
    byp.add_argument("--limit", type=int, default=None, help="先頭 N 人のみ（動作確認用）")
    byp.add_argument("--source", type=str, default=None, help="データソース名")
    byp.add_argument("--no-skip-existing", action="store_true", help="取得済み予想家も再取得する")

    # backfill-persons サブコマンド（人物の年度別成績のみ取得。コア取得と独立）
    bp2 = sub.add_parser(
        "backfill-persons",
        help="results の jockey/trainer_id に人物年度別成績のみ取得。後で rebuild-featured",
    )
    bp2.add_argument("--types", type=str, default=None, help="対象種別（カンマ区切り。既定 jockey,trainer）")
    bp2.add_argument("--limit", type=int, default=None, help="先頭 N 人のみ（動作確認用）")
    bp2.add_argument("--source", type=str, default=None, help="データソース名")
    bp2.add_argument("--no-skip-existing", action="store_true", help="取得済み entity も再取得する")

    # backfill-horses サブコマンド（既存 horse_id の馬ページを網羅取得。peds は別ジョブ）
    bh_p = sub.add_parser(
        "backfill-horses",
        help="results の全 horse_id に馬ページ(horse_results/horse_info)を取得。KEIBA_SKIP_PEDS=1 で血統を分離",
    )
    bh_p.add_argument("--limit", type=int, default=None, help="先頭 N 頭のみ（動作確認用）")
    bh_p.add_argument("--source", type=str, default=None, help="データソース名（既定: 選択保存 > netkeiba）")
    bh_p.add_argument("--no-skip-existing", action="store_true", help="取得済み horse_id も再取得する")

    # backfill-peds サブコマンド（既存 horse_id の血統のみを取得。馬ページ取得と独立）
    bp_p = sub.add_parser(
        "backfill-peds",
        help="既存 raw の horse_id に血統(peds)のみ取得。KEIBA_SKIP_PEDS=1 で馬ページ先行取得した後に使用",
    )
    bp_p.add_argument("--limit", type=int, default=None, help="先頭 N 頭のみ（動作確認用）")
    bp_p.add_argument("--source", type=str, default=None, help="データソース名（既定: 選択保存 > netkeiba）")
    bp_p.add_argument("--no-skip-existing", action="store_true", help="取得済み horse_id も再取得する")

    # evaluate-odds-dynamics サブコマンド
    eval_p = sub.add_parser("evaluate-odds-dynamics", help="オッズ力学モデルの比較評価（重力統計も更新）")
    eval_p.add_argument("--holdout-frac", type=float, default=0.2, help="検証に使う直近レースの割合")

    # fetch-final-odds サブコマンド（過去レースの最終確定オッズを全券種で取得）
    fo_p = sub.add_parser(
        "fetch-final-odds",
        help="過去レースの最終確定オッズを全券種（単複/枠連/馬連/馬単/ワイド/三連複/三連単）で取得・永続化",
    )
    fo_group = fo_p.add_mutually_exclusive_group(required=True)
    fo_group.add_argument("--race-id", dest="race_ids", nargs="+", type=int, help="対象 race_id（個別指定）")
    fo_group.add_argument(
        "--post-date", dest="post_date", metavar="YYYYMMDD", help="開催日を指定して当日の全レースを対象（1 日分）"
    )
    fo_group.add_argument(
        "--from-results", dest="from_results", action="store_true",
        help="取込済み results.pkl の全 race_id を対象（過去全レースのバックフィル）",
    )
    fo_p.add_argument(
        "--bet-types", dest="bet_types", nargs="+", default=None,
        help="対象券種（省略時は全 8 券種）。例: tansho umaren sanrentan",
    )
    fo_p.add_argument(
        "--years", dest="years", nargs="+", type=int, default=None,
        metavar="YYYY", help="race_id の年で絞り込む（例: 2010 2011 … 大量バックフィルの分割用）",
    )
    fo_p.add_argument(
        "--limit", dest="limit", type=int, default=None,
        help="1 回で取得するレース数の上限（resume で分割実行するため）",
    )
    fo_p.add_argument(
        "--force", dest="force", action="store_true",
        help="取得済みレースもスキップせず再取得する",
    )

    # calibrate-takeout サブコマンド（払戻実績から券種別実効控除率を逆算）
    ct_p = sub.add_parser(
        "calibrate-takeout",
        help="払戻実績×単勝勝率から券種別の実効控除率を逆算し models/takeout_calibration.json に保存",
    )
    ct_p.add_argument(
        "--min-samples", dest="min_samples", type=int, default=20,
        help="較正に必要な券種別の最小サンプル数（未満は JRA 公称控除率へフォールバック）",
    )
    ct_p.add_argument(
        "--dry-run", dest="dry_run", action="store_true",
        help="逆算結果をログ表示するのみで保存しない",
    )

    # calibrate-ev サブコマンド（OOS で γ,δ / r̂較正 / α,β を fit して保存）
    ce_p = sub.add_parser(
        "calibrate-ev",
        help="OOSデータで補正Harville(γ,δ)/r̂較正/市場合成(α,β)を fit し models/*.json に保存",
    )
    ce_p.add_argument("--version", default=None, help="基準にするモデルのバージョン名（省略時は最新）")
    ce_p.add_argument(
        "--years", type=int, nargs="+", default=None, metavar="YYYY",
        help="fit に使う年（学習年より後の OOS にすること。例: 2025）",
    )
    ce_p.add_argument(
        "--which", nargs="+", default=["exponents", "calibrator", "blend"],
        choices=["exponents", "calibrator", "blend"],
        help="fit するアーティファクト（既定: 全て）",
    )
    ce_p.add_argument(
        "--dry-run", dest="dry_run", action="store_true",
        help="fit 結果をログ表示するのみで保存しない",
    )

    # backtest サブコマンド
    bt_p = sub.add_parser(
        "backtest",
        help="ホールドアウト期間で2ヘッド予測→確定オッズEV選定→実払戻で券種別回収率を評価",
    )
    bt_p.add_argument("--version", default=None, help="評価するモデルのバージョン名（省略時は最新）")
    bt_p.add_argument(
        "--years", type=int, nargs="+", default=None, metavar="YYYY",
        help="評価対象を race_id の年で絞る（例: 2024 2025）。学習年と重ねないこと",
    )
    bt_p.add_argument(
        "--no-win-head", action="store_true",
        help="Win ヘッドを使わず Place 単独で評価（2ヘッド無効化）",
    )
    bt_p.add_argument(
        "--no-final-odds", action="store_true",
        help="確定オッズを使わず単勝からの Harville 推定オッズで評価",
    )
    bt_p.add_argument(
        "--bet-types", nargs="+", default=None,
        help="評価する券種（省略時は全券種）。例: fukusho wide sanrenpuku",
    )
    bt_p.add_argument("--json", action="store_true", help="結果を JSON で出力")
    bt_p.add_argument(
        "--edge-diagnostic", action="store_true",
        help="自分の勝率 r̂ vs 実現最終市場 p_mkt の較正・エコー・勝ち馬logloss を併せて出力",
    )
    bt_p.add_argument(
        "--no-odds-features", action="store_true",
        help="retrain --no-odds-features で学習したモデルを評価する際に指定（featured から同じ"
             "オッズ由来列を落として列を一致させる。学習=.values で位置一致が必要なため）",
    )
    bt_p.add_argument(
        "--corrected-harville", action="store_true",
        help="models/place_exponents.json の (γ,δ) を読み補正Harvilleで順序券種を評価",
    )
    bt_p.add_argument(
        "--calibrate", action="store_true",
        help="models/win_calibrator.json の r̂ 較正を適用して勝率を補正",
    )
    bt_p.add_argument(
        "--blend", action="store_true",
        help="models/blend_weights.json の (α,β) で市場合成した勝率を使う",
    )
    bt_p.add_argument(
        "--unratable-fallback", action="store_true",
        help="初出走馬(career_starts=0/NaN)を公衆 implied 勝率に置換し初出走のみのレースは除外（ベンター §3）",
    )

    doctor_p = sub.add_parser("doctor", help="データ/モデル/DB/ディスクの健全性を点検")
    doctor_p.add_argument("--json", action="store_true", help="結果を JSON で出力")
    doctor_p.add_argument("--strict", action="store_true", help="WARN でも非0終了する")
    doctor_p.add_argument(
        "--prune-models",
        type=int,
        default=None,
        metavar="KEEP",
        help="モデルを新しい順に KEEP 世代残して古い世代を削除する",
    )

    return parser.parse_args(argv)


def _resolve_backtest_model_path(version: str | None) -> str:
    """評価対象の Place モデル pickle パスを解決する。

    --version 指定時は ``models/*/<version>.pickle`` を厳密一致（__win は除外）。
    省略時は正規モデル（``*_keibam.pickle``）の最新を使う。
    """
    import glob

    if version:
        matches = sorted(
            m for m in glob.glob(os.path.join("models", "*", f"{version}.pickle"))
            if not m.endswith("__win.pickle")
        )
        if not matches:
            raise FileNotFoundError(f"バージョン '{version}' のモデルが見つかりません")
        return matches[-1]
    from app._data_loader import find_model_paths

    paths = find_model_paths("models")
    if not paths:
        raise FileNotFoundError("models/ に評価可能なモデルがありません")
    return paths[0]


def _backtest(args: argparse.Namespace) -> None:
    """ホールドアウト期間で2ヘッド予測→確定オッズEV選定→実払戻で券種別回収率を評価する。"""
    import json

    from app._data_loader import load_model_from_path, load_win_head_for
    from src.constants._local_paths import LocalPaths
    from src.pipeline._ingestion import load_raw
    from src.preparing._odds_snapshot import build_final_odds_lookup
    from src.preparing.odds_scheduler import load_snapshots
    from src.simulation._backtest import default_thresholds, format_report, run_backtest

    place_path = _resolve_backtest_model_path(getattr(args, "version", None))
    place_ai = load_model_from_path(place_path)
    win_ai = None if getattr(args, "no_win_head", False) else load_win_head_for(place_path)
    logger.info(
        "[backtest] モデル: %s（Win ヘッド=%s）",
        os.path.basename(place_path), "あり" if win_ai is not None else "なし",
    )

    # ホールドアウト featured を年でフィルタ
    featured = load_raw(LocalPaths.FEATURED_DATA_PATH)
    if featured is None or featured.empty:
        logger.error("[backtest] featured_data がありません。先に rebuild-featured を実行してください")
        return
    years = getattr(args, "years", None)
    if years:
        yset = {str(y) for y in years}
        rid = featured.index.astype(str)
        featured = featured[rid.str[:4].isin(yset)]
        logger.info("[backtest] 評価対象を年 %s に絞り込み: %d 行", sorted(yset), len(featured))
    if featured.empty:
        logger.error("[backtest] 対象レースがありません（年フィルタが厳しすぎる可能性）")
        return

    # --no-odds-features: retrain --no-odds-features モデルと列を一致させる。学習は
    # X_train.values（位置ベース・generic 名）なので、評価側 featured からも同じオッズ由来
    # 列を落とさないと列数/順序がズレて LightGBM が Fatal（features mismatch）になる。
    if getattr(args, "no_odds_features", False):
        from src.constants._feature_cols import ODDS_DERIVED_FEATURE_COLS

        present = [c for c in ODDS_DERIVED_FEATURE_COLS if c in featured.columns]
        featured = featured.drop(columns=present, errors="ignore")
        logger.info("[backtest] --no-odds-features: オッズ由来 %d 列を除外: %s", len(present), present)

    # 確定オッズ lookup（--no-final-odds なら単勝 Harville 推定にフォールバック）
    final_odds_lookup = None
    if not getattr(args, "no_final_odds", False):
        snaps = load_snapshots(LocalPaths.RAW_ODDS_SNAPSHOT_PATH)
        final_odds_lookup = build_final_odds_lookup(snaps) if snaps else None
        logger.info(
            "[backtest] 確定オッズ lookup: %d 件", len(final_odds_lookup or {})
        )

    # 評価券種の絞り込み
    thresholds = default_thresholds()
    if getattr(args, "bet_types", None):
        want = set(args.bet_types)
        thresholds = {k: v for k, v in thresholds.items() if k in want}
        if not thresholds:
            logger.error("[backtest] --bet-types が全券種に不一致: %s", args.bet_types)
            return

    # EV 較正アーティファクト（calibrate-ev で OOS fit したもの）を opt-in で読み込む
    place_exponents = win_calibrator = blend_weights = None
    if getattr(args, "corrected_harville", False):
        from src.simulation._calibrate import place_exponents_path
        from src.policies._harville import load_place_exponents

        place_exponents = load_place_exponents(place_exponents_path("models"))
        logger.info("[backtest] 補正Harville: %s", place_exponents or "ファイル無し→素のHarville")
    if getattr(args, "calibrate", False):
        from src.simulation._calibrate import win_calibrator_path
        from src.policies._calibration import load_calibrator

        win_calibrator = load_calibrator(win_calibrator_path("models"))
        logger.info("[backtest] r̂較正: %s", "あり" if win_calibrator else "ファイル無し→較正なし")
    if getattr(args, "blend", False):
        from src.simulation._calibrate import blend_weights_path
        from src.policies._blend import load_blend_weights

        blend_weights = load_blend_weights(blend_weights_path("models"))
        logger.info("[backtest] 市場合成: %s", blend_weights or "ファイル無し→合成なし")
    unratable_fallback = getattr(args, "unratable_fallback", False)
    if unratable_fallback:
        logger.info("[backtest] 初出走の公衆フォールバック: 有効（初出走のみのレースは除外）")

    return_processor, _ = _return_processor_db_first()
    result = run_backtest(
        place_ai.effective_model,
        featured,
        return_processor,
        win_model=win_ai.effective_model if win_ai is not None else None,
        final_odds_lookup=final_odds_lookup,
        thresholds=thresholds,
        place_exponents=place_exponents,
        win_calibrator=win_calibrator,
        blend_weights=blend_weights,
        unratable_fallback=unratable_fallback,
    )

    # Edge/EV 診断（任意）: 自分の勝率 r̂ vs 実現最終市場 p_mkt の較正・エコー・勝ち馬logloss。
    # 力学モデル不要（実現最終単勝を使う）。r̂ は Win ヘッド優先、無ければ Place。
    edge_result = None
    if getattr(args, "edge_diagnostic", False):
        from src.simulation._edge_diagnostic import format_edge_report, run_edge_diagnostic

        edge_model = (win_ai or place_ai).effective_model
        edge_result = run_edge_diagnostic(edge_model, featured)

    if getattr(args, "json", False):
        out = {
            "model": os.path.basename(place_path),
            "win_head": win_ai is not None,
            "n_races": result["n_races"],
            "n_candidates": result["n_candidates"],
            "overall": result["overall"].as_dict(),
            "per_bet_type": {str(k): v.as_dict() for k, v in result["per_bet_type"].items()},
        }
        if edge_result is not None:
            out["edge_diagnostic"] = edge_result["summary"]
            if edge_result.get("blend"):
                out["delta_r2"] = edge_result["blend"]
        print(json.dumps(out, ensure_ascii=False, indent=2))
    else:
        print(format_report(result))
        if edge_result is not None:
            print("\n" + format_edge_report(edge_result))


def _doctor(args: argparse.Namespace) -> None:
    """健全性点検を実行し、ERROR（または --strict 時 WARN）で非0終了する。"""
    import json
    import sys

    from src.pipeline._doctor import ERROR, WARN, run_doctor

    if getattr(args, "prune_models", None) is not None:
        from src.pipeline._model_retention import prune_models

        deleted = prune_models("models", args.prune_models, dry_run=False)
        logger.info("[doctor] prune-models keep=%d 削除 %d 世代: %s",
                    args.prune_models, len(deleted), deleted)

    results, level = run_doctor()
    if args.json:
        print(json.dumps(
            {"level": level, "checks": [r.__dict__ for r in results]},
            ensure_ascii=False, indent=2,
        ))
    else:
        for r in results:
            icon = {"OK": "✅", "WARN": "⚠️", "ERROR": "❌"}.get(r.level, "•")
            print(f"{icon} [{r.level}] {r.name}: {r.detail}")
        print(f"\n総合: {level}")

    if level == ERROR or (args.strict and level == WARN):
        sys.exit(1)


def _finish_log(job: str, status: str, started_at: str, start_perf: float, message: str) -> None:
    """ジョブ実行を execution_log に記録する（非致命）。"""
    import datetime as dt
    import time

    try:
        from src.storage import record_execution

        record_execution(
            job, status,
            started_at=started_at,
            finished_at=dt.datetime.now().isoformat(timespec="seconds"),
            duration_sec=time.perf_counter() - start_perf,
            message=message,
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("[run] execution_log 記録失敗 (non-fatal): %s", e)


def _notify_failure(job: str, message: str) -> None:
    """ジョブ失敗を通知する（NOTIFY_SLACK_WEBHOOK があれば Slack、無ければ no-op）。"""
    try:
        from src.operation._notifier import create_notifier

        create_notifier().notify(f"keibam {job} 失敗", message, level="error")
    except Exception as e:  # noqa: BLE001
        logger.warning("[run] 失敗通知に失敗 (non-fatal): %s", e)


def _run_job(job: str, handler, args: argparse.Namespace) -> None:
    """ハンドラを計測・記録付きで実行する（成否を execution_log に記録、失敗時は通知）。"""
    import datetime as dt
    import time

    started_at = dt.datetime.now().isoformat(timespec="seconds")
    start_perf = time.perf_counter()
    try:
        handler(args)
    except SystemExit as e:  # doctor --strict 等の意図的終了
        ok = e.code in (0, None)
        _finish_log(job, "ok" if ok else "failed", started_at, start_perf, f"exit={e.code}")
        raise
    except Exception as e:
        message = f"{type(e).__name__}: {e}"
        _finish_log(job, "failed", started_at, start_perf, message)
        _notify_failure(job, message)
        raise
    else:
        _finish_log(job, "ok", started_at, start_perf, "")


def main(argv: Sequence[str] | None = None) -> None:
    from src.constants._logging_config import setup_logging

    setup_logging()
    args = _parse_args(argv)
    handlers = {
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
        "doctor": _doctor,
    }
    handler = handlers.get(args.job)
    if handler is not None:
        _run_job(args.job, handler, args)


if __name__ == "__main__":
    main()
