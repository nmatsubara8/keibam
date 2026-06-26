"""backfill-* コマンド群（既存 raw に補助データを後追い取得）。"""

from __future__ import annotations

import argparse
import logging

from src.pipeline.commands._ingest import _resolve_data_source

logger = logging.getLogger(__name__)


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
