"""ingest / rebuild-featured コマンド群（取得・featured 再生成）。"""

from __future__ import annotations

import argparse
import logging
import os
from typing import TYPE_CHECKING

from src.pipeline._cli_common import _auto_migrate_db

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
