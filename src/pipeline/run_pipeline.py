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


def _save_ratings_snapshot(merger) -> None:
    """DataMerger が保持するレーティングスナップショットを models/ に保存する。

    Phase 1 Elo → horse_ratings.json、Phase 2 TrueSkill → horse_trueskill.json。
    ライブ予測（出馬表）が各出走馬の現行レーティングを参照できるようにする。non-fatal。
    """
    import json

    from src.constants._local_paths import LocalPaths

    for attr, path, label in (
        ("horse_ratings_snapshot", LocalPaths.HORSE_RATINGS_PATH, "ratings"),
        ("horse_trueskill_snapshot", LocalPaths.HORSE_TRUESKILL_PATH, "trueskill"),
    ):
        snapshot = getattr(merger, attr, None)
        if not snapshot:
            continue
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as f:
                json.dump(snapshot, f, ensure_ascii=False)
            logger.info("[%s] スナップショットを保存: %s (%d 頭)", label, path, len(snapshot))
        except Exception as e:  # noqa: BLE001
            logger.warning("[%s] スナップショット保存失敗 (non-fatal): %s", label, e)


def _scrape_new_race_data(race_ids: list) -> list:
    """新規 race_id のレース HTML を取得し、raw テーブルを増分更新する（Playwright 必須）。

    既存 results.pkl に存在する race_id はスキップする。HTML（bin）→ テーブル化は
    data/html/race/ 配下の bin 全件が対象（bin はキャッシュ。新規分のみ追加取得され、
    transfer_temp_file が既存 pkl へ新データ優先でマージする）。

    Returns
    -------
    list : 今回新たにスクレイプした race_id（pickle へマージ済み）。
    """
    import pandas as pd

    from src.constants._local_paths import LocalPaths
    from src.pipeline._ingestion import existing_race_ids
    from src.pipeline._ingestion import find_new_race_ids
    from src.pipeline._ingestion import load_raw
    from src.preparing._get_rawdata import get_rawdata_info
    from src.preparing._get_rawdata import get_rawdata_results
    from src.preparing._get_rawdata import get_rawdata_return
    from src.preparing._scrape_html_race import scrape_html_race

    existing = load_raw(LocalPaths.RAW_RESULTS_PATH)
    if not existing.empty and "race_id" in existing.columns and existing.index.name != "race_id":
        existing = existing.set_index("race_id")
    new_ids = find_new_race_ids(existing_race_ids(existing), [int(r) for r in race_ids])
    if not new_ids:
        logger.info("[ingest] 新規 race_id なし（HTML スクレイプ不要）")
        return []

    logger.info("[ingest] 新規レース %d 件の HTML を取得します", len(new_ids))
    race_id_df = pd.DataFrame({"race_id": [str(r) for r in new_ids]})
    scrape_html_race(race_id_df, skip=True)
    # skip=False で bin からテーブルを再構築（既存 pkl へは新データ優先でマージされる）
    get_rawdata_results(skip=False)
    get_rawdata_info(skip=False)
    get_rawdata_return(skip=False)
    return new_ids


def _scrape_new_horse_data() -> int:
    """results に存在するが horse_info に無い馬の HTML を取得し、馬系テーブルを増分更新する。

    新規レースの取込で初出走馬・地方/海外からの転入馬が現れると、horse_results /
    horse_info / peds に欠落が生じ、特徴量の馬履歴・血統が NaN になる。
    馬ページ・血統ページは差分ダウンロード（既存 bin はスキップ）。

    Returns
    -------
    int : 新たにスクレイプした馬の数。
    """
    from src.constants._local_paths import LocalPaths
    from src.pipeline._ingestion import load_raw
    from src.preparing._get_rawdata import get_rawdata_horse_info
    from src.preparing._get_rawdata import get_rawdata_horse_results
    from src.preparing._get_rawdata import get_rawdata_peds
    from src.preparing._scrape_html_horse import scrape_html_horse_with_master
    from src.preparing._scrape_html_ped import scrape_html_ped

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
        logger.info("[ingest] 馬データの欠落なし（馬ページのスクレイプ不要）")
        return 0

    logger.info("[ingest] 未取得の馬 %d 頭の HTML（馬ページ・血統）を取得します", len(missing))
    scrape_html_horse_with_master(missing, skip=True)
    scrape_html_ped(missing, skip=True)
    # skip=False で bin からテーブルを再構築（既存 pkl へは horse_id キーでマージされる）
    get_rawdata_horse_results(skip=False)
    get_rawdata_horse_info(skip=False)
    get_rawdata_peds(skip=False)

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


def _ingest(args: argparse.Namespace) -> None:
    """取込ジョブを実行する（selenium / bs4 が実行時に必要）。"""
    from src.pipeline._ingestion import IngestConfig
    from src.pipeline._ingestion import IngestJob

    # Phase 1: pickle → DB の自動移行（DB が空の場合のみ実行される）
    _auto_migrate_db()

    # --post-date が指定された場合は race_id を自動取得する
    if getattr(args, "post_date", None):
        args.race_ids = _resolve_race_ids(args.post_date)

    # 新規レースの HTML 取得 → raw テーブル増分更新。
    # スクレイプ失敗時は既存 pickle のみで継続する（冪等・リジューム前提）。
    scraped_new: list = []
    try:
        scraped_new = _scrape_new_race_data(list(args.race_ids))
    except Exception as e:  # noqa: BLE001
        logger.warning("[ingest] 新規レースのスクレイプに失敗 (既存データのみで継続): %s", e)

    # 新規レースに含まれる未知の馬（初出走・転入）の馬ページ・血統を取得。
    # 失敗しても既存の馬データで featured 再生成は可能なため non-fatal。
    scraped_horses = 0
    try:
        scraped_horses = _scrape_new_horse_data()
    except Exception as e:  # noqa: BLE001
        logger.warning("[ingest] 馬データのスクレイプに失敗 (既存データのみで継続): %s", e)

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
            _save_ratings_snapshot(merger)
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
                .encode_horse_id()
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

    # Phase 1: pickle → DB の自動移行（DB が空の場合のみ実行される）
    _auto_migrate_db()

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
                _save_ratings_snapshot(merger)
                return (
                    FeatureEngineering(merger)
                    .add_interval().add_agedays()
                    .add_interaction_features()
                    .add_race_level_zscore()
                    .dumminize_kaisai().dumminize_sex().dumminize_weather()
                    .dumminize_race_type().dumminize_ground_state1().dumminize_ground_state2()
                    .dumminize_around().dumminize_race_class()
                    .encode_horse_id()
                    .encode_jockey_id().encode_trainer_id().encode_owner_id().encode_breeder_id()
                ).featured_data

        featured_data = _Builder().build(cfg_ing)
        featured_data.to_pickle(featured_path)
        logger.info("[retrain] featured_data.pkl を生成しました shape=%s", featured_data.shape)
    else:
        featured_data = pd.read_pickle(featured_path)

    # レーティング特徴量の On/Off アブレーション（A/B 比較用に variant を生成）。
    # --no-rating-features: 全ファミリー（Elo + TrueSkill）/ --no-elo-features: Elo のみ /
    # --no-trueskill-features: TrueSkill のみ を学習前に落とす（各 _z 版も含む）。
    vname = args.version_name
    from src.constants._feature_cols import ELO_FEATURE_COLS
    from src.constants._feature_cols import RATING_FEATURE_COLS
    from src.constants._feature_cols import TS_FEATURE_COLS

    drop_base: set = set()
    suffixes: list = []
    if getattr(args, "no_rating_features", False):
        drop_base |= set(RATING_FEATURE_COLS)
        suffixes.append("norating")
    else:
        if getattr(args, "no_elo_features", False):
            drop_base |= set(ELO_FEATURE_COLS)
            suffixes.append("noelo")
        if getattr(args, "no_trueskill_features", False):
            drop_base |= set(TS_FEATURE_COLS)
            suffixes.append("nots")

    if drop_base:
        from src.pipeline._retrain import version_name

        drop_cols = [
            c for c in featured_data.columns
            if c in drop_base or (c.endswith("_z") and c[:-2] in drop_base)
        ]
        featured_data = featured_data.drop(columns=drop_cols, errors="ignore")
        logger.info("[retrain] アブレーション: 特徴量 %d 列を除外 %s", len(drop_cols), drop_cols)
        if vname is None:
            vname = version_name(prefix="keibam_" + "_".join(suffixes))

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

    job = RetrainJob(KeibaAIFactory, cfg)
    result = job.run(
        featured_data,
        vname=vname,
        with_tuning=args.with_tuning,
        lgb_params=lgb_params,
        params_rank=params_rank,
    )
    logger.info("[retrain] %s", result)


def _evaluate_odds_dynamics(args: argparse.Namespace) -> None:
    """オッズ力学モデル（Dirichlet/Kalman/Particle/Ensemble）の比較評価ジョブ。

    蓄積スナップショットを時系列 holdout で分割し、各モデルの精度を比較して
    models/odds_dynamics_eval.json と models/odds_gravity.json を更新する。
    結果はモデルラボの「オッズ力学モデル」タブに表示される。
    """
    from src.constants._bet_types import BetType
    from src.constants._local_paths import LocalPaths
    from src.preparing.odds_scheduler import load_snapshots
    from src.training._odds_dynamics_eval import dynamics_eval_path
    from src.training._odds_dynamics_eval import evaluate_dynamics_models
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

    evaluation = evaluate_dynamics_models(sequences, holdout_frac=args.holdout_frac)
    save_dynamics_eval(evaluation, dynamics_eval_path("models"))
    save_gravity(evaluation["gravity"], gravity_path("models"))
    for name, metrics in evaluation["results"].items():
        logger.info("[odds-dynamics] %s: KL=%.4f mae=%.4f mape=%.3f",
                    name, metrics["kl_mean"], metrics["share_mae"], metrics["odds_mape"])


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

    # retrain サブコマンド
    retrain_p = sub.add_parser("retrain", help="全データで週次再学習")
    retrain_p.add_argument("--version-name", default=None, help="バージョン名（省略時は日付自動生成）")
    retrain_p.add_argument("--no-stacking", action="store_true", help="スタッキングを使わない（LightGBM のみ）")
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
    # レーティング特徴量の On/Off アブレーション（A/B 用に variant を学習）
    retrain_p.add_argument(
        "--no-rating-features",
        action="store_true",
        help="全レーティング特徴量（Elo + TrueSkill）を除外して学習する（A/B 比較用）",
    )
    retrain_p.add_argument(
        "--no-elo-features",
        action="store_true",
        help="Elo レーティング特徴量のみ除外して学習する",
    )
    retrain_p.add_argument(
        "--no-trueskill-features",
        action="store_true",
        help="TrueSkill 特徴量のみ除外して学習する",
    )

    # evaluate-odds-dynamics サブコマンド
    eval_p = sub.add_parser("evaluate-odds-dynamics", help="オッズ力学モデルの比較評価（重力統計も更新）")
    eval_p.add_argument("--holdout-frac", type=float, default=0.2, help="検証に使う直近レースの割合")

    # doctor サブコマンド（健全性点検）
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
        "evaluate-odds-dynamics": _evaluate_odds_dynamics,
        "retrain": _retrain,
        "doctor": _doctor,
    }
    handler = handlers.get(args.job)
    if handler is not None:
        _run_job(args.job, handler, args)


if __name__ == "__main__":
    main()
