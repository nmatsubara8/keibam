"""retrain コマンド（全データ再学習）と設定ビルダ。"""

from __future__ import annotations

import argparse
import logging
import os

from src.pipeline._cli_common import _auto_migrate_db

logger = logging.getLogger(__name__)


def _parse_duration(s) -> float:
    """時間予算文字列を秒に変換する。'3h'/'180m'/'10800s'/'10800'（純数値=秒）を受理。"""
    t = str(s).strip().lower()
    if t.endswith("h"):
        return float(t[:-1]) * 3600.0
    if t.endswith("m"):
        return float(t[:-1]) * 60.0
    if t.endswith("s"):
        return float(t[:-1])
    return float(t)


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

    # --gpu: GBDT を GPU で学習（KEIBA_USE_GPU=1 を立て、tuner/factory が xgboost/catboost に反映）。
    # CUDA 不可なら CPU にフォールバック。NN は torch 側で自動検出するため本フラグと独立。
    if getattr(args, "gpu", False):
        try:
            import torch
            if torch.cuda.is_available():
                os.environ["KEIBA_USE_GPU"] = "1"
                logger.info("[retrain] --gpu: GBDT を GPU で学習（xgboost device=cuda / catboost task_type=GPU）")
            else:
                logger.warning("[retrain] --gpu 指定だが CUDA 利用不可 → GBDT は CPU で続行")
        except Exception:  # noqa: BLE001 — torch 不在等でも CPU で継続
            logger.warning("[retrain] --gpu 指定だが torch/CUDA 確認に失敗 → GBDT は CPU で続行")

    # --lgb-gpu: LightGBM を GPU で学習（要 GPU 有効ビルド）。build/tuner が device_type を反映。
    lgb_gpu = getattr(args, "lgb_gpu", None)
    if lgb_gpu:
        os.environ["KEIBA_LGB_GPU"] = lgb_gpu
        logger.info("[retrain] --lgb-gpu %s: LightGBM を GPU で学習（要 GPU 有効ビルド）", lgb_gpu)

    cfg = RetrainConfig(
        use_stacking=not args.no_stacking,
        train_win_head=not getattr(args, "no_win_head", False),
        train_categories=not getattr(args, "no_category_split", False),
        min_category_races=getattr(args, "min_category_races", 300),
        tune_categories=getattr(args, "tune_categories", False),
    )

    # --resume-tuning: Optuna study を models_dir 配下の SQLite に永続化し、再実行で探索を
    # 再開する（trial 追記＝best 単調改善）。TPE 系（xgboost/catboost/nn・手書き LightGBM）が対象。
    if getattr(args, "resume_tuning", False):
        db_path = os.path.join(cfg.models_dir, "optuna_studies.db")
        os.makedirs(cfg.models_dir, exist_ok=True)
        os.environ["KEIBA_TUNING_STORAGE"] = f"sqlite:///{db_path}"
        logger.info("[retrain] --resume-tuning: Optuna 探索を永続化・再開します（%s）", db_path)

    # --featured-path 指定時はその featured で学習（seed など別コーパスの検証用）。既定は本番 featured。
    featured_path = getattr(args, "featured_path", None) or LocalPaths.FEATURED_DATA_PATH
    if getattr(args, "featured_path", None):
        logger.info("[retrain] featured_path 上書き: %s", featured_path)
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

    # --holdout-years: 指定年を学習データから完全に除外する。backtest --years <同年> で
    # out-of-sample（リーク無し）の券種別 ROI を測るための分離。除外しない年で学習する。
    holdout_years = getattr(args, "holdout_years", None)
    if holdout_years:
        yset = {str(y) for y in holdout_years}
        before = len(featured_data)
        rid = featured_data.index.astype(str)
        featured_data = featured_data[~rid.str[:4].isin(yset)]
        logger.info(
            "[retrain] --holdout-years %s を学習から除外: %d→%d 行（backtest 用に分離）",
            sorted(yset), before, len(featured_data),
        )
        if featured_data.empty:
            logger.error("[retrain] 除外後の学習データが空です（holdout-years が広すぎ）")
            return

    # --since-year: 指定年以降の行だけで学習（メモリ/時間節約・A/B 検証用）。全40年 177万行が
    # メモリに載らない環境で、直近数年に絞って Elo 有無等の A/B を回すための行数上限。
    since_year = getattr(args, "since_year", None)
    if since_year:
        before = len(featured_data)
        yr = pd.to_numeric(featured_data.index.astype(str).str[:4], errors="coerce")
        featured_data = featured_data[yr >= since_year]
        logger.info("[retrain] --since-year %d: 直近のみで学習 %d→%d 行", since_year, before, len(featured_data))
        if featured_data.empty:
            logger.error("[retrain] --since-year 後の学習データが空です（年が新しすぎ/race_id 形式不一致）")
            return

    # --no-odds-features: オッズ由来の派生特徴（単勝_log・市場歪み overlay 等）を学習から
    # 除外する。マーケット・エコー検証（r̂ が市場の写しでないかの A/B）用。Place/Win 両ヘッドに
    # 適用される（この時点では DataFrame なので prepared_from_gbdt 変換の前に落とす）。
    if getattr(args, "no_odds_features", False):
        from src.constants._feature_cols import ODDS_DERIVED_FEATURE_COLS

        present = [c for c in ODDS_DERIVED_FEATURE_COLS if c in featured_data.columns]
        featured_data = featured_data.drop(columns=present, errors="ignore")
        logger.info("[retrain] --no-odds-features: オッズ由来 %d 列を除外: %s", len(present), present)

    # --no-rating-features: Elo レーティング由来の特徴（elo_* と その _z）を学習から除外する。
    # レーティング効果の A/B（--no-rating-features 有無で別モデルを学習し backtest 比較）用。
    if getattr(args, "no_rating_features", False):
        from src.constants._feature_cols import ELO_FEATURE_COLS

        rating_cols = ELO_FEATURE_COLS + [f"{c}_z" for c in ELO_FEATURE_COLS]
        present = [c for c in rating_cols if c in featured_data.columns]
        featured_data = featured_data.drop(columns=present, errors="ignore")
        logger.info("[retrain] --no-rating-features: Elo 由来 %d 列を除外: %s", len(present), present)

    # --float32-features: 数値列（float64）を float32 に落として学習時のピーク RAM を約半減する。
    # 全データ（177万行×約200列）の最終スタック学習が OOM で落ちる環境で、--since-year で行を
    # 削らずにデータ量を保ったまま収めるための策。GBDT の分割探索は float32 精度で実質無影響。
    if getattr(args, "float32_features", False):
        f64 = featured_data.select_dtypes(include=["float64"]).columns
        if len(f64):
            before_mb = featured_data.memory_usage(deep=True).sum() / 1024**2
            featured_data[f64] = featured_data[f64].astype("float32")
            after_mb = featured_data.memory_usage(deep=True).sum() / 1024**2
            logger.info(
                "[retrain] --float32-features: float64 %d 列を float32 化 %.0f→%.0f MB（%.0f MB 削減）",
                len(f64), before_mb, after_mb, before_mb - after_mb,
            )
        else:
            logger.info("[retrain] --float32-features: 対象の float64 列なし（変更なし）")

    # --nn-standalone: NN を GBDT スタックと分離して単体学習し保存する（分離NN + 遅延スタッキング）。
    # GBDT スタックは別途 configs/base_models_gbdt.json で全データ学習し、build-combined で meta 融合する。
    if getattr(args, "nn_standalone", False):
        import json as _json

        from src.pipeline._nn_standalone import save_nn_standalone
        from src.pipeline._nn_standalone import train_nn_standalone
        from src.pipeline._retrain import version_name
        from src.preprocessing._prepared_features import prepared_from_gbdt
        from src.training._keiba_ai_factory import KeibaAIFactory

        nn_params = None
        nn_search_space = None
        nn_entity_exclude: list = []
        tune_cfg: dict = {}
        nn_config = getattr(args, "nn_config", None)
        if nn_config:
            with open(nn_config) as f:
                _c = _json.load(f)
            nn_params = _c.get("nn_params")
            nn_search_space = _c.get("nn_search_space")
            # NN 埋め込みから外すエンティティ（汎化しない高カーディナリティ ID。既定 horse_id）。
            nn_entity_exclude = list(_c.get("nn_entity_exclude", []))
            tune_cfg = {
                "n_trials": _c.get("nn_tune_trials", 25),
                "epochs": _c.get("nn_tune_epochs", 15),
                "max_train_rows": _c.get("nn_tune_max_rows", 120000),
                "timeout": _c.get("timeout"),
            }
        # --nn-trials は config の nn_tune_trials を上書きする（1トライアルの動作確認用）。
        if getattr(args, "nn_trials", None) is not None:
            tune_cfg["n_trials"] = args.nn_trials
            logger.info("[retrain] --nn-trials: Optuna trial 数を %d に上書き", args.nn_trials)
        # --nn-timeout は探索を時間で打ち切る（例 3h）。指定時は trial 数上限を事実上無制限にして
        # 「時間が来るまで回す」にする（--nn-trials を明示した場合はそちらの上限も併用＝先着で停止）。
        if getattr(args, "nn_timeout", None) is not None:
            secs = _parse_duration(args.nn_timeout)
            tune_cfg["timeout"] = secs
            if getattr(args, "nn_trials", None) is None:
                tune_cfg["n_trials"] = 10_000_000
            logger.info(
                "[retrain] --nn-timeout: 探索を %.0f 秒（=%.2f 時間）で打ち切り", secs, secs / 3600.0
            )
        if nn_entity_exclude:
            logger.info(
                "[retrain] NN 埋め込みから除外: %s（汎化しない高カーディナリティ ID）", nn_entity_exclude
            )
        prepared = prepared_from_gbdt(featured_data, exclude_entities=nn_entity_exclude)
        ai = KeibaAIFactory().create(prepared, test_size=cfg.test_size, valid_size=cfg.valid_size)

        # --with-tuning かつ nn_search_space があれば、まず構造を Optuna 探索して best を反映する
        # （スタックルートと同じ作法。X_train を 80/20 分割して探索し X_test は未使用＝リーク回避）。
        # --resume-tuning 時は tune_nn 内 study_kwargs("nn") が永続 study を再開する。
        did_search = bool(getattr(args, "with_tuning", False) and nn_search_space)
        best_optuna = None
        if did_search:
            from src.pipeline._nn_standalone import load_nn_leaderboard
            from src.pipeline._nn_standalone import nn_leaderboard_path
            from src.pipeline._nn_standalone import search_nn_standalone

            # 過去の上位モデル（構造+パラメータ）を探索の初期値として投入する。
            lb_path = nn_leaderboard_path(cfg.models_dir)
            warm = [e["optuna_params"] for e in load_nn_leaderboard(lb_path) if e.get("optuna_params")]
            if warm:
                logger.info("[retrain] NN 探索の初期値として leaderboard 上位 %d 件を投入", len(warm))
            result = search_nn_standalone(
                ai.datasets, nn_search_space, warm_start_params=warm, **tune_cfg
            )
            best_optuna = result["optuna_params"]
            logger.info("[retrain] NN 構造探索 完了 val_auc=%.4f best=%s", result["val_auc"], result["nn_params"])
            nn_params = {**(nn_params or {}), **result["nn_params"]}

        model, metrics = train_nn_standalone(ai.datasets, nn_params)
        vname = args.version_name or version_name()
        path = save_nn_standalone(model, ai.datasets.nn_scaler, vname, models_dir=cfg.models_dir)
        logger.info("[retrain] NN 単体学習 完了: %s auc_test=%.4f → %s", vname, metrics["auc_test"], path)

        # 探索した場合、構造+パラメータを台帳へ保存（再現可能）。単純上書きせず上位 top_k を維持し、
        # 次回探索の初期値（ウォームスタート）として参照できるようにする。
        if did_search:
            import datetime as _dt

            from src.pipeline._nn_standalone import nn_leaderboard_path
            from src.pipeline._nn_standalone import update_nn_leaderboard

            entry = {
                "version": vname,
                "auc_test": float(metrics["auc_test"]),
                "nn_params": nn_params,          # 派生形（再現可能な学習仕様）
                "optuna_params": best_optuna,    # 生 suggest（ウォームスタート用）
                "trained_at": _dt.datetime.now().isoformat(),
            }
            lb_path = nn_leaderboard_path(cfg.models_dir)
            board = update_nn_leaderboard(lb_path, entry, top_k=5)
            logger.info(
                "[retrain] NN leaderboard 更新: %d 件保持（best auc_test=%.4f）→ %s",
                len(board), board[0]["auc_test"], lb_path,
            )
        return

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

    # --tune-models: 探索対象モデルを絞る（指定モデルだけ探索・他は固定）。自動で --with-tuning 有効化。
    tune_models = getattr(args, "tune_models", None)
    if tune_models:
        import dataclasses

        from src.training._base_models_config import BaseModelsConfig

        valid = {"lightgbm", "xgboost", "catboost", "nn"}
        selected = tuple(m.strip() for m in tune_models.split(",") if m.strip())
        bad = [m for m in selected if m not in valid]
        if bad:
            raise SystemExit(f"--tune-models に不正なモデル名: {bad}（有効: {sorted(valid)}）")
        if base_models_config is None:
            base_models_config = BaseModelsConfig()
        base_models_config = dataclasses.replace(base_models_config, tune_only=selected)
        if not args.with_tuning:
            args.with_tuning = True
            logger.info("[retrain] --tune-models 指定のため --with-tuning を有効化します")
        logger.info("[retrain] --tune-models: %s のみ探索（他は stored/既定値で固定）", list(selected))

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
