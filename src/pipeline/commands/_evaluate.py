"""evaluate-odds-dynamics / fetch-final-odds / backtest / doctor コマンド群。"""

from __future__ import annotations

import argparse
import logging
import os

from src.pipeline._cli_common import _return_processor_db_first
from src.pipeline.commands._ingest import _resolve_race_ids

logger = logging.getLogger(__name__)


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

    # --no-rating-features: retrain --no-rating-features モデルと列を一致させる（Elo 由来列を除外）。
    if getattr(args, "no_rating_features", False):
        from src.constants._feature_cols import ELO_FEATURE_COLS

        rating_cols = ELO_FEATURE_COLS + [f"{c}_z" for c in ELO_FEATURE_COLS]
        present = [c for c in rating_cols if c in featured.columns]
        featured = featured.drop(columns=present, errors="ignore")
        logger.info("[backtest] --no-rating-features: Elo 由来 %d 列を除外: %s", len(present), present)

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
            "reliable_overall": result["reliable_overall"].as_dict()
            if result.get("reliable_overall") is not None
            else None,
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
