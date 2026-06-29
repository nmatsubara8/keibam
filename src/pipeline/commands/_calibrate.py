"""calibrate-takeout / calibrate-ev コマンド群（控除率・EV 較正の fit）。"""

from __future__ import annotations

import argparse
import logging
import os

from src.pipeline._cli_common import (
    _auto_migrate_db,
    _load_raw_db_first,
    _return_processor_db_first,
)
from src.pipeline.commands._evaluate import _resolve_backtest_model_path

logger = logging.getLogger(__name__)


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
    # --no-odds-features / --no-rating-features: retrain で同フラグを使ったモデルと列を一致させる
    # （backtest と同じ列除外。落とさないと特徴量数が不一致で predict_proba が Fatal になる）。
    if getattr(args, "no_odds_features", False):
        from src.constants._feature_cols import ODDS_DERIVED_FEATURE_COLS

        present = [c for c in ODDS_DERIVED_FEATURE_COLS if c in featured.columns]
        featured = featured.drop(columns=present, errors="ignore")
        logger.info("[calibrate-ev] --no-odds-features: オッズ由来 %d 列を除外: %s", len(present), present)
    if getattr(args, "no_rating_features", False):
        from src.constants._feature_cols import ELO_FEATURE_COLS

        present = [c for c in ELO_FEATURE_COLS if c in featured.columns]
        featured = featured.drop(columns=present, errors="ignore")
        logger.info("[calibrate-ev] --no-rating-features: Elo 由来 %d 列を除外: %s", len(present), present)

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
