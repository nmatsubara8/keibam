"""時系列オッズの自動取得・自動再計算ウォッチャー（CLI エントリ）。

タイマー（cron */2 分 or `--loop` 常駐）で起動し:

1. オッズソース（netkeiba / JRA-VAN）から本日の (race_id, 発走時刻) を取得
2. チェックポイント（発走 30/10/5/1 分前 ± 許容幅）に入ったレースのオッズを取得
3. OddsSnapshot として冪等永続化（pickle + SQLite、既存 odds_scheduler.persist）
4. 当該レースの全時点系列からオッズ力学モデル（Dirichlet / Kalman / Particle /
   Ensemble）で「次チェックポイントのシェア」「発走時の確定シェア・オッズ」を再計算
5. 予測を `data/raw/odds_predictions.pkl` + `raw_odds_predictions` テーブルへ保存

レイヤ: pipeline（preparing のソース・スケジューラと training のモデルを両方呼べる）。

使用例:
    python -m src.pipeline.odds_watch --once                # 1 サイクル（cron 用）
    python -m src.pipeline.odds_watch --loop --interval 120 # 常駐
    python -m src.pipeline.odds_watch --once --source jravan
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import time
from typing import Sequence

import pandas as pd

from src.constants._local_paths import LocalPaths

logger = logging.getLogger(__name__)

PREDICTION_COLUMNS = [
    "race_id", "checkpoint", "model", "umaban", "predicted_at",
    "actual_share", "actual_odds", "pred_next_share",
    "pred_final_share", "pred_final_odds",
]


# ---------------------------------------------------------------------------
# 観測の構築・予測の計算（純粋寄りロジック）
# ---------------------------------------------------------------------------


def observations_for_race(snapshots: list, race_id: str) -> dict:
    """蓄積スナップショットから 1 レース分の {phase: シェア Series} を構築する。"""
    from src.constants._bet_types import BetType
    from src.training._odds_feature_builder import snapshots_to_phase_table
    from src.training._simplex import race_share_sequences

    race_snaps = [s for s in snapshots if str(s.race_id) == str(race_id) and s.bet_type == BetType.TANSHO]
    if not race_snaps:
        return {}
    table = snapshots_to_phase_table(race_snaps, BetType.TANSHO)
    sequences = race_share_sequences(table)
    return sequences.get(str(race_id), {})


def recalculate_predictions(
    snapshots: list,
    race_ids: Sequence[str],
    now: dt.datetime,
    models_dir: str = "models",
) -> pd.DataFrame:
    """指定レースの予測（次時点 + 発走時）を全モデルで再計算する。

    モデルは蓄積済み全系列で fit し、保存済みの重力統計・アンサンブル重みを使う。
    """
    from src.constants._bet_types import BetType
    from src.training._odds_dynamics import EnsembleShareModel
    from src.training._odds_dynamics import HORIZON_FINAL
    from src.training._odds_dynamics import HORIZON_NEXT
    from src.training._odds_dynamics import default_models
    from src.training._odds_dynamics_eval import dynamics_eval_path
    from src.training._odds_dynamics_eval import latest_ensemble_weights
    from src.training._odds_feature_builder import snapshots_to_phase_table
    from src.training._odds_gravity import gravity_path
    from src.training._odds_gravity import load_gravity
    from src.training._share_predictor_adapter import shares_to_odds
    from src.training._simplex import race_share_sequences

    table = snapshots_to_phase_table(snapshots, BetType.TANSHO)
    sequences = race_share_sequences(table)
    gravity = load_gravity(gravity_path(models_dir))

    models = default_models()
    weights = latest_ensemble_weights(dynamics_eval_path(models_dir)) or None
    members = dict(models)
    models["ensemble"] = EnsembleShareModel(members, weights=weights)
    for model in models.values():
        model.fit(sequences, gravity)

    rows = []
    for race_id in race_ids:
        obs = sequences.get(str(race_id), {})
        if not obs:
            continue
        observed_phases = [p for p in obs]
        checkpoint = observed_phases[-1]  # race_share_sequences は時系列順
        actual = obs[checkpoint]
        actual_odds = shares_to_odds(actual)
        for name, model in models.items():
            try:
                next_shares = model.predict_shares(obs, HORIZON_NEXT)
                final_shares = model.predict_shares(obs, HORIZON_FINAL)
            except Exception as e:  # noqa: BLE001 — 1 モデルの失敗で全体を止めない
                logger.warning("[odds_watch] %s の予測失敗 race=%s: %s", name, race_id, e)
                continue
            if final_shares.empty:
                continue
            final_odds = shares_to_odds(final_shares)
            for umaban in actual.index:
                rows.append(
                    {
                        "race_id": str(race_id),
                        "checkpoint": checkpoint,
                        "model": name,
                        "umaban": str(umaban),
                        "predicted_at": now.isoformat(),
                        "actual_share": float(actual.get(umaban, float("nan"))),
                        "actual_odds": float(actual_odds.get(umaban, float("nan"))),
                        "pred_next_share": (
                            float(next_shares.get(umaban, float("nan")))
                            if not next_shares.empty else float("nan")
                        ),
                        "pred_final_share": float(final_shares.get(umaban, float("nan"))),
                        "pred_final_odds": float(final_odds.get(umaban, float("nan"))),
                    }
                )
    return pd.DataFrame(rows, columns=PREDICTION_COLUMNS)


# ---------------------------------------------------------------------------
# 永続化
# ---------------------------------------------------------------------------


def persist_predictions(predictions: pd.DataFrame, path: str | None = None) -> None:
    """予測を pickle（PK 置き換え）+ SQLite（冪等 upsert）に保存する。

    path 既定は LocalPaths.RAW_ODDS_PREDICTIONS_PATH（テストでの差し替えを
    効かせるため、import 時でなく呼び出し時に解決する）。
    """
    path = path or LocalPaths.RAW_ODDS_PREDICTIONS_PATH
    if predictions is None or predictions.empty:
        return
    key_cols = ["race_id", "checkpoint", "model", "umaban"]
    if os.path.exists(path):
        existing = pd.read_pickle(path)
        new_keys = set(map(tuple, predictions[key_cols].to_numpy()))
        keep = existing[~existing[key_cols].apply(tuple, axis=1).isin(new_keys)]
        merged = pd.concat([keep, predictions], ignore_index=True)
    else:
        merged = predictions
    os.makedirs(os.path.dirname(path), exist_ok=True)
    merged.to_pickle(path)

    try:
        from src.storage import RawDataRepo

        inserted = RawDataRepo().upsert("raw_odds_predictions", predictions)
        logger.info("[odds_watch] DB upsert raw_odds_predictions: %d rows", inserted)
    except Exception as e:  # noqa: BLE001
        logger.warning("[odds_watch] DB upsert 失敗 (non-fatal): %s", e)


def load_predictions(path: str | None = None) -> pd.DataFrame:
    """保存済み予測テーブルを読む（無ければ空）。"""
    path = path or LocalPaths.RAW_ODDS_PREDICTIONS_PATH
    if not os.path.exists(path):
        return pd.DataFrame(columns=PREDICTION_COLUMNS)
    return pd.read_pickle(path)


def latest_final_odds_lookup(predictions: pd.DataFrame, model: str = "ensemble") -> dict:
    """{(race_id, umaban): 予測確定オッズ} を最新チェックポイント分から作る（EV 連携用）。"""
    if predictions is None or predictions.empty:
        return {}
    sel = predictions[predictions["model"] == model]
    if sel.empty:
        return {}
    sel = sel.sort_values("predicted_at").groupby(["race_id", "umaban"]).tail(1)
    return {
        (str(r.race_id), int(r.umaban)): float(r.pred_final_odds)
        for r in sel.itertuples()
        if pd.notna(r.pred_final_odds)
    }


# ---------------------------------------------------------------------------
# 1 サイクルの実行
# ---------------------------------------------------------------------------


def run_once(source=None, now: dt.datetime | None = None, date_str: str | None = None) -> dict:
    """取得 → 永続化 → 再計算 → 予測保存 の 1 サイクル。"""
    from src.preparing._odds_snapshot import make_snapshot
    from src.preparing._odds_source import NetkeibaOddsSource
    from src.preparing.odds_scheduler import load_snapshots
    from src.preparing.odds_scheduler import persist
    from src.preparing.odds_scheduler import select_checkpoint_races

    now = now or dt.datetime.now()
    date_str = date_str or now.strftime("%Y%m%d")
    source = source or NetkeibaOddsSource()

    races = source.fetch_today_races(date_str)
    targets = select_checkpoint_races(races, now)
    logger.info("[odds_watch] %s: 開催 %d レース / チェックポイント到来 %d レース",
                date_str, len(races), len(targets))

    captured = []
    from src.constants._bet_types import BetType

    for race_id, post_time, _phase in targets:
        try:
            win_odds = source.fetch_win_odds(race_id)
        except Exception as e:  # noqa: BLE001
            logger.warning("[odds_watch] 取得失敗 %s: %s", race_id, e)
            continue
        for umaban, odds in win_odds:
            captured.append(make_snapshot(str(race_id), BetType.TANSHO, [umaban], odds, post_time, now))

    if captured:
        persist(captured, LocalPaths.RAW_ODDS_SNAPSHOT_PATH)

    target_ids = [rid for rid, _, _ in targets]
    predictions = pd.DataFrame(columns=PREDICTION_COLUMNS)
    if target_ids:
        snapshots = load_snapshots(LocalPaths.RAW_ODDS_SNAPSHOT_PATH)
        predictions = recalculate_predictions(snapshots, target_ids, now)
        persist_predictions(predictions)

    return {
        "date": date_str,
        "n_races": len(races),
        "n_targets": len(targets),
        "n_snapshots": len(captured),
        "n_predictions": len(predictions),
    }


def main(argv: Sequence[str] | None = None) -> None:
    from src.constants._logging_config import setup_logging
    from src.preparing._odds_source import create_odds_source

    setup_logging()
    parser = argparse.ArgumentParser(description="時系列オッズの自動取得・自動再計算ウォッチャー")
    parser.add_argument("--source", default="netkeiba", choices=("netkeiba", "jravan"), help="オッズ取得元")
    parser.add_argument("--date", default=None, help="対象日 YYYYMMDD（既定: 今日）")
    parser.add_argument("--once", action="store_true", help="1 サイクルだけ実行（cron 用）")
    parser.add_argument("--loop", action="store_true", help="常駐ループ実行")
    parser.add_argument("--interval", type=int, default=120, help="--loop 時の実行間隔（秒）")
    args = parser.parse_args(argv)

    source = create_odds_source(args.source)
    try:
        if args.loop:
            logger.info("[odds_watch] 常駐モード開始 (interval=%ds)", args.interval)
            while True:
                result = run_once(source, date_str=args.date)
                logger.info("[odds_watch] %s", result)
                time.sleep(args.interval)
        else:
            result = run_once(source, date_str=args.date)
            logger.info("[odds_watch] %s", result)
    finally:
        source.close()


if __name__ == "__main__":
    main()
