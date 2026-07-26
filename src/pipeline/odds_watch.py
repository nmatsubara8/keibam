"""時系列オッズの自動取得・自動再計算ウォッチャー（CLI エントリ）。

タイマー（cron */3 分 or `--loop --interval 180` 常駐）で起動し:

1. オッズソース（netkeiba / JRA-VAN）から本日の (race_id, 発走時刻) を取得
2. 発走 30 分前〜実締切（+10分猶予 or 確定検知）のレースのオッズを毎ティック取得
3. OddsSnapshot として冪等永続化（pickle + SQLite、既存 odds_scheduler.persist）
4. 当該レースの全時点系列からオッズ力学モデル（Dirichlet / Kalman / Particle /
   Ensemble）で「次チェックポイントのシェア」「発走時の確定シェア・オッズ」を再計算
5. 予測を `data/raw/odds_predictions.pkl` + `raw_odds_predictions` テーブルへ保存

レイヤ: pipeline（preparing のソース・スケジューラと training のモデルを両方呼べる）。

使用例:
    python -m src.pipeline.odds_watch --once                # 1 サイクル（cron 用）
    python -m src.pipeline.odds_watch --loop --interval 180 # 常駐（3分おき）
    python -m src.pipeline.odds_watch --once --source jravan
    # 起動日時・時刻を制御（早めに常駐させ 9:30 開始・16:30 自動終了）:
    python -m src.pipeline.odds_watch --loop --start-at 09:30 --stop-at 16:30
    python -m src.pipeline.odds_watch --loop --start-at 2026-07-20T09:30 --stop-at 2026-07-20T16:30
    # 連休など複数開催日を事前予約（各日 start まで待って stop まで取得し順に実行）:
    python -m src.pipeline.odds_watch --schedule configs/odds_watch_schedule.example.json
"""

from __future__ import annotations

import argparse
import dataclasses
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


def run_once(
    source=None,
    now: dt.datetime | None = None,
    date_str: str | None = None,
    confirmed: "set[str] | None" = None,
) -> dict:
    """取得 → 永続化 → 再計算 → 予測保存 の 1 サイクル。

    confirmed: 締切確定済み（オッズ撤去を検知）レース ID の集合。--loop 実行ではティック間で
    引き継いで「確定したら以降取得しない」を実現する（cron --once では毎回空＝grace で打ち切り）。
    本サイクルで新たに確定したレースは confirmed に追記される（呼び出し側で再利用可）。
    """
    from src.preparing._odds_snapshot import make_snapshot
    from src.preparing._odds_source import NetkeibaOddsSource
    from src.preparing.odds_scheduler import load_snapshots
    from src.preparing.odds_scheduler import persist
    from src.preparing.odds_scheduler import select_checkpoint_races

    now = now or dt.datetime.now()
    date_str = date_str or now.strftime("%Y%m%d")
    source = source or NetkeibaOddsSource()
    confirmed = confirmed if confirmed is not None else set()

    races = source.fetch_today_races(date_str)
    targets = select_checkpoint_races(races, now, confirmed=confirmed)
    logger.info("[odds_watch] %s: 開催 %d レース / 取得対象 %d レース（確定済み %d 除外）",
                date_str, len(races), len(targets), len(confirmed))

    captured = []
    from src.constants._bet_types import BetType

    # 複勝も同一 b1 ページから追加リクエストなしで捕捉し、market overlay（複勝 vs 単勝）の
    # 蓄積を進める。KEIBA_ODDS_CAPTURE_PLACE=0 で従来どおり単勝のみ。複勝を返せるソース
    # （fetch_win_and_place_odds を持つ）だけで有効。持たないソース/スタブは単勝のみに自動フォールバック。
    capture_place = os.environ.get("KEIBA_ODDS_CAPTURE_PLACE", "1") not in ("0", "false", "False", "")
    combined = getattr(source, "fetch_win_and_place_odds", None) if capture_place else None

    # 連系(馬連/三連複)は別ページ＝**追加リクエスト**のため既定 OFF。連系 ΔR² 検証用に事前オッズを
    # 貯めるときだけ KEIBA_ODDS_CAPTURE_EXOTIC="umaren,sanrenpuku" 等で有効化する（各券種で 1 fetch 増）。
    exotic_env = os.environ.get("KEIBA_ODDS_CAPTURE_EXOTIC", "").strip()
    exotic_types = [t.strip() for t in exotic_env.split(",") if t.strip()]
    capture_exotic = getattr(source, "capture_bet_types", None) if exotic_types else None
    if exotic_types:
        logger.info("[odds_watch] 連系オッズも捕捉: %s（各券種で追加取得）", exotic_types)

    for race_id, post_time, _phase in targets:
        try:
            if combined is not None:
                win_odds, place_odds = combined(race_id)
            else:
                win_odds, place_odds = source.fetch_win_odds(race_id), []
        except Exception as e:  # noqa: BLE001
            logger.warning("[odds_watch] 取得失敗 %s: %s", race_id, e)
            continue
        if not win_odds:
            # ライブ発走を過ぎてオッズが空＝投票締切確定（ページからオッズ撤去）。発走時刻が
            # 遅延中なら post は未来（mtp>0）なので誤判定しない。確定したレースは以降スキップ。
            if (post_time - now).total_seconds() <= 0:
                confirmed.add(str(race_id))
                logger.info("[odds_watch] %s 締切確定（オッズ撤去）→ 以降スキップ", race_id)
            continue
        for umaban, odds in win_odds:
            captured.append(make_snapshot(str(race_id), BetType.TANSHO, [umaban], odds, post_time, now))
        # 複勝（単勝が取れたレースだけ）。overlay の元データ。勝率動力学は TANSHO フィルタ済で無影響。
        for umaban, odds in place_odds:
            captured.append(make_snapshot(str(race_id), BetType.FUKUSHO, [umaban], odds, post_time, now))
        # 連系（有効化時のみ・単勝が取れたライブレースだけ）。連系 ΔR² 検証用の事前オッズ蓄積。
        if capture_exotic is not None:
            captured.extend(capture_exotic(race_id, exotic_types, post_time, now))

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
        "n_confirmed": len(confirmed),
    }


# ---------------------------------------------------------------------------
# 取得状況サマリ（--status）
# ---------------------------------------------------------------------------


def summarize_status(snapshots, *, on_date: str | None = None, bet_type: str | None = None) -> list[dict]:
    """蓄積スナップショットをレース単位の取得状況に要約する（純粋関数）。

    on_date='YYYYMMDD' で captured_at の日付フィルタ、bet_type 指定で券種フィルタ
    （ウォッチャは単勝=TANSHO のみ取得）。各レースについて取得ティック数・頭数・最新の
    minutes_to_post・phase・取得時刻・推定発走時刻を集計し、発走順にソートして返す。
    """
    from collections import defaultdict

    groups: dict[str, list] = defaultdict(list)
    for s in snapshots:
        if bet_type is not None and s.bet_type != bet_type:
            continue
        if on_date and s.captured_at.strftime("%Y%m%d") != on_date:
            continue
        groups[str(s.race_id)].append(s)

    rows: list[dict] = []
    for rid, snaps in groups.items():
        ticks = sorted({s.captured_at for s in snaps})
        last_cap = ticks[-1]
        last_snaps = [s for s in snaps if s.captured_at == last_cap]
        last_mtp = min(s.minutes_to_post for s in last_snaps)
        rows.append({
            "race_id": rid,
            "n_ticks": len(ticks),
            "n_horses": len({s.combo[0] for s in last_snaps if s.combo}),
            "first_capture": ticks[0],
            "last_capture": last_cap,
            "last_mtp": last_mtp,
            "last_phase": last_snaps[0].phase,
            "est_post": last_cap + dt.timedelta(minutes=last_mtp),
        })
    rows.sort(key=lambda r: r["est_post"])
    return rows


def format_status_report(rows: list[dict], now: dt.datetime | None = None) -> str:
    """summarize_status の結果を人が読める表に整形する。"""
    now = now or dt.datetime.now()
    if not rows:
        return "（対象日の取得スナップショットはまだありません＝取得対象レース無し or 未起動）"
    header = (
        f"{'race_id':<14}{'取得回':>5}{'頭数':>5}{'最終mtp':>8}"
        f"{'phase':>11}{'最終取得':>8}{'平均間隔':>7}{'経過分':>7}"
    )
    lines = [f"=== odds_watch 取得状況（{len(rows)} レース）===", header, "-" * len(header)]
    for r in rows:
        span = (r["last_capture"] - r["first_capture"]).total_seconds() / 60
        avg = span / (r["n_ticks"] - 1) if r["n_ticks"] > 1 else 0.0
        ago = (now - r["last_capture"]).total_seconds() / 60
        lines.append(
            f"{r['race_id']:<14}{r['n_ticks']:>5}{r['n_horses']:>5}{r['last_mtp']:>+8}"
            f"{r['last_phase']:>11}{r['last_capture'].strftime('%H:%M'):>8}{avg:>6.1f}分{ago:>6.1f}分"
        )
    lines.append("\n（取得回=3分おきの取得回数 / 最終mtp=最後の取得時の締切まで分 / 経過分=最後の取得からの経過）")
    stuck = past_post_single_tick(rows, now)
    if stuck:
        lines.append(
            f"⚠ 発走済みで単一ティックのレース {len(stuck)} 本 = 軌跡を作れず力学評価が NaN: "
            f"{stuck[:10]}（ループが継続起動しているか確認）"
        )
    return "\n".join(lines)


def past_post_single_tick(rows: list[dict], now: dt.datetime) -> list[str]:
    """発走を過ぎたのに取得が1ティックしか無いレース ID を返す（純粋関数）。

    rows は `summarize_status` の出力。est_post ≤ now（発走済み）かつ n_ticks < 2 の
    レースは、同一レースの複数時刻オッズが無い＝軌跡を作れず evaluate-odds-dynamics が
    NaN になる（2026-07 に実際に踏んだ失敗）。この状態を早期検知するためのガード。
    """
    return [str(r["race_id"]) for r in rows if r["est_post"] <= now and int(r["n_ticks"]) < 2]


def check_capture_health(
    now: dt.datetime | None = None, date_str: str | None = None
) -> list[str]:
    """当日の取得状況を点検し、単一ティックで発走済みのレースがあれば警告する。

    ループ運用中に「取得ランタイムが繰り返し発火していない（=単一時刻化）」を即検知する
    ためのガード。戻り値は警告対象 race_id（テスト用）。ネットワークは使わず蓄積のみ読む。
    """
    from src.constants._bet_types import BetType
    from src.preparing.odds_scheduler import load_snapshots

    now = now or dt.datetime.now()
    date_str = date_str or now.strftime("%Y%m%d")
    snaps = load_snapshots(LocalPaths.RAW_ODDS_SNAPSHOT_PATH)
    rows = summarize_status(snaps, on_date=date_str, bet_type=BetType.TANSHO)
    stuck = past_post_single_tick(rows, now)
    if stuck:
        logger.warning(
            "[odds_watch] ⚠ 発走済みなのに単一ティックのレースが %d 本あります"
            "（同一レースの複数時刻オッズが無く evaluate-odds-dynamics が NaN になります）: %s"
            " — ループが継続起動しているか（--once の単発になっていないか）確認してください。",
            len(stuck), stuck[:10],
        )
    return stuck


def parse_when(value: str, now: dt.datetime) -> dt.datetime:
    """--start-at / --stop-at の文字列を datetime へ変換する（純粋関数）。

    受理する形式: ISO 完全形（"2026-07-20T09:00" / "2026-07-20 09:00"）または
    "HH:MM"（＝当日のその時刻）。曜日や相対指定は扱わない（曖昧さ回避）。
    """
    value = value.strip()
    try:
        return dt.datetime.fromisoformat(value)
    except ValueError:
        pass
    hh, mm = value.split(":")
    return now.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)


def wait_seconds(start_at: dt.datetime | None, now: dt.datetime) -> float:
    """start_at まで待機すべき秒数（未指定・過去なら 0）。純粋関数。"""
    if start_at is None:
        return 0.0
    return max(0.0, (start_at - now).total_seconds())


def should_stop(stop_at: dt.datetime | None, now: dt.datetime) -> bool:
    """stop_at 以降に達したか（未指定なら常に False＝無期限）。純粋関数。"""
    return stop_at is not None and now >= stop_at


# ---------------------------------------------------------------------------
# 複数開催日の起動予定（連休対応スケジュール）
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class CaptureSession:
    """スケジュール中の1開催日: date_str(YYYYMMDD) と当日の取得開始/終了 datetime。"""

    date_str: str
    start: dt.datetime
    stop: dt.datetime


def _parse_session_time(date_str: str, value: str) -> dt.datetime:
    """セッションの時刻文字列を datetime へ。ISO 完全形はそのまま、'HH:MM' は当該日の時刻。"""
    value = value.strip()
    try:
        return dt.datetime.fromisoformat(value)  # 日付込み ISO（"2026-07-20T09:30"）
    except ValueError:
        base = dt.datetime.strptime(date_str, "%Y%m%d")
        hh, mm = value.split(":")
        return base.replace(hour=int(hh), minute=int(mm))


def parse_schedule(data: dict) -> tuple[list[CaptureSession], int | None]:
    """スケジュール dict を (sessions, interval) へ検証しつつ変換する（純粋関数）。

    形式: ``{"interval": 120, "sessions": [{"date":"2026-07-20","start":"09:30","stop":"16:30"}, ...]}``
    date は "YYYY-MM-DD" / "YYYYMMDD" 両対応、start/stop は当日 'HH:MM' か ISO 完全形。
    stop ≤ start や日付不正は ValueError。sessions は start 昇順で返す。
    """
    raw = data.get("sessions")
    if not isinstance(raw, list) or not raw:
        raise ValueError("schedule に 'sessions' 配列がありません")
    sessions: list[CaptureSession] = []
    for i, e in enumerate(raw):
        date_str = str(e["date"]).replace("-", "")
        if len(date_str) != 8 or not date_str.isdigit():
            raise ValueError(f"sessions[{i}].date は YYYY-MM-DD か YYYYMMDD: {e['date']!r}")
        start = _parse_session_time(date_str, str(e["start"]))
        stop = _parse_session_time(date_str, str(e["stop"]))
        if stop <= start:
            raise ValueError(f"sessions[{i}]: stop({stop}) は start({start}) より後にしてください")
        sessions.append(CaptureSession(date_str, start, stop))
    sessions.sort(key=lambda s: s.start)
    interval = data.get("interval")
    return sessions, (int(interval) if interval is not None else None)


def load_schedule(path: str) -> tuple[list[CaptureSession], int | None]:
    """スケジュール JSON を読み込んで parse_schedule に委譲する。"""
    import json

    with open(path, encoding="utf-8") as f:
        return parse_schedule(json.load(f))


def pending_sessions(sessions: list[CaptureSession], now: dt.datetime) -> list[CaptureSession]:
    """まだ終わっていない（stop > now）セッションを start 昇順で返す（純粋関数）。

    既に終了時刻を過ぎた過去の開催日は自動的に読み飛ばす。連休の途中から起動しても、
    残りの開催日だけを順に実行できる。
    """
    return [s for s in sorted(sessions, key=lambda s: s.start) if s.stop > now]


def _run_capture_window(
    source,
    date_str: str | None,
    start_at: dt.datetime | None,
    stop_at: dt.datetime | None,
    interval: int,
) -> None:
    """1つの取得ウィンドウを実行する（start_at まで待機 → stop_at まで interval おきに取得）。

    --loop（単一ウィンドウ）と --schedule（連休の各開催日）の共通実行部。
    """
    delay = wait_seconds(start_at, dt.datetime.now())
    if delay > 0:
        logger.info("[odds_watch] 開始待機: %s まで %.0f 秒スリープ", start_at, delay)
        time.sleep(delay)
    logger.info(
        "[odds_watch] 取得開始 date=%s (interval=%ds%s)",
        date_str or "today", interval, f" / 終了予定 {stop_at}" if stop_at else "",
    )
    # 締切確定レースをティック間で保持し、確定後は再取得しない（当日分の早期停止）。
    confirmed: set[str] = set()
    last_date: str | None = None
    while True:
        if should_stop(stop_at, dt.datetime.now()):
            logger.info("[odds_watch] 終了時刻 %s に到達 → ウィンドウ終了（正常）", stop_at)
            break
        today = date_str or dt.datetime.now().strftime("%Y%m%d")
        if today != last_date:
            confirmed = set()  # 日付が変わったら確定集合をリセット
            last_date = today
        result = run_once(source, date_str=date_str, confirmed=confirmed)
        logger.info("[odds_watch] %s", result)
        # 各ティックで蓄積を自己点検し、単一時刻化（=軌跡が作れない）を即警告する。
        check_capture_health(date_str=date_str)
        if should_stop(stop_at, dt.datetime.now()):
            logger.info("[odds_watch] 終了時刻 %s に到達 → ウィンドウ終了（正常）", stop_at)
            break
        time.sleep(interval)


def main(argv: Sequence[str] | None = None) -> None:
    from src.constants._bet_types import BetType
    from src.constants._logging_config import setup_logging
    from src.preparing._odds_source import create_odds_source

    setup_logging()
    parser = argparse.ArgumentParser(description="時系列オッズの自動取得・自動再計算ウォッチャー")
    parser.add_argument("--source", default="netkeiba", choices=("netkeiba", "jravan"), help="オッズ取得元")
    parser.add_argument("--date", default=None, help="対象日 YYYYMMDD（既定: 今日）")
    parser.add_argument("--once", action="store_true", help="1 サイクルだけ実行（cron 用）")
    parser.add_argument("--loop", action="store_true", help="常駐ループ実行")
    parser.add_argument("--interval", type=int, default=120, help="--loop 時の実行間隔（秒）")
    parser.add_argument(
        "--start-at", default=None,
        help="--loop 開始時刻。この時刻まで待ってから取得を始める（ISO 'YYYY-MM-DDTHH:MM' or 当日 'HH:MM'）",
    )
    parser.add_argument(
        "--stop-at", default=None,
        help="--loop 終了時刻。この時刻に達したらループを抜けて正常終了する（ISO or 当日 'HH:MM'）",
    )
    parser.add_argument(
        "--schedule", default=None,
        help="連休など複数開催日の起動予定 JSON。各開催日を start まで待って stop まで取得し順に実行する"
             "（例 configs/odds_watch_schedule.example.json）",
    )
    parser.add_argument(
        "--status", action="store_true",
        help="本日（or --date）の取得状況をレース単位で一覧表示して終了（取得・ネットワークなし）",
    )
    args = parser.parse_args(argv)

    # --status: スクレイプせず蓄積スナップショットを要約するだけ（cron 監視・watch 用）
    if args.status:
        from src.preparing.odds_scheduler import load_snapshots

        on_date = args.date or dt.datetime.now().strftime("%Y%m%d")
        snaps = load_snapshots(LocalPaths.RAW_ODDS_SNAPSHOT_PATH)
        rows = summarize_status(snaps, on_date=on_date, bet_type=BetType.TANSHO)
        print(format_status_report(rows))
        # 発走済みで単一ティックのレースを明示警告（軌跡が作れない＝ NaN の予兆）。
        check_capture_health(date_str=on_date)
        return

    # 開始/終了スケジュール（--loop 時のみ有効）。cron 不安定な環境でもアプリ内で
    # 「いつ起動し・いつ止めるか」を確定させる。now は解釈基準（当日 'HH:MM' 用）。
    _now0 = dt.datetime.now()
    start_at = parse_when(args.start_at, _now0) if args.start_at else None
    stop_at = parse_when(args.stop_at, _now0) if args.stop_at else None

    source = create_odds_source(args.source)
    try:
        if args.schedule:
            # 連休など複数開催日の起動予定。過去日を読み飛ばし、残りを start→stop 順に実行する。
            sessions, sched_interval = load_schedule(args.schedule)
            interval = sched_interval or args.interval
            pend = pending_sessions(sessions, dt.datetime.now())
            if not pend:
                logger.info("[odds_watch] スケジュールに未実施の開催日がありません（全て終了時刻を経過）")
                return
            logger.info(
                "[odds_watch] スケジュール: %d 開催日を実行予定 %s",
                len(pend),
                [f"{s.date_str} {s.start:%H:%M}-{s.stop:%H:%M}" for s in pend],
            )
            for s in pend:
                _run_capture_window(source, s.date_str, s.start, s.stop, interval)
            logger.info("[odds_watch] スケジュール完了（全 %d 開催日）", len(pend))
        elif args.loop:
            _run_capture_window(source, args.date, start_at, stop_at, args.interval)
        else:
            result = run_once(source, date_str=args.date)
            logger.info("[odds_watch] %s", result)
    finally:
        source.close()


if __name__ == "__main__":
    main()
