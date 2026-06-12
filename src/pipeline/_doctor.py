"""ヘルスチェック（doctor）の純粋ロジック。

データ/モデル/DB/ディスク/featured メタの健全性を点検し、OK/WARN/ERROR を集約する。
CLI（run_pipeline doctor）と dashboard 鮮度バッジ（app/_data_loader）から再利用する。

レイヤ規約: pipeline（最上位）。constants/storage を import 可、app は import 不可。
モデル走査は app の find_model_paths を使えないため本モジュールに内製する。
I/O（mtime/disk/DB 接続）はあるが、now や各種パスを引数で受けてテスト決定化する。
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import os
import shutil
from typing import Optional

OK = "OK"
WARN = "WARN"
ERROR = "ERROR"


@dataclasses.dataclass(frozen=True)
class CheckResult:
    name: str
    level: str  # OK | WARN | ERROR
    detail: str


# ---------------------------------------------------------------------------
# 個別チェック
# ---------------------------------------------------------------------------


def _age_hours(path: str, now: dt.datetime) -> float:
    return (now.timestamp() - os.path.getmtime(path)) / 3600.0


def check_file(
    name: str,
    path: str,
    *,
    now: dt.datetime,
    warn_age_h: Optional[float] = None,
    err_age_h: Optional[float] = None,
    required: bool = True,
) -> CheckResult:
    """ファイルの存在と鮮度（mtime からの経過時間）を点検する。"""
    if not os.path.exists(path):
        level = ERROR if required else WARN
        return CheckResult(name, level, f"見つかりません: {path}")
    age = _age_hours(path, now)
    if err_age_h is not None and age >= err_age_h:
        return CheckResult(name, ERROR, f"{age:.1f}h 前（>= {err_age_h}h）: {path}")
    if warn_age_h is not None and age >= warn_age_h:
        return CheckResult(name, WARN, f"{age:.1f}h 前（>= {warn_age_h}h）: {path}")
    return CheckResult(name, OK, f"{age:.1f}h 前: {path}")


def model_pickle_paths(models_dir: str) -> list[str]:
    """models/<date>/*.pickle を新しい順（mtime 降順）で返す（app に依存しない内製版）。"""
    out: list[str] = []
    if not os.path.isdir(models_dir):
        return out
    for date_dir in os.listdir(models_dir):
        full = os.path.join(models_dir, date_dir)
        if not os.path.isdir(full):
            continue
        for fname in os.listdir(full):
            if fname.endswith(".pickle"):
                out.append(os.path.join(full, fname))
    out.sort(key=os.path.getmtime, reverse=True)
    return out


def check_models(models_dir: str, *, now: dt.datetime, warn_age_h: Optional[float] = None) -> CheckResult:
    paths = model_pickle_paths(models_dir)
    if not paths:
        return CheckResult("models", ERROR, "モデルがありません（retrain 未実行）")
    newest = paths[0]
    age = _age_hours(newest, now)
    rel = os.path.relpath(newest, models_dir)
    if warn_age_h is not None and age >= warn_age_h:
        return CheckResult("models", WARN, f"最新 {rel} は {age:.1f}h 前（>= {warn_age_h}h、{len(paths)} 世代）")
    return CheckResult("models", OK, f"最新 {rel}（{age:.1f}h 前、{len(paths)} 世代）")


def check_db_connection(db_path: Optional[str] = None) -> CheckResult:
    try:
        from sqlalchemy import text

        from src.storage._db import get_engine

        engine = get_engine(db_path)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return CheckResult("db", OK, f"接続OK: {db_path or 'default'}")
    except Exception as e:  # noqa: BLE001
        return CheckResult("db", ERROR, f"接続失敗: {e}")


def check_disk_space(path: str, *, warn_free_gb: float = 5.0, err_free_gb: float = 1.0) -> CheckResult:
    try:
        free_gb = shutil.disk_usage(path).free / 1024 ** 3
    except OSError as e:
        return CheckResult("disk", WARN, f"取得失敗: {e}")
    if free_gb < err_free_gb:
        return CheckResult("disk", ERROR, f"空き {free_gb:.1f}GB (< {err_free_gb}GB)")
    if free_gb < warn_free_gb:
        return CheckResult("disk", WARN, f"空き {free_gb:.1f}GB (< {warn_free_gb}GB)")
    return CheckResult("disk", OK, f"空き {free_gb:.1f}GB")


def check_featured_meta(db_path: Optional[str] = None) -> CheckResult:
    try:
        from src.storage._featured import load_featured_meta

        meta = load_featured_meta(db_path=db_path)
    except Exception as e:  # noqa: BLE001
        return CheckResult("featured_meta", WARN, f"取得失敗: {e}")
    if not meta:
        return CheckResult("featured_meta", WARN, "メタ記録なし（ingest/retrain 未実行）")
    return CheckResult(
        "featured_meta", OK,
        f"n_rows={meta.get('n_rows')} created_at={meta.get('created_at')}",
    )


# ---------------------------------------------------------------------------
# 集約
# ---------------------------------------------------------------------------


def overall_level(results: list[CheckResult]) -> str:
    if any(r.level == ERROR for r in results):
        return ERROR
    if any(r.level == WARN for r in results):
        return WARN
    return OK


def run_doctor(
    *,
    now: Optional[dt.datetime] = None,
    data_paths: Optional[dict[str, str]] = None,
    models_dir: str = "models",
    db_path: Optional[str] = None,
    data_warn_age_h: float = 48.0,
    model_warn_age_h: float = 24.0 * 14,  # 2 週間
    warn_free_gb: float = 5.0,
) -> tuple[list[CheckResult], str]:
    """全チェックを実行し、(結果リスト, 総合レベル) を返す。

    data_paths を省略すると LocalPaths から既定の重要ファイル群を点検する。
    """
    now = now or dt.datetime.now()

    if data_paths is None:
        from src.constants._local_paths import LocalPaths

        data_paths = {
            "results.pkl": LocalPaths.RAW_RESULTS_PATH,
            "race_info.pkl": LocalPaths.RAW_RACE_INFO_PATH,
            "featured_data.pkl": LocalPaths.FEATURED_DATA_PATH,
        }

    results: list[CheckResult] = []
    for name, path in data_paths.items():
        results.append(check_file(name, path, now=now, warn_age_h=data_warn_age_h))
    results.append(check_models(models_dir, now=now, warn_age_h=model_warn_age_h))
    results.append(check_db_connection(db_path))
    results.append(check_featured_meta(db_path))
    results.append(check_disk_space(models_dir if os.path.isdir(models_dir) else ".", warn_free_gb=warn_free_gb))

    return results, overall_level(results)
