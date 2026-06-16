"""Optuna チューニング（retrain --with-tuning）の UI 起動・状態監視。

設計（Streamlit から長時間ジョブを安全に回す）:
- ボタン押下で `subprocess` により `python -m src.pipeline.run_pipeline retrain
  --with-tuning` を**デタッチ起動**（start_new_session=True）。Streamlit の再実行や
  ブラウザ切断後もジョブは生き続ける。
- 進捗は `models/tuning_job.log` に追記し、UI は末尾を tail 表示する。
- ジョブ状態（pid / 開始時刻 / 完了 / 終了コード）は `models/tuning_job.json` に記録。
  プロセス完了は「pid の生死」＋「ログ末尾の終了センチネル `__TUNING_EXIT__ <code>`」で判定する
  （Popen オブジェクトは Streamlit 再実行で失われるため、終了コードはログ経由で回収する）。
- 多重起動はステータスが running の間ブロックする。

streamlit に依存しない純粋ロジックとして実装し、UI（5_model_lab.py）から呼ぶ。
"""

from __future__ import annotations

import datetime as dt
import json
import os
import shlex
import signal
import subprocess
import sys
from pathlib import Path
from typing import Callable, Optional

# プロセス完了時にログへ書き込む終了センチネル（終了コードを後から回収するため）。
_EXIT_SENTINEL = "__TUNING_EXIT__"

_STATUS_FILE = "tuning_job.json"
_LOG_FILE = "tuning_job.log"


def _repo_root() -> Path:
    """リポジトリルート（app/ の親）。subprocess の cwd に使う。"""
    return Path(__file__).resolve().parent.parent


def status_path(models_dir: str = "models") -> str:
    return os.path.join(models_dir, _STATUS_FILE)


def log_path(models_dir: str = "models") -> str:
    return os.path.join(models_dir, _LOG_FILE)


# ---------------------------------------------------------------------------
# 読み取り
# ---------------------------------------------------------------------------


def read_job_status(models_dir: str = "models") -> Optional[dict]:
    """ジョブ状態 JSON を読む（無ければ None）。"""
    path = status_path(models_dir)
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def tail_log(models_dir: str = "models", n: int = 40) -> str:
    """ログ末尾 n 行を返す（無ければ空文字）。"""
    path = log_path(models_dir)
    if not os.path.exists(path):
        return ""
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except OSError:
        return ""
    return "".join(lines[-n:])


def is_process_alive(pid: int) -> bool:
    """pid のプロセスが生存しているか（シグナル 0 で確認）。"""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # 存在するが権限なし
    return True


# ---------------------------------------------------------------------------
# 起動
# ---------------------------------------------------------------------------


def _write_status(models_dir: str, payload: dict) -> None:
    os.makedirs(models_dir, exist_ok=True)
    path = status_path(models_dir)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def start_tuning_job(
    models_dir: str = "models",
    *,
    popen: Callable[..., "subprocess.Popen"] = subprocess.Popen,
    now: Optional[Callable[[], dt.datetime]] = None,
    is_alive: Callable[[int], bool] = is_process_alive,
) -> dict:
    """retrain --with-tuning をデタッチ起動し、ジョブ状態を記録して返す。

    既に running のジョブがあれば RuntimeError（多重起動防止）。
    `popen` / `now` / `is_alive` は単体テスト用に差し替え可能。
    """
    now = now or dt.datetime.now

    current = refresh_job_status(models_dir, is_alive=is_alive)
    if current is not None and current.get("status") == "running":
        raise RuntimeError("チューニングジョブは既に実行中です。")

    os.makedirs(models_dir, exist_ok=True)
    lp = log_path(models_dir)

    cmd = [sys.executable, "-m", "src.pipeline.run_pipeline", "retrain", "--with-tuning"]
    inner = " ".join(shlex.quote(c) for c in cmd)
    qlog = shlex.quote(lp)
    # 実行 → ログへ追記。完了後に終了コードをセンチネルで記録（再実行越しに回収するため）。
    shell_cmd = f'{inner} >> {qlog} 2>&1; echo "{_EXIT_SENTINEL} $?" >> {qlog}'

    started_at = now().isoformat(timespec="seconds")
    with open(lp, "a", encoding="utf-8") as logf:
        logf.write(f"\n===== tuning start {started_at} =====\n")
        logf.write(f"$ {inner}\n")

    proc = popen(
        shell_cmd,
        shell=True,
        cwd=str(_repo_root()),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,  # Streamlit から切り離して延命
    )

    payload = {
        "status": "running",
        "pid": int(proc.pid),
        "started_at": started_at,
        "finished_at": None,
        "exit_code": None,
        "cmd": cmd,
        "log_path": lp,
    }
    _write_status(models_dir, payload)
    return payload


# ---------------------------------------------------------------------------
# 状態更新（プロセス完了の検出）
# ---------------------------------------------------------------------------


def _parse_exit_code(models_dir: str) -> Optional[int]:
    """ログ末尾の終了センチネル `__TUNING_EXIT__ <code>` から終了コードを回収する。"""
    text = tail_log(models_dir, n=200)
    code: Optional[int] = None
    for line in text.splitlines():
        s = line.strip()
        if s.startswith(_EXIT_SENTINEL):
            try:
                code = int(s.split()[-1])
            except (ValueError, IndexError):
                code = None
    return code


def refresh_job_status(
    models_dir: str = "models",
    *,
    is_alive: Callable[[int], bool] = is_process_alive,
    now: Optional[Callable[[], dt.datetime]] = None,
) -> Optional[dict]:
    """running ジョブのプロセス生死を確認し、完了していれば状態を確定して書き戻す。

    - pid 生存 → running のまま
    - pid 終了 + センチネル `EXIT 0` → completed
    - pid 終了 + センチネル `EXIT != 0` → failed
    - pid 終了 + センチネル無し → unknown（中断・kill 等）
    """
    now = now or dt.datetime.now
    status = read_job_status(models_dir)
    if status is None or status.get("status") != "running":
        return status

    pid = int(status.get("pid", -1))
    if is_alive(pid):
        return status

    code = _parse_exit_code(models_dir)
    if code is None:
        status["status"] = "unknown"
    elif code == 0:
        status["status"] = "completed"
    else:
        status["status"] = "failed"
    status["exit_code"] = code
    status["finished_at"] = now().isoformat(timespec="seconds")
    _write_status(models_dir, status)
    return status


def stop_tuning_job(
    models_dir: str = "models",
    *,
    is_alive: Callable[[int], bool] = is_process_alive,
    killer: Callable[[int, int], None] = os.killpg,
    get_pgid: Callable[[int], int] = os.getpgid,
    now: Optional[Callable[[], dt.datetime]] = None,
) -> Optional[dict]:
    """running ジョブを停止する（プロセスグループに SIGTERM）。状態を cancelled に更新。"""
    now = now or dt.datetime.now
    status = read_job_status(models_dir)
    if status is None or status.get("status") != "running":
        return status
    pid = int(status.get("pid", -1))
    if pid > 0 and is_alive(pid):
        try:
            killer(get_pgid(pid), signal.SIGTERM)
        except (ProcessLookupError, PermissionError, OSError):
            pass
    status["status"] = "cancelled"
    status["finished_at"] = now().isoformat(timespec="seconds")
    _write_status(models_dir, status)
    return status
