"""netkeiba 取得ジョブの多重起動防止（advisory file lock）。

二重起動は短時間の累積リクエスト増加を招き netkeiba のソフト BAN の主因になる。
`backfill-horses` / `backfill-peds` など「実際に netkeiba を叩くジョブ」は、取得開始前に
`acquire_scrape_lock()` で排他ロックを取り、別プロセス（手動実行でも resilient ラッパーの
子プロセスでも）が保持していれば `AlreadyRunning` で中止する。

ロックは `fcntl.flock`（advisory）で、返した fd を保持し続ける限り有効。プロセス終了で自動解放
されるためスタール（PID ファイルの残骸）問題が無い。lock パスは env `KEIBA_SCRAPE_LOCK` で上書き可。
"""

from __future__ import annotations

import fcntl
import os

DEFAULT_LOCK_PATH = "/tmp/keibam_netkeiba_scrape.lock"


class AlreadyRunning(RuntimeError):
    """別の netkeiba 取得処理が稼働中でロックを取得できないことを表す。"""


def scrape_lock_path() -> str:
    return os.environ.get("KEIBA_SCRAPE_LOCK", DEFAULT_LOCK_PATH)


def acquire_scrape_lock(name: str = "scrape"):
    """netkeiba 取得の排他ロックを取得し、開いた fd を返す（呼び出し側で保持し続けること）。

    既に別プロセスが保持していれば `AlreadyRunning`。fd はプロセス終了で自動解放される。
    """
    path = scrape_lock_path()
    fd = open(path, "w")  # noqa: SIM115 ロックはプロセス寿命の間 open のまま保持する
    try:
        fcntl.flock(fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as e:
        fd.close()
        raise AlreadyRunning(
            f"別の netkeiba 取得処理が稼働中です（lock: {path}）。"
            "二重起動は BAN の原因になるため中止します。"
            "稼働中のジョブの完了を待つか、そちらを停止してください。"
        ) from e
    try:
        fd.truncate(0)
        fd.write(f"pid={os.getpid()} name={name}\n")
        fd.flush()
    except OSError:
        pass  # 診断用の書き込み失敗は致命的でない
    return fd
