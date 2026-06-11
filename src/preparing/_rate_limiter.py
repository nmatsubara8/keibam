"""netkeiba 向けスクレイピングのポライトネス制御（公式クローラー対策への自主規制）。

netkeiba はスクレイピングに関連して通信制限が起きる旨を公式に案内しており、
2024 年 11 月のクローラー対策強化により User-Agent 未設定では HTTP 400 が返る
（UA は `_scraper.py` の `_DEFAULT_USER_AGENT` で対応済み）。本モジュールは
残りの自主規制 2 点を一元管理する:

1. リクエスト間隔: 最低 1 秒以上 + ランダムな揺らぎ（既定で合計 1〜3 秒程度）
2. 大量取得時の 1 時間あたりリクエスト数の自主上限（スライディングウィンドウ）

従来は `modules.py`（固定 ``time.sleep(delay)``）と `_scraper.py`
（固定 ``rate_limit_sec``）に分散していた待機ロジックの SSOT。

環境変数:
    KEIBA_SCRAPE_DELAY           基準待機秒（既定 1.0。正の値は 1.0 未満でも 1.0 に切上げ、
                                 0 以下は明示無効化＝待機なし。テスト用エスケープハッチ）
    KEIBA_SCRAPE_JITTER_MAX      揺らぎ上限秒（既定 2.0 → 合計 1〜3 秒）
    KEIBA_MAX_REQUESTS_PER_HOUR  1 時間あたり上限（既定 1000。0 以下で無効）

レイヤ: preparing（`_scraper.py` / `modules.py` から利用）。
"""

from __future__ import annotations

import logging
import os
import random
import threading
import time
from collections import deque
from typing import Callable, Optional

logger = logging.getLogger(__name__)

_HOUR_SEC = 3600.0

# 最低インターバル（netkeiba 自主規制）。これ未満の正の値はここまで切上げる。
MIN_INTERVAL_SEC = 1.0


# ---------------------------------------------------------------------------
# リクエスト間隔（最低 1 秒 + ランダム揺らぎ）
# ---------------------------------------------------------------------------


def polite_interval(
    base: Optional[float] = None,
    jitter_max: Optional[float] = None,
    rng: Callable[[float, float], float] = random.uniform,
) -> float:
    """1 リクエスト間に置くべき待機秒を返す（待機自体は呼出側が行う）。

    - ``base`` が None なら環境変数 KEIBA_SCRAPE_DELAY（既定 1.0）。
    - ``base <= 0`` は「明示的な無効化」とみなし 0.0 を返す
      （単体テストが ``rate_limit_sec=0`` で待機を切る既存慣行を維持）。
    - 正の ``base`` は MIN_INTERVAL_SEC (1.0) 未満でも 1.0 に切上げる。
    - 揺らぎは ``uniform(0, jitter_max)`` を加算（既定 2.0 → 合計 1〜3 秒）。
    """
    if base is None:
        base = float(os.environ.get("KEIBA_SCRAPE_DELAY", "1.0"))
    if base <= 0:
        return 0.0
    base = max(base, MIN_INTERVAL_SEC)
    if jitter_max is None:
        jitter_max = float(os.environ.get("KEIBA_SCRAPE_JITTER_MAX", "2.0"))
    jitter = rng(0.0, jitter_max) if jitter_max > 0 else 0.0
    return base + jitter


# ---------------------------------------------------------------------------
# 1 時間あたりリクエスト数の自主上限
# ---------------------------------------------------------------------------


class HourlyRateLimiter:
    """スライディングウィンドウ式の 1 時間あたりリクエスト数リミッタ。

    `try_acquire()` はノンブロッキング（async 呼出側が ``asyncio.sleep`` で待てる
    よう、必要待機秒を返す）。`acquire()` は同期ブロッキングの便宜ラッパ。
    スレッドセーフ（同期パイプラインと Playwright 同期ブリッジの両方から呼ばれる）。
    """

    def __init__(
        self,
        max_per_hour: Optional[int] = None,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if max_per_hour is None:
            max_per_hour = int(os.environ.get("KEIBA_MAX_REQUESTS_PER_HOUR", "1000"))
        self._max = max_per_hour
        self._clock = clock
        self._sleep = sleeper
        self._stamps: deque[float] = deque()
        self._lock = threading.Lock()

    @property
    def max_per_hour(self) -> int:
        return self._max

    def try_acquire(self) -> float:
        """枠があれば 1 件記録して 0.0 を返す。満杯なら必要待機秒を返す（記録しない）。"""
        if self._max <= 0:
            return 0.0
        with self._lock:
            now = self._clock()
            while self._stamps and now - self._stamps[0] >= _HOUR_SEC:
                self._stamps.popleft()
            if len(self._stamps) < self._max:
                self._stamps.append(now)
                return 0.0
            # 最古のリクエストが窓から抜けるまでの残り秒
            return max(_HOUR_SEC - (now - self._stamps[0]), 0.05)

    def acquire(self) -> float:
        """枠が空くまでブロックして取得する。合計待機秒を返す（同期呼出用）。"""
        waited = 0.0
        while True:
            wait = self.try_acquire()
            if wait <= 0:
                return waited
            logger.warning(
                "[rate-limit] 1時間あたり上限 %d 件に到達。%.0f 秒待機します", self._max, wait
            )
            self._sleep(wait)
            waited += wait


# ---------------------------------------------------------------------------
# プロセス共有シングルトン（全 PlaywrightScraper.fetch が同じ窓を共有する）
# ---------------------------------------------------------------------------

_global_limiter: Optional[HourlyRateLimiter] = None
_global_lock = threading.Lock()


def get_hourly_limiter() -> HourlyRateLimiter:
    """プロセス共有の HourlyRateLimiter を返す（初回呼出時に環境変数から構築）。"""
    global _global_limiter
    with _global_lock:
        if _global_limiter is None:
            _global_limiter = HourlyRateLimiter()
        return _global_limiter


def _reset_for_testing() -> None:
    """テスト用: シングルトンを破棄して次回 get で再構築させる。"""
    global _global_limiter
    with _global_lock:
        _global_limiter = None
