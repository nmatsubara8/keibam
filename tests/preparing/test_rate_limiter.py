"""netkeiba ポライトネス制御（src/preparing/_rate_limiter.py）のテスト。

- polite_interval: 最低 1 秒への切上げ・ランダム揺らぎ・明示無効化（base<=0）
- HourlyRateLimiter: スライディングウィンドウの上限・窓抜けでの回復・無効化
- get_hourly_limiter: プロセス共有シングルトンと環境変数構築
"""

from __future__ import annotations

import pytest

from src.preparing._rate_limiter import (
    MIN_INTERVAL_SEC,
    HourlyRateLimiter,
    _reset_for_testing,
    get_hourly_limiter,
    polite_interval,
)


@pytest.fixture(autouse=True)
def _reset_singleton():
    _reset_for_testing()
    yield
    _reset_for_testing()


# ---------------------------------------------------------------------------
# polite_interval
# ---------------------------------------------------------------------------


class TestPoliteInterval:
    def test_zero_base_disables_wait(self):
        # 既存テスト慣行（rate_limit_sec=0 で待機オフ）を維持する
        assert polite_interval(0) == 0.0
        assert polite_interval(-1) == 0.0

    def test_floors_base_to_min_interval(self):
        # 1 秒未満の正の値は netkeiba 自主規制の最低 1 秒に切上げる
        v = polite_interval(0.2, jitter_max=0)
        assert v == MIN_INTERVAL_SEC

    def test_jitter_within_range(self):
        # 既定 jitter（2.0）で合計 1〜3 秒程度になる
        for _ in range(50):
            v = polite_interval(1.0, jitter_max=2.0)
            assert MIN_INTERVAL_SEC <= v <= MIN_INTERVAL_SEC + 2.0

    def test_jitter_uses_injected_rng(self):
        v = polite_interval(1.0, jitter_max=2.0, rng=lambda a, b: 1.5)
        assert v == 2.5

    def test_env_defaults(self, monkeypatch):
        monkeypatch.setenv("KEIBA_SCRAPE_DELAY", "1.4")
        monkeypatch.setenv("KEIBA_SCRAPE_JITTER_MAX", "0")
        assert polite_interval() == 1.4

    def test_env_zero_disables(self, monkeypatch):
        monkeypatch.setenv("KEIBA_SCRAPE_DELAY", "0")
        assert polite_interval() == 0.0


# ---------------------------------------------------------------------------
# HourlyRateLimiter
# ---------------------------------------------------------------------------


class _FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, sec: float) -> None:
        self.now += sec


class TestHourlyRateLimiter:
    def test_disabled_when_nonpositive_cap(self):
        lim = HourlyRateLimiter(max_per_hour=0)
        for _ in range(10):
            assert lim.try_acquire() == 0.0

    def test_acquires_under_cap_without_wait(self):
        clock = _FakeClock()
        lim = HourlyRateLimiter(max_per_hour=3, clock=clock)
        assert lim.try_acquire() == 0.0
        assert lim.try_acquire() == 0.0
        assert lim.try_acquire() == 0.0

    def test_returns_wait_when_cap_reached(self):
        clock = _FakeClock()
        lim = HourlyRateLimiter(max_per_hour=2, clock=clock)
        lim.try_acquire()
        clock.advance(10)
        lim.try_acquire()
        clock.advance(10)
        wait = lim.try_acquire()
        # 最古（t=1000）が窓から抜けるまで 3600-20 秒
        assert wait == pytest.approx(3580.0)

    def test_recovers_after_window_expiry(self):
        clock = _FakeClock()
        lim = HourlyRateLimiter(max_per_hour=1, clock=clock)
        assert lim.try_acquire() == 0.0
        assert lim.try_acquire() > 0.0
        clock.advance(3600.0)
        assert lim.try_acquire() == 0.0

    def test_blocking_acquire_waits_until_slot_frees(self):
        clock = _FakeClock()
        sleeps: list[float] = []

        def sleeper(sec: float) -> None:
            sleeps.append(sec)
            clock.advance(sec)

        lim = HourlyRateLimiter(max_per_hour=1, clock=clock, sleeper=sleeper)
        assert lim.acquire() == 0.0  # 1 件目は即時
        waited = lim.acquire()  # 2 件目は窓が空くまで待つ
        assert waited == pytest.approx(3600.0)
        assert len(sleeps) == 1

    def test_cap_from_env(self, monkeypatch):
        monkeypatch.setenv("KEIBA_MAX_REQUESTS_PER_HOUR", "42")
        lim = HourlyRateLimiter()
        assert lim.max_per_hour == 42


# ---------------------------------------------------------------------------
# シングルトン
# ---------------------------------------------------------------------------


class TestGetHourlyLimiter:
    def test_returns_same_instance(self):
        assert get_hourly_limiter() is get_hourly_limiter()

    def test_reset_recreates(self, monkeypatch):
        first = get_hourly_limiter()
        _reset_for_testing()
        monkeypatch.setenv("KEIBA_MAX_REQUESTS_PER_HOUR", "7")
        second = get_hourly_limiter()
        assert second is not first
        assert second.max_per_hour == 7

    def test_default_cap_is_positive(self):
        # 既定で自主上限が有効になっている（無制限ではない）
        assert get_hourly_limiter().max_per_hour > 0
