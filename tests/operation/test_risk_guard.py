"""src/operation/_risk_guard.py: 損失ストップ・実効 bankroll のテスト。

合成した投票履歴（settled / 未確定混在）で、実現損益・当日損失・kill switch 判定・
実効 bankroll を決定的に検証する。
"""

from __future__ import annotations

import datetime as dt

from src.operation._config import OperationConfig
from src.operation._risk_guard import (
    daily_realized_loss,
    effective_bankroll,
    evaluate_kill_switch,
    realized_pnl,
)


def _settled(stake, payout, settled_at):
    return {"status": "settled", "stake": stake, "payout": payout, "settled_at": settled_at}


def _pending(stake):
    return {"status": "recommended", "stake": stake}


# ---------------------------------------------------------------------------
# realized_pnl
# ---------------------------------------------------------------------------


class TestRealizedPnl:
    def test_sums_settled_only(self):
        history = [
            _settled(100, 250, "2026-06-11T10:00:00"),   # +150
            _settled(100, 0, "2026-06-11T11:00:00"),     # -100
            _pending(100),                               # 未確定 → 無視
        ]
        assert realized_pnl(history) == 50.0

    def test_empty_is_zero(self):
        assert realized_pnl([]) == 0.0

    def test_ignores_malformed_amounts(self):
        history = [{"status": "settled", "stake": "x", "payout": None,
                    "settled_at": "2026-06-11T10:00:00"}]
        assert realized_pnl(history) == 0.0


# ---------------------------------------------------------------------------
# daily_realized_loss
# ---------------------------------------------------------------------------


class TestDailyRealizedLoss:
    def test_loss_on_target_day(self):
        day = dt.date(2026, 6, 11)
        history = [
            _settled(1000, 0, "2026-06-11T10:00:00"),    # -1000 当日
            _settled(1000, 3000, "2026-06-11T12:00:00"),  # +2000 当日 → 合計 +1000 → loss 0
        ]
        assert daily_realized_loss(history, day) == 0.0

    def test_net_negative_returns_positive_loss(self):
        day = dt.date(2026, 6, 11)
        history = [
            _settled(1000, 0, "2026-06-11T10:00:00"),    # -1000
            _settled(500, 200, "2026-06-11T12:00:00"),    # -300
        ]
        assert daily_realized_loss(history, day) == 1300.0

    def test_other_day_excluded(self):
        day = dt.date(2026, 6, 11)
        history = [_settled(1000, 0, "2026-06-10T10:00:00")]  # 前日 → 対象外
        assert daily_realized_loss(history, day) == 0.0

    def test_missing_settled_at_skipped(self):
        day = dt.date(2026, 6, 11)
        history = [{"status": "settled", "stake": 1000, "payout": 0}]  # settled_at なし
        assert daily_realized_loss(history, day) == 0.0


# ---------------------------------------------------------------------------
# evaluate_kill_switch
# ---------------------------------------------------------------------------


class TestKillSwitch:
    def _cfg(self, **kw):
        base = {"bankroll": 100000.0, "max_daily_loss_ratio": 0.3}
        base.update(kw)
        return OperationConfig(**base)

    def test_blocks_when_loss_reaches_limit(self):
        day = dt.date(2026, 6, 11)
        # 上限 = 100000 * 0.3 = 30000。損失ちょうど 30000 で停止。
        history = [_settled(30000, 0, "2026-06-11T10:00:00")]
        d = evaluate_kill_switch(history, self._cfg(), today=day)
        assert d.blocked is True
        assert d.daily_loss == 30000.0
        assert d.limit == 30000.0

    def test_allows_below_limit(self):
        day = dt.date(2026, 6, 11)
        history = [_settled(10000, 0, "2026-06-11T10:00:00")]  # 損失 10000 < 30000
        d = evaluate_kill_switch(history, self._cfg(), today=day)
        assert d.blocked is False

    def test_disabled_never_blocks(self):
        day = dt.date(2026, 6, 11)
        history = [_settled(90000, 0, "2026-06-11T10:00:00")]  # 大損失でも
        d = evaluate_kill_switch(history, self._cfg(kill_switch_enabled=False), today=day)
        assert d.blocked is False


# ---------------------------------------------------------------------------
# effective_bankroll
# ---------------------------------------------------------------------------


class TestEffectiveBankroll:
    def test_adds_realized_pnl(self):
        history = [
            _settled(100, 250, "2026-06-11T10:00:00"),   # +150
            _settled(100, 0, "2026-06-11T11:00:00"),     # -100
        ]
        assert effective_bankroll(history, 100000.0) == 100050.0

    def test_empty_equals_initial(self):
        assert effective_bankroll([], 100000.0) == 100000.0
