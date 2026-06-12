"""損失ストップ（kill switch）と実効 bankroll の判定（純粋ロジック）。

設計（レイヤ規約）:
- operation 層。app（最上位）には依存できないため、投票履歴 `list[dict]` を
  引数で受け取る依存逆転にする（呼出側 = app/pages が `load_history()` を渡す）。
- I/O・グローバル状態を持たない純粋関数。`today` / 履歴を注入してテスト決定化。

実現損益の定義:
- `app/_order_service.py::settle_records` が清算時に `status="settled"` / `payout`(float)
  / `settled_at`(ISO) を付与する。実現損益 = settled レコードの (payout - stake) 総和。
- 当日損失は `settled_at` の日付で集計する（未清算レコードは損益未確定のため除外）。
"""

from __future__ import annotations

import dataclasses
import datetime as dt
from typing import Optional

from src.operation._config import OperationConfig

_SETTLED = "settled"


def _f(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def realized_pnl(history: list[dict]) -> float:
    """settled レコードの (payout - stake) 総和（実現損益。負なら損失）。"""
    total = 0.0
    for r in history:
        if r.get("status") != _SETTLED:
            continue
        total += _f(r.get("payout")) - _f(r.get("stake"))
    return total


def _record_day(record: dict) -> Optional[dt.date]:
    """レコードの確定日（settled_at の日付）。パース不能なら None。"""
    raw = record.get("settled_at")
    if not raw:
        return None
    try:
        return dt.datetime.fromisoformat(str(raw)).date()
    except ValueError:
        return None


def daily_realized_loss(history: list[dict], day: dt.date) -> float:
    """指定日に確定した実現損失を正の数で返す（利益なら 0）。

    当日の settled レコードの (payout - stake) 合計が負のときその絶対値、
    非負なら 0.0 を返す。
    """
    net = 0.0
    for r in history:
        if r.get("status") != _SETTLED:
            continue
        if _record_day(r) != day:
            continue
        net += _f(r.get("payout")) - _f(r.get("stake"))
    return -net if net < 0 else 0.0


@dataclasses.dataclass(frozen=True)
class GuardDecision:
    blocked: bool
    reason: str
    daily_loss: float
    limit: float


def evaluate_kill_switch(
    history: list[dict],
    config: OperationConfig,
    *,
    today: Optional[dt.date] = None,
) -> GuardDecision:
    """当日実現損失が bankroll × max_daily_loss_ratio を超えたら blocked=True。

    `config.kill_switch_enabled` が False のときは常に blocked=False。
    """
    limit = float(config.bankroll) * float(config.max_daily_loss_ratio)
    day = today or dt.date.today()
    loss = daily_realized_loss(history, day)

    if not config.kill_switch_enabled:
        return GuardDecision(blocked=False, reason="kill switch 無効", daily_loss=loss, limit=limit)

    if loss >= limit:
        return GuardDecision(
            blocked=True,
            reason=f"当日実現損失 ¥{loss:,.0f} が上限 ¥{limit:,.0f} に達しました",
            daily_loss=loss,
            limit=limit,
        )
    return GuardDecision(blocked=False, reason="取引可能", daily_loss=loss, limit=limit)


def effective_bankroll(history: list[dict], initial_bankroll: float) -> float:
    """initial_bankroll + 実現損益（settled のみ反映）。"""
    return float(initial_bankroll) + realized_pnl(history)
