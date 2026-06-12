"""馬券の実行（運用モード別）。

UI の「実行ボタン」から呼ばれる境界。運用モードにより挙動を切り替えるが、
発注 I/O は recorder（注入）に委譲し、本モジュールはモード判定と整形のみを担う。
安定運用が確認できるまで advisory（履歴記録のみ）。full_auto は既定で無効。
"""

from __future__ import annotations

import datetime as dt
from abc import ABC
from abc import abstractmethod
from typing import Callable
from typing import Optional

from src.operation._config import ADVISORY
from src.operation._config import FULL_AUTO
from src.operation._config import SEMI_AUTO

# 1件の実行記録（dict）を受け取る副作用関数（DB/CSV/ログ等）。
Recorder = Callable[[dict], None]


def _to_record(candidate, status: str, *, now: Optional[Callable[[], dt.datetime]] = None) -> dict:
    # created_at は損失ストップ（日次集計）が記録日を判定するために付与する。
    created_at = (now or dt.datetime.now)().isoformat(timespec="seconds")
    return {
        "race_id": candidate.race_id,
        "bet_type": candidate.bet_type,
        "combo": list(candidate.combo),
        "odds": candidate.odds,
        "probability": candidate.probability,
        "expected_value": candidate.expected_value,
        "confidence": candidate.confidence,
        "stake": candidate.stake,
        "status": status,
        "created_at": created_at,
    }


class AbstractBetExecutor(ABC):
    @abstractmethod
    def execute(self, allocated: list) -> list:
        """配分済み候補を実行し、実行記録のリストを返す。"""
        raise NotImplementedError


class AdvisoryExecutor(AbstractBetExecutor):
    """推奨を履歴に記録するのみ（人間が手動で発注）。"""

    def __init__(self, recorder: Recorder) -> None:
        self._recorder = recorder

    def execute(self, allocated: list) -> list:
        records = [_to_record(c, status="recommended") for c in allocated if c.stake > 0]
        for r in records:
            self._recorder(r)
        return records


class SemiAutoExecutor(AbstractBetExecutor):
    """購入リストを出力する（発注は人間）。"""

    def __init__(self, recorder: Recorder) -> None:
        self._recorder = recorder

    def execute(self, allocated: list) -> list:
        records = [_to_record(c, status="queued") for c in allocated if c.stake > 0]
        for r in records:
            self._recorder(r)
        return records


class AutoExecutor(AbstractBetExecutor):
    """自動発注（将来）。規約・法的リスクのため既定で無効。"""

    def __init__(self, recorder: Recorder, enabled: bool = False) -> None:
        self._recorder = recorder
        self._enabled = enabled

    def execute(self, allocated: list) -> list:
        if not self._enabled:
            raise NotImplementedError(
                "full_auto（自動発注）は既定で無効です。規約・法的リスクを確認の上で有効化してください。"
            )
        records = [_to_record(c, status="placed") for c in allocated if c.stake > 0]
        for r in records:
            self._recorder(r)
        return records


def create_bet_executor(operation_mode: str, recorder: Recorder, enable_auto: bool = False) -> AbstractBetExecutor:
    """運用モードから Executor を生成する単一の入口（DI/Factory）。"""
    if operation_mode == ADVISORY:
        return AdvisoryExecutor(recorder)
    if operation_mode == SEMI_AUTO:
        return SemiAutoExecutor(recorder)
    if operation_mode == FULL_AUTO:
        return AutoExecutor(recorder, enabled=enable_auto)
    raise ValueError(f"未知の operation_mode: {operation_mode}")
