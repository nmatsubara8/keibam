"""通知アダプタ（Slack Webhook / Null）。

cron/CLI ジョブの成否や予測サマリを外部へ通知するための差し替え可能な抽象。
`_odds_source.py`(AbstractOddsSource) / `create_bet_executor` と同じ DI 流儀。

重要: 稼働アプリ/cron からは Slack MCP を使えないため、Slack 通知は **Incoming
Webhook（requests.post）** で行う。環境変数 `NOTIFY_SLACK_WEBHOOK` は既存
`scripts/on_failure_notify.sh` の規約に合わせる。

レイヤ: operation。`requests` は遅延 import（CI に存在するが import 軽量化のため）。
"""

from __future__ import annotations

import logging
import os
from abc import ABC
from abc import abstractmethod
from typing import Callable
from typing import Optional

logger = logging.getLogger(__name__)

# requests.post 互換のシグネチャ（テストで差し替え可能にする）。
PostFn = Callable[..., object]


class AbstractNotifier(ABC):
    @abstractmethod
    def notify(self, subject: str, body: str, *, level: str = "info") -> bool:
        """通知を送る。成功で True、失敗/無効で False を返す（例外は投げない）。"""
        raise NotImplementedError


class NullNotifier(AbstractNotifier):
    """通知先未設定時のノーオペ（常に False）。"""

    def notify(self, subject: str, body: str, *, level: str = "info") -> bool:
        return False


def _default_post(url: str, *, json: dict, timeout: float):  # noqa: ANN201
    import requests  # 遅延 import

    return requests.post(url, json=json, timeout=timeout)


class SlackWebhookNotifier(AbstractNotifier):
    """Slack Incoming Webhook へ POST する Notifier。"""

    _ICON = {"info": "ℹ️", "warn": "⚠️", "error": "❌", "success": "✅"}

    def __init__(self, webhook_url: str, *, post: Optional[PostFn] = None, timeout: float = 5.0) -> None:
        self._url = webhook_url
        self._post = post or _default_post
        self._timeout = timeout

    def notify(self, subject: str, body: str, *, level: str = "info") -> bool:
        icon = self._ICON.get(level, "")
        payload = {"text": f"{icon} *{subject}*\n{body}".strip()}
        try:
            self._post(self._url, json=payload, timeout=self._timeout)
            return True
        except Exception as e:  # noqa: BLE001
            logger.warning("[notifier] Slack 通知失敗 (non-fatal): %s", e)
            return False


def create_notifier(webhook_url: Optional[str] = None, *, post: Optional[PostFn] = None) -> AbstractNotifier:
    """通知先を解決して Notifier を生成する。

    webhook_url 省略時は環境変数 `NOTIFY_SLACK_WEBHOOK` を使う。未設定なら NullNotifier。
    """
    url = webhook_url or os.environ.get("NOTIFY_SLACK_WEBHOOK")
    if url:
        return SlackWebhookNotifier(url, post=post)
    return NullNotifier()
