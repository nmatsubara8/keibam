"""src/operation/_notifier.py: 通知アダプタのテスト（requests を注入差し替え）。"""

from __future__ import annotations

import pytest

from src.operation._notifier import (
    NullNotifier,
    SlackWebhookNotifier,
    create_notifier,
)


class _FakePost:
    def __init__(self, raise_exc: bool = False):
        self.calls: list[dict] = []
        self._raise = raise_exc

    def __call__(self, url, *, json, timeout):
        if self._raise:
            raise RuntimeError("network down")
        self.calls.append({"url": url, "json": json, "timeout": timeout})
        return object()


class TestSlackWebhookNotifier:
    def test_posts_payload(self):
        post = _FakePost()
        n = SlackWebhookNotifier("https://hook", post=post)
        ok = n.notify("件名", "本文", level="error")
        assert ok is True
        assert post.calls[0]["url"] == "https://hook"
        text = post.calls[0]["json"]["text"]
        assert "件名" in text and "本文" in text

    def test_returns_false_on_exception(self):
        n = SlackWebhookNotifier("https://hook", post=_FakePost(raise_exc=True))
        assert n.notify("s", "b") is False


class TestNullNotifier:
    def test_always_false(self):
        assert NullNotifier().notify("s", "b") is False


class TestCreateNotifier:
    def test_null_when_no_url(self, monkeypatch):
        monkeypatch.delenv("NOTIFY_SLACK_WEBHOOK", raising=False)
        assert isinstance(create_notifier(), NullNotifier)

    def test_slack_when_env_set(self, monkeypatch):
        monkeypatch.setenv("NOTIFY_SLACK_WEBHOOK", "https://hook")
        n = create_notifier(post=_FakePost())
        assert isinstance(n, SlackWebhookNotifier)

    def test_explicit_url_overrides_env(self, monkeypatch):
        monkeypatch.delenv("NOTIFY_SLACK_WEBHOOK", raising=False)
        n = create_notifier("https://explicit", post=_FakePost())
        assert isinstance(n, SlackWebhookNotifier)
