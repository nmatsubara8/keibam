"""① stacking split が single-class(degenerate)のとき非stacking へ自動 fallback するテスト。"""
from __future__ import annotations

from src.pipeline._retrain import _train_with_optional_stacking
from src.training._data_splitter import StackingSplitDegenerateError


class _StubAI:
    """train_* の呼び出し順を記録するスタブ（重い学習を回さずに分岐を検証）。"""

    def __init__(self, raise_degenerate: bool = False) -> None:
        self._raise = raise_degenerate
        self.calls: list[str] = []

    def train_with_stacking(self, **kw) -> None:
        self.calls.append("stacking")
        if self._raise:
            raise StackingSplitDegenerateError("meta single-class")

    def train_with_tuning(self, tuning_config=None) -> None:
        self.calls.append("tuning")

    def train_without_tuning(self) -> None:
        self.calls.append("no_tuning")


def test_degenerate_error_is_valueerror_subclass():
    # 既存の except ValueError も従来どおり捕捉できる（後方互換）
    assert issubclass(StackingSplitDegenerateError, ValueError)


def test_uses_stacking_when_ok():
    ai = _StubAI(raise_degenerate=False)
    used = _train_with_optional_stacking(ai, use_stacking=True, meta_ratio=0.3, with_tuning=False)
    assert used is True
    assert ai.calls == ["stacking"]


def test_falls_back_to_non_stacking_on_degenerate():
    ai = _StubAI(raise_degenerate=True)
    used = _train_with_optional_stacking(
        ai, use_stacking=True, meta_ratio=0.3, with_tuning=True, tuning_config="cfg")
    assert used is False
    assert ai.calls == ["stacking", "tuning"]   # stacking を試み→degenerate→非stacking へ


def test_falls_back_to_without_tuning_when_no_tuning():
    ai = _StubAI(raise_degenerate=True)
    used = _train_with_optional_stacking(ai, use_stacking=True, meta_ratio=0.3, with_tuning=False)
    assert used is False
    assert ai.calls == ["stacking", "no_tuning"]


def test_non_stacking_path_when_disabled():
    ai = _StubAI()
    used = _train_with_optional_stacking(ai, use_stacking=False, meta_ratio=0.3, with_tuning=False)
    assert used is False
    assert ai.calls == ["no_tuning"]           # stacking を一切試みない
