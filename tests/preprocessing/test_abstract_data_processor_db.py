"""AbstractDataProcessor の DB 直読み対応（Phase 2）テスト。

filepath / (repo, alias) の両入力経路と、どちらも欠けた場合のエラーを検証する。
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.preprocessing._abstract_data_processor import AbstractDataProcessor


class _PassThroughProcessor(AbstractDataProcessor):
    """raw_data をそのまま preprocessed_data として返す最小実装。"""

    def _preprocess(self):
        return self.raw_data


class _StubRepo:
    """read(alias) で固定 DataFrame を返す duck-typed リポジトリ。"""

    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df
        self.read_calls: list[str] = []

    def read(self, alias: str) -> pd.DataFrame:
        self.read_calls.append(alias)
        return self._df.copy()


def _df() -> pd.DataFrame:
    return pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})


def test_reads_from_filepath(tmp_path):
    path = tmp_path / "d.pkl"
    _df().to_pickle(str(path))

    proc = _PassThroughProcessor(str(path))
    assert len(proc.preprocessed_data) == 2


def test_reads_from_repo_alias():
    repo = _StubRepo(_df())
    proc = _PassThroughProcessor(repo=repo, alias="raw_results")

    assert repo.read_calls == ["raw_results"]
    assert list(proc.preprocessed_data["a"]) == [1, 2]


def test_raises_when_no_source():
    with pytest.raises(ValueError, match="filepath か"):
        _PassThroughProcessor()


def test_raises_when_repo_without_alias():
    repo = _StubRepo(_df())
    with pytest.raises(ValueError, match="filepath か"):
        _PassThroughProcessor(repo=repo, alias=None)
