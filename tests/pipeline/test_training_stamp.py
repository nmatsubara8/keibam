"""学習側 meta への ⑤ 合流（_training_stamp）のテスト。"""
from __future__ import annotations

import pandas as pd

from src.pipeline._eval_stamp import feature_schema_hash
from src.pipeline._retrain import _training_stamp


class _StubContract:
    def __init__(self, names) -> None:
        self.names = tuple(names)


class _StubAI:
    def __init__(self, names=None) -> None:
        self.feature_contract_ = _StubContract(names) if names else None


def test_training_stamp_with_contract_and_dates():
    ai = _StubAI(["a", "b", "c"])
    featured = pd.DataFrame(
        {"a": [1, 2], "b": [2, 3], "c": [3, 4], "date": ["2015-01-01", "2024-12-31"]})
    s = _training_stamp(ai, featured)
    assert s["feature_schema_hash"] == feature_schema_hash(["a", "b", "c"])
    assert s["n_features"] == 3
    assert s["seed"] == 100                        # base 学習器の固定 seed
    assert s["odds_included"] is False             # 本番は市場オッズを学習除外
    assert s["split_method"].startswith("temporal")
    assert s["data_period"] == ("2015-01-01", "2024-12-31")


def test_training_stamp_without_contract_or_date():
    s = _training_stamp(_StubAI(None), pd.DataFrame({"a": [1]}))
    assert "feature_schema_hash" not in s and "data_period" not in s
    assert s["seed"] == 100 and s["odds_included"] is False and "split_method" in s
