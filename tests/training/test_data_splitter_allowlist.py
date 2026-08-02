"""DataSplitter の feature_allowlist（続36-f）単体テスト。

新規 JRDB 列が featured に増えても、allowlist 未指定の既存モデルは挙動不変（denylist）で、
allowlist 指定モデルは **その列だけ** を学習入力にする（silent 混入なし・未実体化は fail-closed）。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.training._data_splitter import DataSplitter


def _featured(n=40):
    rs = np.random.RandomState(0)
    idx = [f"2020010101{i % 4:02d}" for i in range(n)]
    return pd.DataFrame({
        "date": [f"2020-01-{1 + (i % 20):02d}" for i in range(n)],
        "rank": [1 if i % 3 == 0 else 0 for i in range(n)],
        "着順": [1 if i % 3 == 0 else 5 for i in range(n)],
        "単勝": np.linspace(2, 20, n),
        "f1": rs.rand(n), "f2": rs.rand(n),
        "jrdb_idm": rs.rand(n),
        "jrdb_ten_idx": rs.rand(n),   # 新規実体化列（allowlist 外なら混入しない）
    }, index=idx)


def test_denylist_default_keeps_all_features():
    d = DataSplitter(_featured(), 0.25, 0.25)
    cols = set(d.X_train.columns)
    assert {"f1", "f2", "jrdb_idm", "jrdb_ten_idx"} <= cols   # 従来どおり新規列も入る


def test_allowlist_restricts_to_configured_features():
    d = DataSplitter(_featured(), 0.25, 0.25, feature_allowlist=["f1", "f2", "jrdb_idm"])
    assert set(d.X_train.columns) == {"f1", "f2", "jrdb_idm"}  # 指定列だけ
    assert "jrdb_ten_idx" not in d.X_train.columns             # 新規列は silent 混入しない
    # X_test は EV 用に 単勝(TANSHO_ODDS) を保持する既存仕様。allowlist＋単勝で、新規列は入らない。
    assert set(d.X_test.columns) == {"f1", "f2", "jrdb_idm", "単勝"}
    assert "jrdb_ten_idx" not in d.X_test.columns


def test_allowlist_missing_feature_fails_closed():
    with pytest.raises(RuntimeError, match="Missing configured features"):
        DataSplitter(_featured(), 0.25, 0.25, feature_allowlist=["f1", "does_not_exist"])
