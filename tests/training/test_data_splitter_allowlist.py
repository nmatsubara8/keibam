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
        # legacy 5列（denylist で許容される既存 schema）
        "jrdb_idm": rs.rand(n), "jrdb_joho_idx": rs.rand(n), "jrdb_kijun_odds": rs.rand(n),
        "jrdb_kishu_idx": rs.rand(n), "jrdb_kyakushitsu": rs.rand(n),
        "jrdb_ten_idx": rs.rand(n),   # augment 専用列（allowlist 外なら混入しない）
    }, index=idx)


def _legacy_featured(n=40):
    # legacy schema（既存5 JRDB のみ・augment 専用列なし）。従来経路が通ることの確認用。
    df = _featured(n)
    return df.drop(columns=["jrdb_ten_idx"])   # augment 専用列を除く（jrdb_idm は legacy）


def test_legacy_featured_denylist_ok():
    # legacy featured＋allowlist なし → 従来どおり成功（augment 専用列が無いので拒否されない）。
    d = DataSplitter(_legacy_featured(), 0.25, 0.25)
    assert {"f1", "f2", "jrdb_idm"} <= set(d.X_train.columns)


def test_augment_featured_without_allowlist_fails_closed():
    # augment featured（jrdb_ten_idx 等）＋allowlist なし → fail-closed（silent 混入拒否）。
    with pytest.raises(RuntimeError, match="Refusing denylist-based training"):
        DataSplitter(_featured(), 0.25, 0.25)


def test_b_five_columns_resolved_exactly_five():
    # B の固定5列 allowlist＋augment featured → 解決後の入力列が厳密に5列。
    d = DataSplitter(_featured(), 0.25, 0.25,
                     feature_allowlist=["jrdb_idm", "jrdb_joho_idx", "jrdb_kijun_odds",
                                        "jrdb_kishu_idx", "jrdb_kyakushitsu"])
    assert len(d.resolved_feature_columns) == 5
    assert set(d.resolved_feature_columns) == {
        "jrdb_idm", "jrdb_joho_idx", "jrdb_kijun_odds", "jrdb_kishu_idx", "jrdb_kyakushitsu"}


def test_resolved_feature_hash_stable_and_order_sensitive():
    d = DataSplitter(_featured(), 0.25, 0.25, feature_allowlist=["f1", "f2", "jrdb_idm"])
    h = d.resolved_feature_hash
    assert isinstance(h, str) and len(h) == 16
    d2 = DataSplitter(_featured(), 0.25, 0.25, feature_allowlist=["f1", "f2", "jrdb_idm"])
    assert d2.resolved_feature_hash == h        # 同じ解決列 → 同じ hash（artifact 照合用）


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
