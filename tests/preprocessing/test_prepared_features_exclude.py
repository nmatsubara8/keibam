"""prepared_from_gbdt の exclude_entities（NN 埋め込みからの高カーディナリティ ID 除外）。

horse_id（17万頭・中央7走で識別子の丸暗記になり test 期へ転移しない）を NN ストリームから
外すための穴を検証する。既定（None）はスタックルート互換で全 NN_ENTITY_COLS を使う。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.preprocessing._prepared_features import prepared_from_gbdt


def _sample():
    return pd.DataFrame(
        {
            "horse_id": pd.Categorical(["h1", "h2", "h3", "h1"]),
            "jockey_id": pd.Categorical(["j1", "j2", "j1", "j2"]),
            "feat1": np.arange(4, dtype="float64"),
            "rank": [1, 2, 3, 1],
        },
        index=["r1", "r1", "r2", "r2"],
    )


def test_exclude_none_keeps_all_entities():
    p = prepared_from_gbdt(_sample())
    assert "horse_id" in p.nn.columns and "jockey_id" in p.nn.columns


def test_exclude_horse_id_drops_only_that_entity():
    p = prepared_from_gbdt(_sample(), exclude_entities=["horse_id"])
    assert "horse_id" not in p.nn.columns          # 除外された
    assert "jockey_id" in p.nn.columns             # 他エンティティは残る
    assert "feat1" in p.nn.columns                 # 数値特徴は残る
    # gbdt ストリームは無傷（除外は NN 側だけ）
    assert "horse_id" in p.gbdt.columns


def test_exclude_multiple():
    p = prepared_from_gbdt(_sample(), exclude_entities=["horse_id", "jockey_id"])
    assert "horse_id" not in p.nn.columns and "jockey_id" not in p.nn.columns
    assert "feat1" in p.nn.columns
