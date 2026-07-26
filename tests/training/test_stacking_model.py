"""StackingModel の stream-aware 化（Phase 2）の回帰テスト。

- 後方互換: base_streams 省略 + ndarray 入力で従来通り全 gbdt 扱い。
- stream-aware: nn ストリームが nn_scaler 経由で正しく導出される（列順・codes 変換）。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.preprocessing._nn_feature_scaler import NnFeatureScaler
from src.training._stacking_model import StackingModel


class _StubBase:
    """fit/predict_proba で受け取った X を記録するスタブ base 学習器。"""

    def __init__(self):
        self.fit_X = None
        self.pred_X = None

    def fit(self, x, y, sample_weight=None):
        self.fit_X = np.asarray(x)
        return self

    def predict_proba(self, x):
        self.pred_X = np.asarray(x)
        n = len(self.pred_X)
        p = np.full(n, 0.3)
        return np.column_stack([1 - p, p])


class _StubMeta:
    def fit(self, x, y):
        self.n_features = np.asarray(x).shape[1]
        return self

    def predict_proba(self, x):
        n = len(np.asarray(x))
        p = np.full(n, 0.5)
        return np.column_stack([1 - p, p])


def test_backward_compat_all_gbdt():
    """base_streams 省略 + ndarray 入力で従来挙動（全 gbdt）が維持される。"""
    x_base = np.random.rand(20, 4)
    y_base = np.random.randint(0, 2, 20)
    x_meta = np.random.rand(10, 4)
    y_meta = np.random.randint(0, 2, 10)

    b1, b2 = _StubBase(), _StubBase()
    stk = StackingModel([b1, b2], _StubMeta())
    stk.fit(x_base, y_base, x_meta, y_meta)

    # gbdt は ndarray をそのまま受け取る
    assert b1.fit_X.shape == (20, 4)
    proba = stk.predict_proba(np.random.rand(5, 4))
    assert proba.shape == (5, 2)


def test_stream_aware_nn_derivation():
    """nn ストリームが [entity_codes+1, scaled_numeric] の float 配列として渡る。"""
    df = pd.DataFrame(
        {
            "jockey_id": pd.Categorical(["a", "b", "a", "c"]),
            "x": [10.0, 20.0, 30.0, 40.0],
        }
    )
    scaler = NnFeatureScaler(entity_cols=["jockey_id"], numeric_cols=["x"])
    scaler.fit_transform(df)

    gbdt = _StubBase()
    nn = _StubBase()
    stk = StackingModel(
        [gbdt, nn], _StubMeta(), base_streams=["gbdt", "nn"], nn_scaler=scaler
    )
    y = np.array([0, 1, 0, 1])
    stk.fit(df, y, df, y)

    # nn base は 2 列（entity codes + numeric）を受け取る
    assert nn.fit_X.shape == (4, 2)
    # col0 = cat.codes + 1（a=0→1, b=1→2, a=0→1, c=2→3）
    np.testing.assert_array_equal(nn.fit_X[:, 0], np.array([1, 2, 1, 3], dtype=np.float32))
    # col1 = 標準化された numeric（平均 0 付近）
    assert abs(float(np.mean(nn.fit_X[:, 1]))) < 1e-6


def test_nn_unknown_category_maps_to_zero():
    """学習時に無いカテゴリは predict 時に code -1 → +1 → 0（未知バケット）になる。"""
    train = pd.DataFrame(
        {"jockey_id": pd.Categorical(["a", "b"]), "x": [1.0, 2.0]}
    )
    scaler = NnFeatureScaler(entity_cols=["jockey_id"], numeric_cols=["x"])
    scaler.fit_transform(train)

    nn = _StubBase()
    stk = StackingModel([nn], _StubMeta(), base_streams=["nn"], nn_scaler=scaler)
    stk.fit(train, np.array([0, 1]), train, np.array([0, 1]))

    # 学習カテゴリ集合（a,b）に無い値（NaN→code -1）を含む推論
    test = pd.DataFrame(
        {"jockey_id": pd.Categorical([np.nan], categories=["a", "b"]), "x": [3.0]}
    )
    stk.predict_proba(test)
    assert nn.pred_X[0, 0] == 0.0  # 未知 → 0
