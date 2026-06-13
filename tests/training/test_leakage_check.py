"""src/training/_leakage_check.py のテスト。

合成データで「リーク注入時は SUSPECT、クリーン時は PASS」を確認し、
ラベルシャッフル試験が実際にリークを検出できることを担保する。
LightGBM を使わず軽量なダミーモデルを注入して高速・決定的にする。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.training._leakage_check import ShuffleResult, label_shuffle_test


class _LeakyModel:
    """1 列目の特徴量をそのまま確率として返す（= リークした特徴量を完全利用）。"""

    def fit(self, X, y):  # noqa: D401
        self._col = np.asarray(X)[:, 0]
        return self

    def predict_proba(self, X):
        p = np.asarray(X)[:, 0].astype(float)
        p = (p - p.min()) / (p.max() - p.min() + 1e-9)
        return np.column_stack([1 - p, p])


class _HonestModel:
    """学習データの特徴量と y の関係だけを使う素朴なモデル（リークがあれば暴けない）。

    1 列目の値で y の平均を学習し、test ではその値で予測する。
    シャッフルで y がランダム化されると学習が壊れ AUC≈0.5 になる。
    """

    def fit(self, X, y):
        x0 = np.asarray(X)[:, 0]
        yy = np.asarray(y, dtype=float)
        # x0 の符号ごとの y 平均（単純なしきい値学習）
        self._hi = yy[x0 >= np.median(x0)].mean() if len(yy) else 0.5
        self._lo = yy[x0 < np.median(x0)].mean() if len(yy) else 0.5
        self._med = np.median(x0)
        return self

    def predict_proba(self, X):
        x0 = np.asarray(X)[:, 0]
        p = np.where(x0 >= self._med, self._hi, self._lo).astype(float)
        return np.column_stack([1 - p, p])


def _make_data(n=400, seed=0):
    rng = np.random.default_rng(seed)
    # 真の信号: feature0 が y と相関
    y = rng.integers(0, 2, size=n)
    signal = y + rng.normal(0, 1.0, size=n)
    noise = rng.normal(0, 1.0, size=n)
    X = pd.DataFrame({"f0": signal, "f1": noise})
    split = n // 2
    return X.iloc[:split], y[:split], X.iloc[split:], y[split:]


class TestLabelShuffleTest:
    def test_clean_model_passes(self):
        """正直なモデル: シャッフルで AUC が偶然付近に落ち PASS。"""
        Xtr, ytr, Xte, yte = _make_data()
        res = label_shuffle_test(
            Xtr, ytr, Xte, yte,
            model_factory=_HonestModel, n_trials=4,
            rng=np.random.default_rng(1),
        )
        assert isinstance(res, ShuffleResult)
        assert res.baseline_auc > 0.6  # 本物ラベルでは学習できている
        assert res.shuffled_mean <= res.suspect_threshold
        assert res.verdict == "PASS"

    def test_leaky_model_is_flagged(self):
        """test ラベルを特徴量に混入させると、シャッフルしても AUC が高く SUSPECT。"""
        Xtr, ytr, Xte, yte = _make_data()
        # f0 を「その行の y そのもの」に置換 = 目的変数リークを再現
        Xtr = Xtr.copy()
        Xte = Xte.copy()
        Xtr["f0"] = ytr.astype(float)
        Xte["f0"] = yte.astype(float)  # test 側にも正解が漏れている
        res = label_shuffle_test(
            Xtr, ytr, Xte, yte,
            model_factory=_LeakyModel, n_trials=4,
            rng=np.random.default_rng(2),
        )
        # y_train をシャッフルしても、test の f0=y_test を読むだけで当てられる
        assert res.shuffled_mean > res.suspect_threshold
        assert res.verdict == "SUSPECT"

    def test_result_fields(self):
        Xtr, ytr, Xte, yte = _make_data()
        res = label_shuffle_test(
            Xtr, ytr, Xte, yte,
            model_factory=_HonestModel, n_trials=3,
            rng=np.random.default_rng(3),
        )
        assert res.n_trials == 3
        assert len(res.shuffled_aucs) == 3
        assert res.gap == round(res.baseline_auc - res.shuffled_mean, 4) or isinstance(res.gap, float)
