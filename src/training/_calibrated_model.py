"""確率較正（Isotonic Regression）モデルラッパー。

任意の予測器（predict_proba を持つ）の出力を、独立した較正用ホールドアウトで
Isotonic 回帰により較正する。`predict_proba` の呼び出し規約を保つため、既存の
ScorePolicy（model.predict_proba(X)[:, 1]）から無改修で利用できる。

base モデルは DI で受け取り、本クラスは較正ロジックのみを担う（疎結合・単一責務）。
optuna/torch 等の重い依存は持たない。
"""

from __future__ import annotations

import numpy as np
from sklearn.isotonic import IsotonicRegression


class CalibratedModel:
    """base モデル + Isotonic 較正器。"""

    def __init__(self, base_model, calibrator: IsotonicRegression) -> None:
        self._base_model = base_model
        self._calibrator = calibrator

    @classmethod
    def fit(cls, base_model, x_calib, y_calib) -> "CalibratedModel":
        """学習済み base_model を、較正用データで Isotonic 較正して返す。

        base_model は呼び出し前に学習済みであること（test へのリークを避けるため、
        較正データは base 学習に使っていないホールドアウトを渡す）。
        """
        raw = np.asarray(base_model.predict_proba(x_calib))[:, 1]
        calibrator = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
        calibrator.fit(raw, np.asarray(y_calib))
        return cls(base_model, calibrator)

    def predict_proba(self, x) -> np.ndarray:
        raw = np.asarray(self._base_model.predict_proba(x))[:, 1]
        calibrated = self._calibrator.predict(raw)
        calibrated = np.clip(calibrated, 0.0, 1.0)
        return np.column_stack([1.0 - calibrated, calibrated])

    @property
    def base_model(self):
        return self._base_model
