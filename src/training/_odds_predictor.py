"""Layer2: 締切確定オッズの予測。

段階スナップショット（前日/数時間前/30分前/直前）から締切時の確定オッズを予測する。
過去の途中オッズ系列は遡及取得できないため、既定は恒等（現在=確定）とし、スナップショットが
蓄積したら LightGBM 実装へ差し替える（AbstractOddsPredictor で疎結合）。

段階拡張: ①LightGBM回帰+レース内正規化 →（将来）②分位点 →③時系列DL。
"""

from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from typing import Any

import numpy as np
import pandas as pd


class AbstractOddsPredictor(ABC):
    """スナップショット特徴量から確定単勝オッズ（馬ごと）を予測する契約。"""

    @abstractmethod
    def predict(self, features: pd.DataFrame) -> pd.Series:
        """race_id を index に持つ features を受け、予測確定オッズの Series を返す。"""
        raise NotImplementedError


class IdentityOddsPredictor(AbstractOddsPredictor):
    """現在オッズをそのまま確定オッズとみなす（Phase A 既定／履歴は確定単勝）。"""

    def __init__(self, current_odds_col: str = "current_odds") -> None:
        self._current_col = current_odds_col

    def predict(self, features: pd.DataFrame) -> pd.Series:
        return features[self._current_col].astype(float)


def _normalize_within_race(pred_odds: pd.Series, current_odds: pd.Series) -> pd.Series:
    """レース内の implied-prob 総和（オーバーラウンド）を現在オッズと一致させる。

    馬間結合（パリミュチュエルのゼロサム性）を尊重するための後処理。
    """
    df = pd.DataFrame({"pred": pred_odds, "cur": current_odds})
    normalized = df["pred"].copy()
    for _, idx in df.groupby(level=0).groups.items():
        block = df.loc[idx]
        target = (1.0 / block["cur"]).sum()
        pred_implied = (1.0 / block["pred"]).sum()
        if pred_implied > 0 and target > 0:
            k = target / pred_implied
            normalized.loc[idx] = block["pred"] / k
    return normalized


class LgbOddsPredictor(AbstractOddsPredictor):
    """LightGBM 回帰（① 目標 log(確定/現在)）+ レース内正規化。

    Parameters
    ----------
    feature_cols : 学習・推論に使う特徴量列。
    current_odds_col : 現在オッズ列（推論の基準）。
    """

    def __init__(self, feature_cols: list, current_odds_col: str = "current_odds", **lgb_params) -> None:
        self._feature_cols = list(feature_cols)
        self._current_col = current_odds_col
        self._lgb_params = lgb_params
        self._model: Any = None

    def fit(self, features: pd.DataFrame, final_odds) -> "LgbOddsPredictor":
        import lightgbm as lgb

        current = features[self._current_col].astype(float).to_numpy()
        target = np.log(np.asarray(final_odds, dtype=float) / current)
        self._model = lgb.LGBMRegressor(**self._lgb_params)
        self._model.fit(features[self._feature_cols].to_numpy(), target)
        return self

    def predict(self, features: pd.DataFrame) -> pd.Series:
        if self._model is None:
            raise RuntimeError("fit を先に呼んでください。")
        current = features[self._current_col].astype(float)
        yhat = self._model.predict(features[self._feature_cols].to_numpy())
        pred = current * np.exp(yhat)
        pred = pd.Series(pred.to_numpy() if hasattr(pred, "to_numpy") else pred, index=features.index)
        return _normalize_within_race(pred, current)
