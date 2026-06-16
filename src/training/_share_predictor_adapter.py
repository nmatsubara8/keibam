"""シェア力学モデル → AbstractOddsPredictor 互換アダプタ。

シェア予測（Σ=1）を `odds = (1 − 控除率) / share` で単勝オッズへ変換し、
既存の EV 計算・評価系（OddsProvider / モデルラボのバックテスト）が
無改修で消費できる形にする。
"""

from __future__ import annotations

import pandas as pd

from src.constants._odds_dynamics import TAKEOUT_RATE
from src.training._odds_dynamics import AbstractShareDynamicsModel
from src.training._odds_dynamics import HORIZON_FINAL
from src.training._odds_predictor import AbstractOddsPredictor


def shares_to_odds(shares: pd.Series, takeout: float = TAKEOUT_RATE) -> pd.Series:
    """シェア → 単勝オッズ（パリミュチュエル: (1−控除率)/share）。"""
    return (1.0 - takeout) / shares.clip(lower=1e-6)


class SharePredictorAdapter(AbstractOddsPredictor):
    """1 レース分の観測（{phase: シェア Series}）の供給関数を DI して使うアダプタ。

    Parameters
    ----------
    model : 学習済みのシェア力学モデル。
    obs_lookup : race_id → {phase: シェア Series} を返す関数
        （ウォッチャー/評価側がスナップショットから構築して渡す）。
    takeout : 単勝の控除率（既定 0.2 — policies/_odds_provider.py と整合）。
    """

    def __init__(self, model: AbstractShareDynamicsModel, obs_lookup, takeout: float = TAKEOUT_RATE) -> None:
        self._model = model
        self._obs_lookup = obs_lookup
        self._takeout = takeout

    def predict(self, features: pd.DataFrame) -> pd.Series:
        """race_id index の features を受け、予測確定オッズの Series を返す。

        観測が無いレースは features['current_odds'] をそのまま返す（恒等フォールバック）。
        """
        out = features["current_odds"].astype(float).copy()
        for race_id in features.index.unique():
            obs = self._obs_lookup(str(race_id))
            if not obs:
                continue
            shares = self._model.predict_shares(obs, HORIZON_FINAL)
            if shares.empty:
                continue
            odds = shares_to_odds(shares, self._takeout)
            # features の行順に対応付けられないため、race 単位で umaban →
            # オッズの平均的整合のみ保証する（呼び出し側が umaban 列を持つ場合は
            # ウォッチャーの予測テーブル経由の利用を推奨）。
            n = (features.index == race_id).sum()
            if n == len(odds):
                out.loc[features.index == race_id] = odds.to_numpy()
        return out
