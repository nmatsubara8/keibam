from abc import ABCMeta
from abc import abstractmethod
from typing import Callable

import pandas as pd

from src.constants._results_cols import ResultsCols

# const
_SCORE = "score"


# common funcs
def _calc(model, X: pd.DataFrame) -> pd.DataFrame:
    score_table = X[ResultsCols.UMABAN].to_frame().copy()
    score_table[ResultsCols.WAKUBAN] = X[ResultsCols.WAKUBAN]
    # race_idごとにWAKUBANの組み合わせをカウント
    # race_idごとにUMABANとWAKUBANの個数を取得
    umaban_count_per_race = X.groupby("race_id")[ResultsCols.UMABAN].nunique()
    wakuban_count_per_race = X.groupby("race_id")[ResultsCols.WAKUBAN].nunique()

    # UMABANの個数がWAKUBANの個数よりも多い場合にwakuban_flagを設定
    wakuban_flag = (umaban_count_per_race > wakuban_count_per_race).astype(int)
    wakuban_flag.name = "wakuban_flag"
    score = model.predict_proba(X.drop([ResultsCols.TANSHO_ODDS], axis=1))[:, 1]
    score_table[_SCORE] = score
    # race_idに対応するwakuban_flagを結合
    score_table = pd.merge(score_table, wakuban_flag, left_on="race_id", right_index=True)

    return score_table


def _apply_scaler(score: pd.Series, scaler: Callable[[pd.Series], pd.Series]) -> pd.Series:
    return score.groupby(level=0, group_keys=False).apply(scaler)


# scalers
_scaler_standard = lambda x: (x - x.mean()) / x.std(ddof=0)
_scaler_relative_proba = lambda x: x / x.sum()


# policies
class AbstractScorePolicy(metaclass=ABCMeta):
    @staticmethod
    @abstractmethod
    def calc(model, X: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError


class BasicScorePolicy(AbstractScorePolicy):
    """
    LightGBMの出力をそのままscoreとして計算。
    """

    @staticmethod
    def calc(model, X: pd.DataFrame) -> pd.DataFrame:
        return _calc(model, X)


class StdScorePolicy(AbstractScorePolicy):
    """
    レース内で標準化して、相対評価する。「レース内偏差値」のようなもの。
    """

    @staticmethod
    def calc(model, X: pd.DataFrame) -> pd.DataFrame:
        score_table = _calc(model, X)
        # レース内でスコアを標準化
        score_table[_SCORE] = _apply_scaler(score_table[_SCORE], _scaler_standard)
        return score_table


class MinMaxScorePolicy(AbstractScorePolicy):
    """
    レース内で標準化して、相対評価した後、全体を0~1にスケーリング。
    """

    @staticmethod
    def calc(model, X: pd.DataFrame) -> pd.DataFrame:
        score_table = _calc(model, X)
        # レース内でスコアを標準化
        score = _apply_scaler(score_table[_SCORE], _scaler_standard)
        # データ全体で0~1にスケーリング
        min_ = score.min()
        score_table[_SCORE] = (score - min_) / (score.max() - min_)
        return score_table


class RelativeProbaScorePolicy(AbstractScorePolicy):
    """
    レース内での相対確率。
    """

    @staticmethod
    def calc(model, X: pd.DataFrame) -> pd.DataFrame:
        score_table = _calc(model, X)
        # レース内でスコアを相対確率化
        score_table[_SCORE] = _apply_scaler(score_table[_SCORE], _scaler_relative_proba)
        return score_table
