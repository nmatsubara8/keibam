from abc import ABCMeta
from abc import abstractmethod
from typing import Callable

import pandas as pd

from src.constants._results_cols import ResultsCols

# const
_SCORE = "score"
# 期待値計算で用いる列名（ExpectedValueBetPolicy / OddsProvider と整合）
PROB = "prob"
CURRENT_ODDS = "current_odds"

# predict_proba に渡す前に除外する非特徴量列（目的変数・日付・オッズ）
_DROP_FOR_PREDICT = [ResultsCols.TANSHO_ODDS, "rank", "date", ResultsCols.RANK]


# common funcs
def _calc(model, X: pd.DataFrame) -> pd.DataFrame:
    score_table = X[ResultsCols.UMABAN].to_frame().copy()
    score_table[ResultsCols.WAKUBAN] = X[ResultsCols.WAKUBAN]
    # race_idごとにWAKUBANの組み合わせをカウント
    # race_idごとにUMABANとWAKUBANの個数を取得
    race_key = "race_id" if "race_id" in X.columns else X.index
    umaban_count_per_race = X.groupby(race_key)[ResultsCols.UMABAN].nunique()
    wakuban_count_per_race = X.groupby(race_key)[ResultsCols.WAKUBAN].nunique()

    # UMABANの個数がWAKUBANの個数よりも多い場合にwakuban_flagを設定
    wakuban_flag = (umaban_count_per_race > wakuban_count_per_race).astype(int)
    wakuban_flag.name = "wakuban_flag"
    score = model.predict_proba(X.drop(_DROP_FOR_PREDICT, axis=1, errors="ignore"))[:, 1]
    score_table[_SCORE] = score
    # race_idに対応するwakuban_flagを結合
    score_table = pd.merge(score_table, wakuban_flag, left_on="race_id", right_index=True)

    return score_table


def _apply_scaler(score: pd.Series, scaler: Callable[[pd.Series], pd.Series]) -> pd.Series:
    return score.groupby(level=0, group_keys=False).apply(scaler)


# scalers
def _scaler_standard(x):
    return (x - x.mean()) / x.std(ddof=0)


def _scaler_relative_proba(x):
    return x / x.sum()


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


class ExpectedValueScorePolicy(AbstractScorePolicy):
    """期待値ベース馬券選定（ExpectedValueBetPolicy）向けのテーブルを作る。

    較正済みモデルの勝率と単勝オッズを保持した DataFrame を返す。
    列: [馬番, prob(較正勝率), current_odds(単勝オッズ)]、index は race_id。
    既存のスコア系ポリシーと異なり「スコア」ではなく確率とオッズを供給する。
    """

    @staticmethod
    def calc(model, X: pd.DataFrame) -> pd.DataFrame:
        prob = model.predict_proba(X.drop(_DROP_FOR_PREDICT, axis=1, errors="ignore"))[:, 1]
        table = X[[ResultsCols.UMABAN]].copy()
        table[PROB] = prob
        table[CURRENT_ODDS] = X[ResultsCols.TANSHO_ODDS].astype(float)
        return table
