"""人気順ベースライン・シミュレータ（§8）。

ML モデルの有効性は「単純に人気上位馬を買い続けた場合の回収率」との比較で初めて
主張できる（KB shard-19）。本クラスは各レースで人気上位 N 頭（= 単勝オッズが低い順、
または人気列の昇順）を機械的に購入するベースライン戦略の actions を生成し、
既存 Simulator に委譲して回収率・的中率を算出する。

Simulator と同じ決済経路（ReturnProcessor / BettingTickets）を使うため、ML 戦略と
同一条件での比較が保証される。
"""

from __future__ import annotations

import pandas as pd

from src.constants._bet_types import BetType
from src.constants._results_cols import ResultsCols


class PopularityBaselineSimulator:
    """人気上位 N 頭を毎レース購入するベースライン戦略。

    Parameters
    ----------
    simulator : 決済を委譲する Simulator（ReturnProcessor 注入済み）。
    top_n : 各レースで購入する人気上位の頭数。
    bet_type : 購入する馬券種（既定は単勝）。単勝/複勝のみ対応。
    """

    def __init__(self, simulator, top_n: int = 1, bet_type: str = BetType.TANSHO) -> None:
        if bet_type not in (BetType.TANSHO, BetType.FUKUSHO):
            raise ValueError("PopularityBaselineSimulator は単勝/複勝のみ対応です。")
        self._simulator = simulator
        self._top_n = top_n
        self._bet_type = bet_type

    def build_actions(self, score_table: pd.DataFrame) -> dict:
        """人気上位 N 頭を選ぶ actions dict を生成する。

        score_table: race_id を index に持ち、馬番列と
        （人気列 ResultsCols.POPULARITY もしくは単勝オッズ列 ResultsCols.TANSHO_ODDS）を含む。
        人気列があれば昇順（1番人気が上位）、無ければ単勝オッズ昇順で選ぶ。
        """
        if ResultsCols.POPULARITY in score_table.columns:
            sort_col, ascending = ResultsCols.POPULARITY, True
        elif ResultsCols.TANSHO_ODDS in score_table.columns:
            sort_col, ascending = ResultsCols.TANSHO_ODDS, True
        else:
            raise ValueError("人気列または単勝オッズ列が score_table にありません。")

        actions: dict = {}
        for race_id, race_df in score_table.groupby(level=0):
            top = race_df.sort_values(sort_col, ascending=ascending).head(self._top_n)
            umaban_list = list(top[ResultsCols.UMABAN])
            actions[race_id] = {self._bet_type: umaban_list}
        return actions

    def calc_returns(self, score_table: pd.DataFrame) -> dict:
        """ベースライン戦略の成績指標を返す（Simulator.calc_returns に委譲）。"""
        actions = self.build_actions(score_table)
        return self._simulator.calc_returns(actions)
