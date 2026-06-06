from abc import ABCMeta
from abc import abstractstaticmethod
from itertools import combinations
from itertools import permutations

import pandas as pd

from src.constants._bet_thresholds import RiskLimits
from src.constants._bet_types import COMBO_SIZE
from src.constants._bet_types import ORDERED
from src.constants._results_cols import ResultsCols
from src.policies import _harville as harville
from src.policies._bet_candidate import BetCandidate
from src.policies._odds_provider import AbstractOddsProvider

# 較正勝率を格納する score_table の列名
PROB = "prob"


class AbstractBetPolicy(metaclass=ABCMeta):
    """
    クラスの型を決めるための抽象クラス。
    """

    @abstractstaticmethod
    def judge(score_table, **params):
        """
        bet_dictは{race_id: {馬券の種類: 馬番のリスト}}の形式で返す。

        例)
        {'202101010101': {'tansho': [6, 8], 'fukusho': [4, 5]},
        '202101010102': {'tansho': [1], 'fukusho': [4]},
        '202101010103': {'tansho': [6], 'fukusho': []},
        '202101010104': {'tansho': [5], 'fukusho': [11]},
        ...}
        """
        pass


class BetPolicyTansho:
    """
    thresholdを超えた馬に単勝で賭ける戦略。
    """

    @staticmethod
    def judge(score_table: pd.DataFrame, threshold: float) -> dict:
        filtered_table = score_table[score_table["score"] >= threshold]
        bet_df = filtered_table.groupby(level=0)[ResultsCols.UMABAN].apply(list).to_frame()
        bet_dict = bet_df.rename(columns={ResultsCols.UMABAN: "tansho"}).T.to_dict()
        # print(f"bet_dict:{bet_dict}")
        return bet_dict


class BetPolicyFukusho:
    """
    thresholdを超えた馬に複勝で賭ける戦略。
    """

    @staticmethod
    def judge(score_table: pd.DataFrame, threshold: float) -> dict:
        filtered_table = score_table[score_table["score"] >= threshold]
        bet_df = filtered_table.groupby(level=0)[ResultsCols.UMABAN].apply(list).to_frame()
        bet_dict = bet_df.rename(columns={ResultsCols.UMABAN: "fukusho"}).T.to_dict()
        return bet_dict


class BetPolicyWakurenBox:
    """
    thresholdを超えた馬の枠に複勝で賭ける戦略。
    """

    @staticmethod
    def judge(score_table: pd.DataFrame, threshold: float) -> dict:
        filtered_table = score_table[(score_table["score"] >= threshold) & (score_table["wakuban_flag"] == 1)]
        # filtered_table = score_table[score_table["score"] >= threshold]
        bet_df = filtered_table.groupby(level=0)[ResultsCols.WAKUBAN].apply(list).to_frame()
        bet_df = bet_df[bet_df[ResultsCols.WAKUBAN].apply(len) >= 2]
        bet_dict = bet_df.rename(columns={ResultsCols.WAKUBAN: "wakuren"}).T.to_dict()
        return bet_dict


class BetPolicyUmarenBox:
    """
    thresholdを超えた馬に馬連BOXで賭ける戦略。
    """

    def judge(score_table: pd.DataFrame, threshold: float) -> dict:
        filtered_table = score_table[score_table["score"] >= threshold]
        bet_df = filtered_table.groupby(level=0)[ResultsCols.UMABAN].apply(list).to_frame()
        bet_df = bet_df[bet_df[ResultsCols.UMABAN].apply(len) >= 2]
        bet_dict = bet_df.rename(columns={ResultsCols.UMABAN: "umaren"}).T.to_dict()
        return bet_dict


class BetPolicyUmatanBox:
    """
    thresholdを超えた馬に馬単BOXで賭ける戦略。
    """

    def judge(score_table: pd.DataFrame, threshold: float) -> dict:
        filtered_table = score_table[score_table["score"] >= threshold]
        bet_df = filtered_table.groupby(level=0)[ResultsCols.UMABAN].apply(list).to_frame()
        bet_df = bet_df[bet_df[ResultsCols.UMABAN].apply(len) >= 2]
        bet_dict = bet_df.rename(columns={ResultsCols.UMABAN: "umatan"}).T.to_dict()
        return bet_dict


class BetPolicyWideBox:
    """
    thresholdを超えた馬にワイドBOXで賭ける戦略。
    """

    def judge(score_table: pd.DataFrame, threshold: float) -> dict:
        filtered_table = score_table[score_table["score"] >= threshold]
        bet_df = filtered_table.groupby(level=0)[ResultsCols.UMABAN].apply(list).to_frame()
        bet_df = bet_df[bet_df[ResultsCols.UMABAN].apply(len) >= 2]
        bet_dict = bet_df.rename(columns={ResultsCols.UMABAN: "wide"}).T.to_dict()
        return bet_dict


class BetPolicySanrenpukuBox:
    """
    thresholdを超えた馬に三連複BOXで賭ける戦略。
    """

    def judge(score_table: pd.DataFrame, threshold: float) -> dict:
        filtered_table = score_table[score_table["score"] >= threshold]
        bet_df = filtered_table.groupby(level=0)[ResultsCols.UMABAN].apply(list).to_frame()
        bet_df = bet_df[bet_df[ResultsCols.UMABAN].apply(len) >= 3]
        bet_dict = bet_df.rename(columns={ResultsCols.UMABAN: "sanrenpuku"}).T.to_dict()
        return bet_dict


class BetPolicySanrentanBox:
    """
    thresholdを超えた馬に三連単BOXで賭ける戦略。
    """

    def judge(score_table: pd.DataFrame, threshold: float) -> dict:
        filtered_table = score_table[score_table["score"] >= threshold]
        bet_df = filtered_table.groupby(level=0)[ResultsCols.UMABAN].apply(list).to_frame()
        bet_df = bet_df[bet_df[ResultsCols.UMABAN].apply(len) >= 3]
        bet_dict = bet_df.rename(columns={ResultsCols.UMABAN: "sanrentan"}).T.to_dict()
        return bet_dict


class BetPolicyUmatanNagashi:
    """
    threshold1を超えた馬を軸にし、threshold2を超えた馬を相手にして馬単で賭ける。（未実装）
    """

    def judge(score_table: pd.DataFrame, threshold1: float, threshold2: float) -> dict:
        bet_dict = {}
        filtered_table = score_table.query("score >= @threshold2")
        filtered_table["flg"] = filtered_table["score"].map(lambda x: "jiku" if x >= threshold1 else "aite")
        for race_id, table in filtered_table.groupby(level=0):
            bet_dict_1R = {}
            bet_dict_1R["tansho"] = list(table.query('flg == "tansho"')[ResultsCols.UMABAN])
            bet_dict_1R["fukusho"] = list(table.query('flg == "fukusho"')[ResultsCols.UMABAN])
            bet_dict[race_id] = bet_dict_1R
        return bet_dict


class ExpectedValueBetPolicy:
    """期待値（=的中確率×オッズ）ベースで全馬券種を選定する戦略。

    既存の BetPolicy* を壊さず追加する新しいポリシー。較正勝率と注入された
    オッズ供給（AbstractOddsProvider）から、レースごとに期待値が閾値を超える
    組合せを抽出する。リスク管理（1レース上限枚数・低確率足切り）を内包する。

    Parameters
    ----------
    odds_provider : オッズ供給（依存性注入。過去推定／ライブ実オッズを差し替え可能）。
    thresholds : {馬券種: 期待値閾値}。
    bet_types : 対象とする馬券種（省略時は thresholds のキー全て）。
    risk_limits : リスク管理パラメータ。
    """

    def __init__(
        self,
        odds_provider: AbstractOddsProvider,
        thresholds: dict,
        bet_types=None,
        risk_limits: RiskLimits = RiskLimits(),
    ) -> None:
        self._odds_provider = odds_provider
        self._thresholds = thresholds
        self._bet_types = list(bet_types) if bet_types is not None else list(thresholds.keys())
        self._risk = risk_limits

    def select(self, prob_table: pd.DataFrame) -> list:
        """較正勝率テーブルから BetCandidate のリストを返す。

        prob_table: race_id を index に持ち、列 [ResultsCols.UMABAN, "prob"] を含む DataFrame。
        """
        candidates = []
        for race_id, race_df in prob_table.groupby(level=0):
            candidates.extend(self._select_for_race(race_id, race_df))
        return candidates

    def _select_for_race(self, race_id, race_df: pd.DataFrame) -> list:
        win_probs = dict(zip(race_df[ResultsCols.UMABAN], race_df[PROB]))
        # 低確率帯のノイズを足切り（KB 7.3）
        eligible = [u for u, p in win_probs.items() if p >= self._risk.MIN_WIN_PROB]

        race_candidates = []
        for bet_type in self._bet_types:
            size = COMBO_SIZE[bet_type]
            if len(eligible) < size:
                continue
            generator = permutations if bet_type in ORDERED else combinations
            threshold = self._thresholds[bet_type]
            for combo in generator(eligible, size):
                prob = harville.combo_probability(bet_type, win_probs, combo)
                odds = self._odds_provider.get_odds(race_id, bet_type, combo)
                ev = prob * odds
                if ev > threshold:
                    race_candidates.append(
                        BetCandidate(
                            race_id=race_id,
                            bet_type=bet_type,
                            combo=tuple(combo),
                            probability=prob,
                            odds=odds,
                            expected_value=ev,
                        )
                    )
        # リスク管理: 1レースの投票枚数を上限未満に。期待値上位を採用（KB 7.3）。
        race_candidates.sort(key=lambda c: c.expected_value, reverse=True)
        return race_candidates[: self._risk.MAX_TICKETS_PER_RACE]
