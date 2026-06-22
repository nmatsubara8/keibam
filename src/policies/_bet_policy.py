import math
from abc import ABCMeta
from abc import abstractmethod
from itertools import combinations
from itertools import permutations

import pandas as pd

from src.constants._bet_thresholds import RiskLimits
from src.constants._bet_types import COMBO_SIZE
from src.constants._bet_types import ORDERED
from src.constants._bet_types import BetType
from src.constants._results_cols import ResultsCols
from src.policies import _bet_type_params as bet_type_params
from src.policies import _harville as harville
from src.policies._bet_candidate import BetCandidate
from src.policies._odds_provider import AbstractOddsProvider

# 較正勝率を格納する score_table の列名
PROB = "prob"


class AbstractBetPolicy(metaclass=ABCMeta):
    """
    クラスの型を決めるための抽象クラス。
    """

    @staticmethod
    @abstractmethod
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


def _threshold_umaban_judge(score_table: pd.DataFrame, threshold: float, key: str, min_horses: int = 1) -> dict:
    """score >= threshold の馬を馬番リスト化し {race_id: {key: [馬番...]}} を返す共通処理。

    min_horses >= 2 のとき、頭数が min_horses 未満のレースを除外する（BOX 馬券で
    組合せが成立しないレースを落とす）。min_horses <= 1 のときはフィルタしない
    （単勝・複勝。各グループは必ず 1 頭以上のため挙動は従来と同一）。
    """
    filtered_table = score_table[score_table["score"] >= threshold]
    bet_df = filtered_table.groupby(level=0)[ResultsCols.UMABAN].apply(list).to_frame()
    if min_horses > 1:
        bet_df = bet_df[bet_df[ResultsCols.UMABAN].apply(len) >= min_horses]
    return bet_df.rename(columns={ResultsCols.UMABAN: key}).T.to_dict()


class BetPolicyTansho:
    """thresholdを超えた馬に単勝で賭ける戦略。"""

    @staticmethod
    def judge(score_table: pd.DataFrame, threshold: float) -> dict:
        return _threshold_umaban_judge(score_table, threshold, "tansho")


class BetPolicyFukusho:
    """thresholdを超えた馬に複勝で賭ける戦略。"""

    @staticmethod
    def judge(score_table: pd.DataFrame, threshold: float) -> dict:
        return _threshold_umaban_judge(score_table, threshold, "fukusho")


class BetPolicyWakurenBox:
    """thresholdを超えた馬の枠に枠連BOXで賭ける戦略（wakuban_flag を併用）。"""

    @staticmethod
    def judge(score_table: pd.DataFrame, threshold: float) -> dict:
        filtered_table = score_table[(score_table["score"] >= threshold) & (score_table["wakuban_flag"] == 1)]
        bet_df = filtered_table.groupby(level=0)[ResultsCols.WAKUBAN].apply(list).to_frame()
        bet_df = bet_df[bet_df[ResultsCols.WAKUBAN].apply(len) >= 2]
        return bet_df.rename(columns={ResultsCols.WAKUBAN: "wakuren"}).T.to_dict()


class BetPolicyUmarenBox:
    """thresholdを超えた馬に馬連BOXで賭ける戦略。"""

    @staticmethod
    def judge(score_table: pd.DataFrame, threshold: float) -> dict:
        return _threshold_umaban_judge(score_table, threshold, "umaren", min_horses=2)


class BetPolicyUmatanBox:
    """thresholdを超えた馬に馬単BOXで賭ける戦略。"""

    @staticmethod
    def judge(score_table: pd.DataFrame, threshold: float) -> dict:
        return _threshold_umaban_judge(score_table, threshold, "umatan", min_horses=2)


class BetPolicyWideBox:
    """thresholdを超えた馬にワイドBOXで賭ける戦略。"""

    @staticmethod
    def judge(score_table: pd.DataFrame, threshold: float) -> dict:
        return _threshold_umaban_judge(score_table, threshold, "wide", min_horses=2)


class BetPolicySanrenpukuBox:
    """thresholdを超えた馬に三連複BOXで賭ける戦略。"""

    @staticmethod
    def judge(score_table: pd.DataFrame, threshold: float) -> dict:
        return _threshold_umaban_judge(score_table, threshold, "sanrenpuku", min_horses=3)


class BetPolicySanrentanBox:
    """thresholdを超えた馬に三連単BOXで賭ける戦略。"""

    @staticmethod
    def judge(score_table: pd.DataFrame, threshold: float) -> dict:
        return _threshold_umaban_judge(score_table, threshold, "sanrentan", min_horses=3)


class BetPolicyTanshoFukusho:
    """threshold1 を超えた馬に単勝、threshold2 を超えた馬に複勝を併用して賭ける戦略。

    出力は {race_id: {'tansho': [馬番...], 'fukusho': [馬番...]}} 形式で、
    単勝・複勝それぞれ独立の閾値で対象馬を選ぶ。
    """

    @staticmethod
    def judge(score_table: pd.DataFrame, threshold1: float, threshold2: float) -> dict:
        tansho = _threshold_umaban_judge(score_table, threshold1, "tansho")
        fukusho = _threshold_umaban_judge(score_table, threshold2, "fukusho")
        merged = {}
        for race_id in set(tansho) | set(fukusho):
            entry = {}
            entry.update(tansho.get(race_id, {}))
            entry.update(fukusho.get(race_id, {}))
            merged[race_id] = entry
        return merged


class BetPolicyUmatanNagashi:
    """threshold1 を超えた馬を軸、threshold2 を超えた馬を相手に馬単流しで賭ける戦略（未実装）。"""

    @staticmethod
    def judge(score_table: pd.DataFrame, threshold1: float, threshold2: float) -> dict:
        raise NotImplementedError


# 既定のリスク管理パラメータ（RiskLimits は frozen で不変のため単一インスタンスを共有）。
_DEFAULT_RISK_LIMITS = RiskLimits()


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
    ev_max : 期待値の上限。EV がこれを超える超高倍率・極小確率の馬券を除外する
        （リスク集中を防ぐ。§7）。既定は inf（上限なし・後方互換）。
    """

    def __init__(
        self,
        odds_provider: AbstractOddsProvider,
        thresholds: dict,
        bet_types=None,
        risk_limits: RiskLimits = _DEFAULT_RISK_LIMITS,
        ev_max: float = float("inf"),
        bet_type_params=None,
        direct_place_prob: bool = True,
    ) -> None:
        self._odds_provider = odds_provider
        self._thresholds = thresholds
        self._bet_types = list(bet_types) if bet_types is not None else list(thresholds.keys())
        self._risk = risk_limits
        self._ev_max = ev_max
        # Stage A: 複勝はモデルの top3 出力（place_prob_table もしくは PROB 列）を
        # **直接** 的中確率に使う（Harville 再導出しない）。Place ヘッドが top3 を
        # 直接予測しているため、これが本来の複勝確率。False で従来の Harville 経路。
        self._direct_place_prob = direct_place_prob
        # 券種別最適化パラメータ（DI）。{券種: BetTypeParams}。指定券種は temperature /
        # prob_scale / ev_threshold / ev_max を上書きする。未指定券種は従来挙動
        # （thresholds[券種] と ev_max、温度・較正なし）を完全に保持する。
        self._bet_type_params = dict(bet_type_params) if bet_type_params else {}

    def select(self, prob_table: pd.DataFrame, place_prob_table: pd.DataFrame | None = None) -> list:
        """較正確率テーブルから BetCandidate のリストを返す。

        prob_table: race_id を index に持ち、列 [ResultsCols.UMABAN, "prob"] を含む DataFrame。
            連系の Harville 計算に使う「勝率」相当（Stage B では Win ヘッド出力）。
        place_prob_table: 複勝の top3 確率テーブル（同形式）。省略時は prob_table を流用
            （Place ヘッド単独運用＝base モデルが top3 を出している前提）。
        """
        place_map = self._build_place_map(place_prob_table)
        candidates = []
        for race_id, race_df in prob_table.groupby(level=0):
            candidates.extend(
                self._select_for_race(race_id, race_df, place_probs=place_map.get(race_id))
            )
        return candidates

    @staticmethod
    def _build_place_map(place_prob_table: pd.DataFrame | None) -> dict:
        """place_prob_table を {race_id: {馬番: top3確率}} に変換する（無ければ空）。"""
        if place_prob_table is None or place_prob_table.empty:
            return {}
        out: dict = {}
        for race_id, df in place_prob_table.groupby(level=0):
            out[race_id] = {
                u: float(p)
                for u, p in zip(df[ResultsCols.UMABAN], df[PROB], strict=False)
                if pd.notna(p) and p > 0
            }
        return out

    def judge(self, score_table: pd.DataFrame, **params) -> dict:
        """既存 BetPolicy* と同じ {race_id: {馬券種: [馬番...]}} 形式で返す。

        Simulator.calc_returns_per_race と互換。馬連等の複数頭組合せは馬番を
        フラットに展開して BOX 互換のリストにする（Simulator の bet_*_box が
        組合せを再生成するため）。EV 上位を採用する select() の結果を流用する。
        """
        candidates = self.select(score_table)
        bet_dict: dict = {}
        for cand in candidates:
            race_bets = bet_dict.setdefault(cand.race_id, {})
            umaban_list = race_bets.setdefault(cand.bet_type, [])
            for umaban in cand.combo:
                if umaban not in umaban_list:
                    umaban_list.append(umaban)
        return bet_dict

    def _select_for_race(self, race_id, race_df: pd.DataFrame, place_probs: dict | None = None) -> list:
        # 確率の健全性ガード: NaN/<=0 は Harville 正規化を汚染するため win_probs から除外する
        win_probs = {
            u: float(p)
            for u, p in zip(race_df[ResultsCols.UMABAN], race_df[PROB], strict=False)
            if pd.notna(p) and p > 0
        }
        # 複勝直接確率: place_prob_table 指定があればそれ、無ければ prob_table を流用
        # （base モデルが top3 を予測している前提＝Stage A）。
        if place_probs is None:
            place_probs = dict(win_probs)
        # 低確率帯のノイズを足切り（KB 7.3）
        eligible = [u for u, p in win_probs.items() if p >= self._risk.MIN_WIN_PROB]

        race_candidates = []
        for bet_type in self._bet_types:
            size = COMBO_SIZE[bet_type]
            if len(eligible) < size:
                continue
            generator = permutations if bet_type in ORDERED else combinations
            # 券種別パラメータがあれば EV 閾値・上限・温度・較正を上書きする。
            bt_params = self._bet_type_params.get(bet_type)
            if bt_params is not None:
                threshold = bt_params.ev_threshold
                ev_max = bt_params.ev_max
                bt_win_probs = bet_type_params.apply_temperature(win_probs, bt_params.temperature)
                prob_scale = bt_params.prob_scale
            else:
                threshold = self._thresholds[bet_type]
                ev_max = self._ev_max
                bt_win_probs = win_probs
                prob_scale = 1.0
            for combo in generator(eligible, size):
                if self._direct_place_prob and bet_type == BetType.FUKUSHO:
                    # Stage A: 複勝はモデルの top3 出力を直接使う（Harville 再導出しない）
                    base_p = place_probs.get(combo[0])
                    if base_p is None or base_p <= 0:
                        continue
                    prob = base_p * prob_scale
                else:
                    prob = harville.combo_probability(bet_type, bt_win_probs, combo) * prob_scale
                odds = self._odds_provider.get_odds(race_id, bet_type, combo)
                # オッズ健全性ガード: NaN / <=0 / inf の異常オッズは EV 計算せずスキップ
                if not (math.isfinite(odds) and odds > 0):
                    continue
                ev = prob * odds
                # 期待値が閾値超〜上限以内のもののみ採用（上限で超高倍率を除外。§7）
                if threshold < ev <= ev_max:
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
