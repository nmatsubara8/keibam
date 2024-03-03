import numpy as np
import pandas as pd

from src.preprocessing._return_processor import ReturnProcessor

from ._betting_tickets import BettingTickets


class Simulator:
    """
    賭けた馬券を元に、成績を記録していくクラス。
    """

    def __init__(self, return_processor: ReturnProcessor) -> None:
        self.betting_tickets = BettingTickets(return_processor)

    def calc_returns_per_race(self, actions: dict) -> pd.DataFrame:
        """
        KeibaAI.decideActionの出力を入れると、レースごとに

        - n_bets: そのレースで賭けた馬券の枚数
        - bet_amount: そのレースで賭けた金額
        - return_amount: そのレースでの払戻金
        - hit_or_not: 的中したかどうか

        が返ってくる。
        """
        returns_per_race_dict = {}

        for race_id, action_list in actions.items():
            # print(f"race_id:{race_id}")

            n_bets_race = 0
            bet_amount_race = 0
            return_amount_race = 0
            for action, umaban in action_list.items():
                if action == "tansho":
                    n_bets, bet_amount, return_amount = self.betting_tickets.bet_tansho(race_id, umaban, 1)
                    # print(f"n_bets, bet_amount, return_amount:{n_bets, bet_amount, return_amount}")
                elif action == "fukusho":
                    n_bets, bet_amount, return_amount = self.betting_tickets.bet_fukusho(race_id, umaban, 1)
                elif action == "umaren":
                    n_bets, bet_amount, return_amount = self.betting_tickets.bet_umaren_box(race_id, umaban, 1)
                elif action == "umatan":
                    n_bets, bet_amount, return_amount = self.betting_tickets.bet_umatan_box(race_id, umaban, 1)
                elif action == "wide":
                    n_bets, bet_amount, return_amount = self.betting_tickets.bet_wide_box(race_id, umaban, 1)
                elif action == "sanrenpuku":
                    n_bets, bet_amount, return_amount = self.betting_tickets.bet_sanrenpuku_box(race_id, umaban, 1)
                elif action == "sanrentan":
                    n_bets, bet_amount, return_amount = self.betting_tickets.bet_sanrentan_box(race_id, umaban, 1)
                n_bets_race += n_bets
                bet_amount_race += bet_amount
                return_amount_race += return_amount
                returns_per_race_dict[race_id] = {
                    "n_bets": n_bets_race,
                    "bet_amount": bet_amount_race,
                    "return_amount": return_amount_race,
                    "hit_or_not": 1 if return_amount_race > 0 else 0,
                }
                # print(f"returns_per_race_dict:{returns_per_race_dict}")
        return pd.DataFrame.from_dict(returns_per_race_dict, orient="index")

    def calc_returns(self, actions: dict) -> dict:
        """
        self.calc_returns_per_race(actions)の結果を集計する
        """
        returns_dict = {}
        if len(actions) != 0:
            returns_per_race = self.calc_returns_per_race(actions)

            returns_dict["n_bets"] = returns_per_race["n_bets"].sum()
            returns_dict["n_races"] = returns_per_race.index.nunique()
            returns_dict["n_hits"] = returns_per_race["hit_or_not"].sum()
            returns_dict["total_bet_amount"] = returns_per_race["bet_amount"].sum()

            if returns_dict["total_bet_amount"] == 0:
                returns_dict["return_rate"] = 0
            else:
                returns_dict["return_rate"] = returns_per_race["return_amount"].sum() / returns_dict["total_bet_amount"]

            if returns_dict["total_bet_amount"] == 0:
                returns_dict["std"] = 0
            else:
                returns_dict["std"] = (
                    returns_per_race["return_amount"].std()
                    * np.sqrt(returns_dict["n_races"])
                    / returns_dict["total_bet_amount"]
                )
            # print("returns_dict in sim", returns_dict)
        return returns_dict
