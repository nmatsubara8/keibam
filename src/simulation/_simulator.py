import pandas as pd

from src.preprocessing._return_processor import ReturnProcessor
from src.simulation._betting_tickets import BettingTickets
from src.simulation._metrics import summarize_returns


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

        # 馬券種 → BettingTickets の対応メソッド名のディスパッチ表。
        # 全 bet_* は (race_id, 馬番リスト, 金額) -> (n_bets, bet_amount, return_amount)。
        dispatch = {
            "tansho": "bet_tansho",
            "fukusho": "bet_fukusho",
            "wakuren": "bet_wakuren_box",
            "umaren": "bet_umaren_box",
            "umatan": "bet_umatan_box",
            "wide": "bet_wide_box",
            "sanrenpuku": "bet_sanrenpuku_box",
            "sanrentan": "bet_sanrentan_box",
        }

        for race_id in actions:
            n_bets_race = 0
            bet_amount_race = 0
            return_amount_race = 0
            for action in actions[race_id]:
                method_name = dispatch.get(action)
                if method_name is None:
                    continue  # 未知の馬券種はスキップ
                bet_method = getattr(self.betting_tickets, method_name)
                n_bets, bet_amount, return_amount = bet_method(race_id, actions[race_id][action], 1)

                n_bets_race += n_bets
                bet_amount_race += bet_amount
                return_amount_race += return_amount

            # 実際に賭けが成立したレースのみ記録する。払戻テーブル欠損や閾値未満で
            # 1 枚も賭けなかったレース（n_bets=0）を含めると的中率が希釈されるため除外。
            if n_bets_race > 0:
                returns_per_race_dict[race_id] = {
                    "n_bets": n_bets_race,
                    "bet_amount": bet_amount_race,
                    "return_amount": return_amount_race,
                    "hit_or_not": 1 if return_amount_race > 0 else 0,
                }
        return pd.DataFrame.from_dict(returns_per_race_dict, orient="index")

    def calc_returns(self, actions: dict) -> dict:
        """
        self.calc_returns_per_race(actions)の結果を集計する。

        回収率・標準偏差に加え、シャープレシオ・的中率・最大ドローダウン・損益を返す
        （指標計算は src.simulation._metrics.summarize_returns に委譲）。
        """
        if len(actions) == 0:
            return {}
        returns_per_race = self.calc_returns_per_race(actions)
        return summarize_returns(returns_per_race)
