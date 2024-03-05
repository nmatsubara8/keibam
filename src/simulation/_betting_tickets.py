from itertools import combinations
from itertools import permutations
from math import factorial

from src.preprocessing._return_processor import ReturnProcessor


def count_comb(n, r):
    # 組み合わせの数を計算する
    return factorial(n) / (factorial(r) * factorial(n - r))


def count_perm(n, r):
    # 組み合わせの数を計算する
    return factorial(n) // factorial(n - r)


class BettingTickets:
    """
    馬券の買い方と、賭けた時のリターンを計算する。
    """

    def __init__(self, returnProcessor: ReturnProcessor) -> None:
        self.__returnTables = returnProcessor.preprocessed_data
        self.__returnTablesTansho = self.__returnTables["tansho"]
        self.__returnTablesFukusho = self.__returnTables["fukusho"]
        self.__returnTablesUmaren = self.__returnTables["umaren"]
        self.__returnTablesWakuren = self.__returnTables["wakuren"]
        self.__returnTablesUmatan = self.__returnTables["umatan"]
        self.__returnTablesWide = self.__returnTables["wide"]
        self.__returnTablesSanrenpuku = self.__returnTables["sanrenpuku"]
        self.__returnTablesSanrentan = self.__returnTables["sanrentan"]

    def bet_tansho(self, race_id: int, umaban: list, amount: int):
        """
        race_id: レースid。
        umaban: 賭けたい馬番をリストで入れる。一頭のみ賭けたい場合もリストで入れる。
        amount: 1枚に賭ける額。
        """
        # 賭ける枚数

        n_bets = len(umaban)
        # print(f"n_bets:{n_bets}")
        if n_bets == 0:
            return 0, 0, 0
        else:
            # 賭けた合計額
            bet_amount = n_bets * amount
            # print(f"n_bets:{n_bets}")
            # 的中判定
            return_amount = 0
            table_1R = self.__returnTablesTansho.loc[race_id]
            length = len(table_1R) // 2
            for uma in umaban:
                for i in range(length):
                    win_column = f"win_{i}"
                    # print(f"win_{i}")
                    if table_1R[win_column] != 0 and int(table_1R[win_column]) == uma:
                        return_column = f"return_{i}"
                        return_amount += table_1R[return_column] * amount / 100
            return n_bets, bet_amount, return_amount

    def bet_fukusho(self, race_id: int, umaban: list, amount: int):
        """
        引数の考え方は単勝と同様。
        """
        # 賭ける枚数
        n_bets = len(umaban)
        if n_bets == 0:
            return 0, 0, 0
        else:
            # 賭けた合計額
            bet_amount = n_bets * amount
            # print(f"n_bets:{n_bets}")
            # 的中判定
            return_amount = 0
            table_1R = self.__returnTablesFukusho.loc[race_id]
            length = len(table_1R) // 2
            for uma in umaban:
                for i in range(length):
                    win_column = f"win_{i}"
                    # print(f"win_{i}")
                    if table_1R[win_column] != 0 and int(table_1R[win_column]) == uma:
                        return_column = f"return_{i}"
                        return_amount += table_1R[return_column] * amount / 100
            return n_bets, bet_amount, return_amount

    def bet_wakuren_box(self, race_id: int, umaban: list, amount: int):
        """
        枠連BOX馬券。1枚のみ買いたい場合もこの関数を使う。
        """
        # 賭ける枚数
        # 例）4C2（4コンビネーション2
        #
        # print("Value", umaban)
        if len(umaban) < 2:
            print("例外", umaban)
            return 0, 0, 0
        else:
            # print("umaban", umaban)
            n_bets = count_comb(len(umaban), 2)
            print(f"n_bets:{n_bets}")
            # 賭けた合計額
            bet_amount = n_bets * amount

            # 的中判定
            return_amount = 0
            table_1R = self.__returnTablesWakuren.loc[race_id]
            length = len(table_1R) // 2
            # 2つの要素を選ぶ組み合わせを生成してセットに変換
            unique_combinations_set = set(combinations(umaban, 2))
            # セットをリストに変換する（任意の順序で表示するため）
            unique_combinations = list(unique_combinations_set)
            for combination in unique_combinations:
                for i in range(length):
                    win_column = f"win_{i}"
                    if table_1R[win_column] != 0 and tuple(sorted(table_1R[win_column])) == combination:
                        return_column = f"return_{i}"
                        return_amount += table_1R[return_column] * amount / 100
            return n_bets, bet_amount, return_amount

    def bet_umaren_box(self, race_id: int, umaban: list, amount: int):
        """
        馬連BOX馬券。1枚のみ買いたい場合もこの関数を使う。
        """
        # 賭ける枚数
        # 例）4C2（4コンビネーション2
        #
        if len(umaban) < 2:
            print("例外", umaban)
            return 0, 0, 0
        else:
            # print("umaban", umaban)
            n_bets = count_comb(len(umaban), 2)
            print(f"n_bets:{n_bets}")
            # 賭けた合計額
            bet_amount = n_bets * amount

            # 的中判定
            return_amount = 0
            table_1R = self.__returnTablesUmaren.loc[race_id]
            length = len(table_1R) // 2
            combinations_list = list(combinations(umaban, 2))

            for combination in combinations_list:
                for i in range(length):
                    win_column = f"win_{i}"
                    if table_1R[win_column] != 0 and tuple(sorted(table_1R[win_column])) == combination:
                        return_column = f"return_{i}"
                        return_amount += table_1R[return_column] * amount / 100
            return n_bets, bet_amount, return_amount

    def bet_umatan_box(self, race_id: int, umaban: list, amount: int):
        """
        馬単を一枚のみ賭ける場合の関数。umabanは[1着予想, 2着予想]の形で馬番を入れる。
        """
        if len(umaban) < 2:
            print("例外", umaban)
            return 0, 0, 0
        else:
            # 賭ける枚数
            # 例）4P2（4順列2)
            n_bets = count_perm(len(umaban), 2)
            print(f"n_bets:{n_bets}")
            # 賭けた合計額
            bet_amount = n_bets * amount

            # 的中判定
            return_amount = 0
            table_1R = self.__returnTablesUmatan.loc[race_id]
            length = len(table_1R) // 2
            permutations_list = list(permutations(umaban, 2))

            for permutation in permutations_list:
                for i in range(length):
                    win_column = f"win_{i}"
                    if table_1R[win_column] != 0 and tuple(table_1R[win_column]) == permutation:
                        return_column = f"return_{i}"
                        return_amount += table_1R[return_column] * amount / 100
            return n_bets, bet_amount, return_amount

    def bet_umatan(self, race_id: int, umaban: list, amount: int):
        """
        馬単をBOX馬券で賭ける場合の関数。
        """
        n_bets = len(umaban)
        print(f"n_bets:{n_bets}")
        if n_bets == 0:
            print("例外")
            return 0, 0, 0
        else:
            # 賭けた合計額
            bet_amount = n_bets * amount

            # 的中判定
            return_amount = 0
            table_1R = self.__returnTablesUmatan.loc[race_id]
            length = int(len(table_1R) / 2)
            for uma in umaban:
                for i in range(length):
                    win_column = f"win_{i}"
                    # print(f"win_{i}")
                    if table_1R[win_column] != 0 and int(table_1R[win_column]) == uma:
                        return_column = f"return_{i}"
                        return_amount += table_1R[return_column] * amount / 100
            return n_bets, bet_amount, return_amount

    def bet_wide_box(self, race_id: int, umaban: list, amount: int):
        """
        ワイドをBOX馬券で賭ける関数。1枚のみ賭ける場合もこの関数を使う。
        """
        if len(umaban) < 2:
            print("例外", umaban)
            return 0, 0, 0
        else:
            # 賭けた合計額
            n_bets = count_comb(len(umaban), 2)
            print(f"n_bets:{n_bets}")
            bet_amount = n_bets * amount

            # 的中判定
            return_amount = 0
            table_1R = self.__returnTablesWide.loc[race_id]
            length = len(table_1R) // 2
            combinations_list = list(combinations(umaban, 2))
            for combination in combinations_list:
                for i in range(length):
                    win_column = f"win_{i}"
                    # print(f"win_{i}")
                    if table_1R[win_column] != 0 and tuple(sorted(table_1R[win_column])) == combination:
                        return_column = f"return_{i}"
                        return_amount += table_1R[return_column] * amount / 100
            return n_bets, bet_amount, return_amount

    def bet_sanrenpuku_box(self, race_id: str, umaban: list, amount: int):
        """
        三連複BOX馬券。1枚のみ買いたい場合もこの関数を使う。
        """
        # 賭ける枚数
        # 例）4C2（4コンビネーション2

        if len(umaban) < 3:
            print("例外", umaban)
            return 0, 0, 0
        else:
            # print("umaban", umaban)
            n_bets = count_comb(len(umaban), 3)
            print(f"n_bets:{n_bets}")
            # 賭けた合計額
            bet_amount = n_bets * amount

            # 的中判定
            return_amount = 0
            table_1R = self.__returnTablesSanrenpuku.loc[race_id]
            length = len(table_1R) // 2
            combinations_list = list(combinations(umaban, 3))

            for combination in combinations_list:
                for i in range(length):
                    win_column = f"win_{i}"
                    if table_1R[win_column] != 0 and tuple(sorted(table_1R[win_column])) == combination:
                        return_column = f"return_{i}"
                        return_amount += table_1R[return_column] * amount / 100
            return n_bets, bet_amount, return_amount

    def _bet_sanrentan(self, race_id: int, umaban: list, amount: int):
        """
        三連単を一枚のみ賭ける場合の関数。umabanは[1着予想, 2着予想, 3着予想]の形で馬番を入れる。
        """
        n_bets = len(umaban)
        print(f"n_bets:{n_bets}")
        if n_bets == 0:
            print("例外")
            return 0, 0, 0
        else:
            # 賭けた合計額
            bet_amount = n_bets * amount

            # 的中判定
            return_amount = 0
            table_1R = self.__returnTablesSanrentan.loc[race_id]
            length = int(len(table_1R) / 2)
            for uma in umaban:
                for i in range(length):
                    win_column = f"win_{i}"
                    # print(f"win_{i}")
                    if table_1R[win_column] != 0 and int(table_1R[win_column]) == uma:
                        return_column = f"return_{i}"
                        return_amount += table_1R[return_column] * amount / 100
            return n_bets, bet_amount, return_amount

    def bet_sanrentan_box(self, race_id: str, umaban: list, amount: int):
        """
        三連単をBOX馬券で賭ける場合の関数。
        """
        if len(umaban) < 3:
            print("例外", umaban)
            return 0, 0, 0
        else:
            # 賭ける枚数
            # 例）4P2（4順列2)
            n_bets = count_perm(len(umaban), 3)
            print(f"n_bets:{n_bets}")
            # 賭けた合計額
            bet_amount = n_bets * amount

            # 的中判定
            return_amount = 0
            table_1R = self.__returnTablesSanrentan.loc[race_id]
            length = len(table_1R) // 2
            permutations_list = list(permutations(umaban, 3))

            for permutation in permutations_list:
                for i in range(length):
                    win_column = f"win_{i}"
                    if table_1R[win_column] != 0 and tuple(table_1R[win_column]) == permutation:
                        return_column = f"return_{i}"
                        return_amount += table_1R[return_column] * amount / 100
            return n_bets, bet_amount, return_amount

    def others(self, race_id: str, umaban: list, amount: int):
        """
        その他、フォーメーション馬券や流し馬券の定義
        """
        pass
