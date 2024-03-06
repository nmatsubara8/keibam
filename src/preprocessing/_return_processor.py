import pandas as pd

from src.preprocessing._abstract_data_processor import AbstractDataProcessor


def count_br(df, column):
    """
    DataFrameの特定の列に対して、行ごとに含まれる文字列"br"の数を数える関数
    :param df: DataFrame
    :param column: 対象の列
    :return: 各行に含まれる"br"の数のリスト
    """
    return max([cell.count("br") if isinstance(cell, str) else 0 for cell in df[column]])


# カンマ区切りの数字文字列からカンマを削除して整数に変換する関数
def convert_to_int(s):
    if isinstance(s, str):
        s = s.replace(",", "")
    return int(s)


# - 区切りをカンマ区切りに変換
def split_bar_to_int(s):
    if isinstance(s, str):
        s = s.split("-")
        s = [int(num) for num in s]
    return s


# 矢印 区切りをカンマ区切りに変換
def split_arrow_to_int(s):
    if isinstance(s, str):
        s = s.split("→")
        s = [int(num) for num in s]
    return s


def sort_tuple(tup):
    """
    タプルの中身をソートする関数
    """
    return tuple(sorted(tup))


class ReturnProcessor(AbstractDataProcessor):
    def __init__(self, filepath):
        """
        初期処理
        """
        super().__init__(filepath)

    def _preprocess(self):
        """
        前処理
        """

        return_dict = {}
        return_dict["tansho"] = self.__tansho()
        return_dict["wakuren"] = self.__wakuren()
        return_dict["fukusho"] = self.__fukusho()
        return_dict["umaren"] = self.__umaren()
        return_dict["umatan"] = self.__umatan()
        return_dict["wide"] = self.__wide()
        return_dict["sanrentan"] = self.__sanrentan()
        return_dict["sanrenpuku"] = self.__sanrenpuku()
        return return_dict

    def __tansho(self):
        """
        単勝
        """
        # print(f"self.raw_data1:{self.raw_data}")

        tansho = self.raw_data[self.raw_data[0] == "単勝"][[1, 2, "race_id"]]
        tansho_row_num = count_br(tansho, 1) + 1
        print(f"単勝列数:{tansho_row_num}")
        # "br"で分割し、expand=Trueを指定してDataFrameに展開する
        wins = tansho[1].str.split("br", n=tansho_row_num, expand=True)
        # 不足する部分を0に置き換える
        wins = wins.fillna(0)
        # 列名を変更する
        wins.columns = [f"win_{i}" for i in range(len(wins.columns))]
        for i in range(len(wins.columns)):
            col_name = f"win_{i}"
            wins[col_name] = wins[col_name].apply(convert_to_int)
        return_ = tansho[2].str.split("br", n=tansho_row_num, expand=True)
        # 不足する部分を0に置き換える
        return_ = return_.fillna(0)
        return_.columns = [f"return_{i}" for i in range(len(return_.columns))]
        for i in range(len(return_.columns)):
            col_name = f"return_{i}"
            return_[col_name] = return_[col_name].apply(convert_to_int)
        df = pd.concat([tansho["race_id"], wins, return_], axis=1)
        df.set_index("race_id", inplace=True)
        return df

    def __wakuren(self):
        """
        枠連
        """
        wakuren = self.raw_data[self.raw_data[0] == "枠連"][[1, 2, "race_id"]]
        wakuren_row_num = count_br(wakuren, 1) + 1
        print(f"枠連列数:{wakuren_row_num}")
        # "br"で分割し、expand=Trueを指定してDataFrameに展開する
        wins = wakuren[1].str.split("br", n=wakuren_row_num, expand=True)
        # 不足する部分を0に置き換える
        wins = wins.fillna(0)
        # 列名を変更する
        wins.columns = [f"win_{i}" for i in range(len(wins.columns))]
        for i in range(len(wins.columns)):
            col_name = f"win_{i}"
            wins[col_name] = wins[col_name].apply(split_bar_to_int)
        return_ = wakuren[2].str.split("br", n=wakuren_row_num, expand=True)
        # 不足する部分を0に置き換える
        return_ = return_.fillna(0)
        return_.columns = [f"return_{i}" for i in range(len(return_.columns))]
        for i in range(len(return_.columns)):
            col_name = f"return_{i}"
            return_[col_name] = return_[col_name].apply(convert_to_int)
        df = pd.concat([wakuren["race_id"], wins, return_], axis=1)
        df.set_index("race_id", inplace=True)
        return df

    def __fukusho(self):
        """
        #複勝
        """
        fukusho = self.raw_data[self.raw_data[0] == "複勝"][[1, 2, "race_id"]]
        fukusho_row_num = count_br(fukusho, 1) + 1
        print(f"複勝列数:{fukusho_row_num}")
        # "br"で分割し、expand=Trueを指定してDataFrameに展開する
        wins = fukusho[1].str.split("br", n=fukusho_row_num, expand=True)
        # 不足する部分を0に置き換える
        wins = wins.fillna(0)
        # 列名を変更する
        wins.columns = [f"win_{i}" for i in range(len(wins.columns))]

        return_ = fukusho[2].str.split("br", n=fukusho_row_num, expand=True)
        # 不足する部分を0に置き換える
        return_ = return_.fillna(0)
        return_.columns = [f"return_{i}" for i in range(len(return_.columns))]
        for i in range(len(return_.columns)):
            col_name = f"return_{i}"
            return_[col_name] = return_[col_name].apply(convert_to_int)
        df = pd.concat([fukusho["race_id"], wins, return_], axis=1)
        df.set_index("race_id", inplace=True)
        return df

    def __umaren(self):
        """
        #馬連
        """
        umaren = self.raw_data[self.raw_data[0] == "馬連"][[1, 2, "race_id"]]
        umaren_row_num = count_br(umaren, 1) + 1
        print(f"馬単列数:{umaren_row_num}")
        # "br"で分割し、expand=Trueを指定してDataFrameに展開する
        wins = umaren[1].str.split("br", n=umaren_row_num, expand=True)
        # 不足する部分を0に置き換える
        wins = wins.fillna(0)
        # 列名を変更する
        wins.columns = [f"win_{i}" for i in range(len(wins.columns))]
        for i in range(len(wins.columns)):
            col_name = f"win_{i}"
            wins[col_name] = wins[col_name].apply(split_bar_to_int)
        return_ = umaren[2].str.split("br", n=umaren_row_num, expand=True)
        # 不足する部分を0に置き換える
        return_ = return_.fillna(0)
        return_.columns = [f"return_{i}" for i in range(len(return_.columns))]
        for i in range(len(return_.columns)):
            col_name = f"return_{i}"
            return_[col_name] = return_[col_name].apply(convert_to_int)
        df = pd.concat([umaren["race_id"], wins, return_], axis=1)
        df.set_index("race_id", inplace=True)
        return df

    def __umatan(self):
        """
        #馬単
        """
        umatan = self.raw_data[self.raw_data[0] == "馬単"][[1, 2, "race_id"]]
        umatan_row_num = count_br(umatan, 1) + 1
        print(f"馬単列数:{umatan_row_num}")
        # "br"で分割し、expand=Trueを指定してDataFrameに展開する
        wins = umatan[1].str.split("br", n=umatan_row_num, expand=True)
        # 不足する部分を0に置き換える
        wins = wins.fillna(0)
        # 列名を変更する
        wins.columns = [f"win_{i}" for i in range(len(wins.columns))]
        for i in range(len(wins.columns)):
            col_name = f"win_{i}"
            wins[col_name] = wins[col_name].apply(split_arrow_to_int)
        return_ = umatan[2].str.split("br", n=umatan_row_num, expand=True)
        # 不足する部分を0に置き換える
        return_ = return_.fillna(0)
        return_.columns = [f"return_{i}" for i in range(len(return_.columns))]
        for i in range(len(return_.columns)):
            col_name = f"return_{i}"
            return_[col_name] = return_[col_name].apply(convert_to_int)
        df = pd.concat([umatan["race_id"], wins, return_], axis=1)
        df.set_index("race_id", inplace=True)
        return df

    def __wide(self):
        """
        #ワイド
        """
        wide = self.raw_data[self.raw_data[0] == "ワイド"][[1, 2, "race_id"]]
        wide_row_num = count_br(wide, 1) + 1
        print(f"ワイド列数:{wide_row_num}")

        # "br"で分割し、expand=Trueを指定してDataFrameに展開する
        wins = wide[1].str.split("br", n=wide_row_num, expand=True)
        # 不足する部分を0に置き換える
        wins = wins.fillna(0)
        # 列名を変更する
        wins.columns = [f"win_{i}" for i in range(len(wins.columns))]
        for i in range(len(wins.columns)):
            col_name = f"win_{i}"
            wins[col_name] = wins[col_name].apply(split_bar_to_int)

        # wins = wins.stack().str.split("-", n=2, expand=True).add_prefix("win_")
        # "br"で分割し、expand=Trueを指定してDataFrameに展開する
        return_ = wide[2].str.split("br", n=wide_row_num, expand=True)
        # 不足する部分を0に置き換える
        return_ = return_.fillna(0)
        # 列名を変更する
        return_.columns = [f"return_{i}" for i in range(len(return_.columns))]
        for i in range(len(return_.columns)):
            col_name = f"return_{i}"
            return_[col_name] = return_[col_name].apply(convert_to_int)
        # return_ = return_.stack().rename("return")
        # wide["race_id"].astype(int)
        df = pd.concat([wide["race_id"], wins, return_], axis=1)
        df.set_index("race_id", inplace=True)
        return df
        # return df.apply(lambda x: pd.to_numeric(x.str.replace(",", ""), errors="coerce", downcast="integer"))

    def __sanrentan(self):
        """
        #三連単
        """
        rentan = self.raw_data[self.raw_data[0] == "三連単"][[1, 2, "race_id"]]
        rentan_row_num = count_br(rentan, 1) + 1
        print(f"三連単列数:{rentan_row_num}")
        # "br"で分割し、expand=Trueを指定してDataFrameに展開する
        wins = rentan[1].str.split("br", n=rentan_row_num, expand=True)
        # 不足する部分を0に置き換える
        wins = wins.fillna(0)
        # 列名を変更する
        wins.columns = [f"win_{i}" for i in range(len(wins.columns))]
        for i in range(len(wins.columns)):
            col_name = f"win_{i}"
            wins[col_name] = wins[col_name].apply(split_arrow_to_int)
        return_ = rentan[2].str.split("br", n=rentan_row_num, expand=True)
        # 不足する部分を0に置き換える
        return_ = return_.fillna(0)
        return_.columns = [f"return_{i}" for i in range(len(return_.columns))]
        for i in range(len(return_.columns)):
            col_name = f"return_{i}"
            return_[col_name] = return_[col_name].apply(convert_to_int)
        df = pd.concat([rentan["race_id"], wins, return_], axis=1)
        df.set_index("race_id", inplace=True)
        return df

    def __sanrenpuku(self):
        """
        #三連複
        """
        renpuku = self.raw_data[self.raw_data[0] == "三連複"][[1, 2, "race_id"]]
        renpuku_row_num = count_br(renpuku, 1) + 1
        print(f"三連複列数:{renpuku_row_num}")
        # "br"で分割し、expand=Trueを指定してDataFrameに展開する
        wins = renpuku[1].str.split("br", n=renpuku_row_num, expand=True)
        # 不足する部分を0に置き換える
        wins = wins.fillna(0)
        # 列名を変更する
        wins.columns = [f"win_{i}" for i in range(len(wins.columns))]
        for i in range(len(wins.columns)):
            col_name = f"win_{i}"
            wins[col_name] = wins[col_name].apply(split_bar_to_int)

        return_ = renpuku[2].str.split("br", n=renpuku_row_num, expand=True)
        # 不足する部分を0に置き換える
        return_ = return_.fillna(0)
        return_.columns = [f"return_{i}" for i in range(len(return_.columns))]
        # return列のカンマ区切りの数字文字列を整数に変換する
        for i in range(len(return_.columns)):
            col_name = f"return_{i}"
            return_[col_name] = return_[col_name].apply(convert_to_int)
        df = pd.concat([renpuku["race_id"], wins, return_], axis=1)
        df.set_index("race_id", inplace=True)
        return df
