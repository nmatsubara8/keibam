"""払戻テーブル前処理。netkeiba のレース結果ページから取得した「払戻」
HTML テーブル（カテゴリ／的中組合せ／払戻金）を、馬券種別 DataFrame の dict に変換する。

## 入出力の構造（マジック値の意味）

raw_data（スクレイプ生）は以下の列構造を持つ:
- 列 0: 馬券種ラベル（"単勝"/"複勝"/.../"三連単"）
- 列 1: 的中組合せ（"3 br 5" のように `br` 区切り、組合せ系は `-` / 順列系は `→` で内部分割）
- 列 2: 払戻金額（同様に `br` 区切り）

preprocessed_data（出力 dict）のキーは BetType の値（"tansho" 等）と完全一致する。
これを Simulator / BettingTickets 側でそのまま受ける。
"""

from __future__ import annotations

import logging

import pandas as pd

from src.constants._bet_types import BetType
from src.preprocessing._abstract_data_processor import AbstractDataProcessor

logger = logging.getLogger(__name__)


# raw_data 内の列インデックス（HTML スクレイプ由来の固定構造）
_RAW_CATEGORY_COL = 0
_RAW_WIN_COL = 1
_RAW_RETURN_COL = 2

# 内部識別子（BetType）と HTML 表示ラベル（日本語）の対応
_LABEL = {
    BetType.TANSHO: "単勝",
    BetType.FUKUSHO: "複勝",
    BetType.WAKUREN: "枠連",
    BetType.UMAREN: "馬連",
    BetType.UMATAN: "馬単",
    BetType.WIDE: "ワイド",
    BetType.SANRENPUKU: "三連複",
    BetType.SANRENTAN: "三連単",
}

# セル内分割文字
_HTML_ROW_SEP = "br"  # 1 セル内で複数払戻がある場合の行区切り（<br> 由来）
_COMBO_SEP = "-"  # 馬連/枠連/ワイド/三連複: 順序なしの馬番区切り
_ORDERED_SEP = "→"  # 馬単/三連単: 順序ありの馬番区切り


def count_br(df, column):
    """指定列に含まれる行区切り文字 `br` の出現数（最大値）を返す。"""
    return max([cell.count(_HTML_ROW_SEP) if isinstance(cell, str) else 0 for cell in df[column]])


def convert_to_int(s):
    """カンマ区切りの数字文字列を整数化する。"""
    if isinstance(s, str):
        s = s.replace(",", "")
    return int(s)


def split_bar_to_int(s):
    """`-` 区切りの馬番文字列を int リストへ。"""
    if isinstance(s, str):
        s = s.split(_COMBO_SEP)
        s = [int(num) for num in s]
    return s


def split_arrow_to_int(s):
    """`→` 区切りの馬番文字列を int リストへ（順序保持）。"""
    if isinstance(s, str):
        s = s.split(_ORDERED_SEP)
        s = [int(num) for num in s]
    return s


def sort_tuple(tup):
    """タプルの中身をソートする関数。"""
    return tuple(sorted(tup))


class ReturnProcessor(AbstractDataProcessor):
    def __init__(self, filepath):
        super().__init__(filepath)

    def _preprocess(self):
        return_dict = {}
        return_dict[BetType.TANSHO] = self.__tansho()
        return_dict[BetType.WAKUREN] = self.__wakuren()
        return_dict[BetType.FUKUSHO] = self.__fukusho()
        return_dict[BetType.UMAREN] = self.__umaren()
        return_dict[BetType.UMATAN] = self.__umatan()
        return_dict[BetType.WIDE] = self.__wide()
        return_dict[BetType.SANRENTAN] = self.__sanrentan()
        return_dict[BetType.SANRENPUKU] = self.__sanrenpuku()
        return return_dict

    def __tansho(self):
        tansho = self.raw_data[self.raw_data[_RAW_CATEGORY_COL] == _LABEL[BetType.TANSHO]][
            [_RAW_WIN_COL, _RAW_RETURN_COL, "race_id"]
        ]
        tansho_row_num = count_br(tansho, _RAW_WIN_COL) + 1
        wins = tansho[_RAW_WIN_COL].str.split(_HTML_ROW_SEP, n=tansho_row_num, expand=True)
        wins = wins.fillna(0)
        wins.columns = [f"win_{i}" for i in range(len(wins.columns))]
        for i in range(len(wins.columns)):
            col_name = f"win_{i}"
            wins[col_name] = wins[col_name].apply(convert_to_int)
        return_ = tansho[_RAW_RETURN_COL].str.split(_HTML_ROW_SEP, n=tansho_row_num, expand=True)
        return_ = return_.fillna(0)
        return_.columns = [f"return_{i}" for i in range(len(return_.columns))]
        for i in range(len(return_.columns)):
            col_name = f"return_{i}"
            return_[col_name] = return_[col_name].apply(convert_to_int)
        df = pd.concat([tansho["race_id"], wins, return_], axis=1)
        df.set_index("race_id", inplace=True)
        return df

    def __wakuren(self):
        wakuren = self.raw_data[self.raw_data[_RAW_CATEGORY_COL] == _LABEL[BetType.WAKUREN]][
            [_RAW_WIN_COL, _RAW_RETURN_COL, "race_id"]
        ]
        wakuren_row_num = count_br(wakuren, _RAW_WIN_COL) + 1
        wins = wakuren[_RAW_WIN_COL].str.split(_HTML_ROW_SEP, n=wakuren_row_num, expand=True)
        wins = wins.fillna(0)
        wins.columns = [f"win_{i}" for i in range(len(wins.columns))]
        for i in range(len(wins.columns)):
            col_name = f"win_{i}"
            wins[col_name] = wins[col_name].apply(split_bar_to_int)
        return_ = wakuren[_RAW_RETURN_COL].str.split(_HTML_ROW_SEP, n=wakuren_row_num, expand=True)
        return_ = return_.fillna(0)
        return_.columns = [f"return_{i}" for i in range(len(return_.columns))]
        for i in range(len(return_.columns)):
            col_name = f"return_{i}"
            return_[col_name] = return_[col_name].apply(convert_to_int)
        df = pd.concat([wakuren["race_id"], wins, return_], axis=1)
        df.set_index("race_id", inplace=True)
        return df

    def __fukusho(self):
        fukusho = self.raw_data[self.raw_data[_RAW_CATEGORY_COL] == _LABEL[BetType.FUKUSHO]][
            [_RAW_WIN_COL, _RAW_RETURN_COL, "race_id"]
        ]
        fukusho_row_num = count_br(fukusho, _RAW_WIN_COL) + 1
        logger.debug("複勝列数:%s", fukusho_row_num)
        wins = fukusho[_RAW_WIN_COL].str.split(_HTML_ROW_SEP, n=fukusho_row_num, expand=True)
        wins = wins.fillna(0)
        wins.columns = [f"win_{i}" for i in range(len(wins.columns))]

        return_ = fukusho[_RAW_RETURN_COL].str.split(_HTML_ROW_SEP, n=fukusho_row_num, expand=True)
        return_ = return_.fillna(0)
        return_.columns = [f"return_{i}" for i in range(len(return_.columns))]
        for i in range(len(return_.columns)):
            col_name = f"return_{i}"
            return_[col_name] = return_[col_name].apply(convert_to_int)
        df = pd.concat([fukusho["race_id"], wins, return_], axis=1)
        df.set_index("race_id", inplace=True)
        return df

    def __umaren(self):
        umaren = self.raw_data[self.raw_data[_RAW_CATEGORY_COL] == _LABEL[BetType.UMAREN]][
            [_RAW_WIN_COL, _RAW_RETURN_COL, "race_id"]
        ]
        umaren_row_num = count_br(umaren, _RAW_WIN_COL) + 1
        wins = umaren[_RAW_WIN_COL].str.split(_HTML_ROW_SEP, n=umaren_row_num, expand=True)
        wins = wins.fillna(0)
        wins.columns = [f"win_{i}" for i in range(len(wins.columns))]
        for i in range(len(wins.columns)):
            col_name = f"win_{i}"
            wins[col_name] = wins[col_name].apply(split_bar_to_int)
        return_ = umaren[_RAW_RETURN_COL].str.split(_HTML_ROW_SEP, n=umaren_row_num, expand=True)
        return_ = return_.fillna(0)
        return_.columns = [f"return_{i}" for i in range(len(return_.columns))]
        for i in range(len(return_.columns)):
            col_name = f"return_{i}"
            return_[col_name] = return_[col_name].apply(convert_to_int)
        df = pd.concat([umaren["race_id"], wins, return_], axis=1)
        df.set_index("race_id", inplace=True)
        return df

    def __umatan(self):
        umatan = self.raw_data[self.raw_data[_RAW_CATEGORY_COL] == _LABEL[BetType.UMATAN]][
            [_RAW_WIN_COL, _RAW_RETURN_COL, "race_id"]
        ]
        umatan_row_num = count_br(umatan, _RAW_WIN_COL) + 1
        wins = umatan[_RAW_WIN_COL].str.split(_HTML_ROW_SEP, n=umatan_row_num, expand=True)
        wins = wins.fillna(0)
        wins.columns = [f"win_{i}" for i in range(len(wins.columns))]
        for i in range(len(wins.columns)):
            col_name = f"win_{i}"
            wins[col_name] = wins[col_name].apply(split_arrow_to_int)
        return_ = umatan[_RAW_RETURN_COL].str.split(_HTML_ROW_SEP, n=umatan_row_num, expand=True)
        return_ = return_.fillna(0)
        return_.columns = [f"return_{i}" for i in range(len(return_.columns))]
        for i in range(len(return_.columns)):
            col_name = f"return_{i}"
            return_[col_name] = return_[col_name].apply(convert_to_int)
        df = pd.concat([umatan["race_id"], wins, return_], axis=1)
        df.set_index("race_id", inplace=True)
        return df

    def __wide(self):
        wide = self.raw_data[self.raw_data[_RAW_CATEGORY_COL] == _LABEL[BetType.WIDE]][
            [_RAW_WIN_COL, _RAW_RETURN_COL, "race_id"]
        ]
        wide_row_num = count_br(wide, _RAW_WIN_COL) + 1

        wins = wide[_RAW_WIN_COL].str.split(_HTML_ROW_SEP, n=wide_row_num, expand=True)
        wins = wins.fillna(0)
        wins.columns = [f"win_{i}" for i in range(len(wins.columns))]
        for i in range(len(wins.columns)):
            col_name = f"win_{i}"
            wins[col_name] = wins[col_name].apply(split_bar_to_int)

        return_ = wide[_RAW_RETURN_COL].str.split(_HTML_ROW_SEP, n=wide_row_num, expand=True)
        return_ = return_.fillna(0)
        return_.columns = [f"return_{i}" for i in range(len(return_.columns))]
        for i in range(len(return_.columns)):
            col_name = f"return_{i}"
            return_[col_name] = return_[col_name].apply(convert_to_int)
        df = pd.concat([wide["race_id"], wins, return_], axis=1)
        df.set_index("race_id", inplace=True)
        return df

    def __sanrentan(self):
        rentan = self.raw_data[self.raw_data[_RAW_CATEGORY_COL] == _LABEL[BetType.SANRENTAN]][
            [_RAW_WIN_COL, _RAW_RETURN_COL, "race_id"]
        ]
        rentan_row_num = count_br(rentan, _RAW_WIN_COL) + 1
        wins = rentan[_RAW_WIN_COL].str.split(_HTML_ROW_SEP, n=rentan_row_num, expand=True)
        wins = wins.fillna(0)
        wins.columns = [f"win_{i}" for i in range(len(wins.columns))]
        for i in range(len(wins.columns)):
            col_name = f"win_{i}"
            wins[col_name] = wins[col_name].apply(split_arrow_to_int)
        return_ = rentan[_RAW_RETURN_COL].str.split(_HTML_ROW_SEP, n=rentan_row_num, expand=True)
        return_ = return_.fillna(0)
        return_.columns = [f"return_{i}" for i in range(len(return_.columns))]
        for i in range(len(return_.columns)):
            col_name = f"return_{i}"
            return_[col_name] = return_[col_name].apply(convert_to_int)
        df = pd.concat([rentan["race_id"], wins, return_], axis=1)
        df.set_index("race_id", inplace=True)
        return df

    def __sanrenpuku(self):
        renpuku = self.raw_data[self.raw_data[_RAW_CATEGORY_COL] == _LABEL[BetType.SANRENPUKU]][
            [_RAW_WIN_COL, _RAW_RETURN_COL, "race_id"]
        ]
        renpuku_row_num = count_br(renpuku, _RAW_WIN_COL) + 1
        wins = renpuku[_RAW_WIN_COL].str.split(_HTML_ROW_SEP, n=renpuku_row_num, expand=True)
        wins = wins.fillna(0)
        wins.columns = [f"win_{i}" for i in range(len(wins.columns))]
        for i in range(len(wins.columns)):
            col_name = f"win_{i}"
            wins[col_name] = wins[col_name].apply(split_bar_to_int)

        return_ = renpuku[_RAW_RETURN_COL].str.split(_HTML_ROW_SEP, n=renpuku_row_num, expand=True)
        return_ = return_.fillna(0)
        return_.columns = [f"return_{i}" for i in range(len(return_.columns))]
        for i in range(len(return_.columns)):
            col_name = f"return_{i}"
            return_[col_name] = return_[col_name].apply(convert_to_int)
        df = pd.concat([renpuku["race_id"], wins, return_], axis=1)
        df.set_index("race_id", inplace=True)
        return df
