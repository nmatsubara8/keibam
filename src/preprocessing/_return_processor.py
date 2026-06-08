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
from typing import Callable

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
    """指定列に含まれる行区切り文字 `br` の出現数（最大値）を返す。空の場合は 0。"""
    values = [cell.count(_HTML_ROW_SEP) if isinstance(cell, str) else 0 for cell in df[column]]
    return max(values) if values else 0


def convert_to_int(s):
    """カンマ区切りの数字文字列を整数化する。"""
    if isinstance(s, str):
        s = s.replace(",", "").strip()
        # スペース区切りで複数値が混入している場合は最初の値を使う
        if " " in s:
            s = s.split()[0]
    return int(s) if s else 0


def _str_to_int_list(s: str, sep: str) -> list:
    """sep で分割後、各トークンをスペース除去して int 変換する。
    sep で分割しても複数トークンが得られない場合はスペースでも再分割する。
    """
    parts = [p.strip() for p in s.split(sep) if p.strip()]
    result = []
    for p in parts:
        # トークン内にスペースが残る場合（"4 4" 等）はさらにスペースで分割
        for sub in p.split():
            if sub:
                result.append(int(sub))
    return result


def split_bar_to_int(s):
    """`-` 区切りの馬番文字列を int リストへ。"""
    if isinstance(s, str):
        s = _str_to_int_list(s, _COMBO_SEP)
    return s


def split_arrow_to_int(s):
    """`→` 区切りの馬番文字列を int リストへ（順序保持）。"""
    if isinstance(s, str):
        s = _str_to_int_list(s, _ORDERED_SEP)
    return s


class ReturnProcessor(AbstractDataProcessor):
    def __init__(self, filepath):
        super().__init__(filepath)

    def _preprocess(self):
        return {
            BetType.TANSHO:     self._build_bet_df(BetType.TANSHO,     convert_to_int),
            BetType.FUKUSHO:    self._build_bet_df(BetType.FUKUSHO,     None),
            BetType.WAKUREN:    self._build_bet_df(BetType.WAKUREN,     split_bar_to_int),
            BetType.UMAREN:     self._build_bet_df(BetType.UMAREN,      split_bar_to_int),
            BetType.UMATAN:     self._build_bet_df(BetType.UMATAN,      split_arrow_to_int),
            BetType.WIDE:       self._build_bet_df(BetType.WIDE,        split_bar_to_int),
            BetType.SANRENPUKU: self._build_bet_df(BetType.SANRENPUKU,  split_bar_to_int),
            BetType.SANRENTAN:  self._build_bet_df(BetType.SANRENTAN,   split_arrow_to_int),
        }

    def _build_bet_df(self, bet_type: str, win_transform: Callable | None) -> pd.DataFrame:
        """馬券種別 DataFrame を生成する共通ロジック。

        win_transform が None の場合、win 列は文字列のまま保持する（複勝など）。
        """
        subset = self.raw_data[self.raw_data[_RAW_CATEGORY_COL] == _LABEL[bet_type]][
            [_RAW_WIN_COL, _RAW_RETURN_COL, "race_id"]
        ]
        row_num = count_br(subset, _RAW_WIN_COL) + 1
        logger.debug("%s 列数: %s", _LABEL[bet_type], row_num)

        wins = subset[_RAW_WIN_COL].str.split(_HTML_ROW_SEP, n=row_num, expand=True)
        wins = wins.fillna(0)
        wins.columns = [f"win_{i}" for i in range(len(wins.columns))]
        if win_transform is not None:
            for col in wins.columns:
                wins[col] = wins[col].apply(win_transform)

        returns = subset[_RAW_RETURN_COL].str.split(_HTML_ROW_SEP, n=row_num, expand=True)
        returns = returns.fillna(0)
        returns.columns = [f"return_{i}" for i in range(len(returns.columns))]
        for col in returns.columns:
            returns[col] = returns[col].apply(convert_to_int)

        df = pd.concat([subset["race_id"], wins, returns], axis=1)
        df.set_index("race_id", inplace=True)
        return df
