"""馬券種ごとの購入計算と的中判定を Strategy パターンで実装する。

## 設計

`BettingTickets` はファサードとして従来の 8 メソッド（dispatch 表が参照する公開 API）を
維持しつつ、内部では `_BettingStrategy` のサブクラスへ委譲する。

- `_SingleStrategy`: 馬番単体で照合（単勝・複勝）
- `_ComboStrategy`: 組合せ（sorted tuple）で照合（馬連・ワイド・三連複 BOX）
- `_PermStrategy`: 順列（ordered tuple）で照合（馬単 BOX・三連単 BOX）
- `_WakurenStrategy`: 重複あり組合せ＋既存の `range(length-1)` クセを保持

リファクタ前の挙動を 100% 維持することを優先する（quirk は意図せず修正しない）。
回帰ガードは `tests/simulation/test_betting_tickets.py`。
"""

from __future__ import annotations

import logging
from abc import ABC
from abc import abstractmethod
from itertools import combinations
from itertools import combinations_with_replacement
from itertools import permutations
from typing import Sequence

import pandas as pd

from src.constants._bet_types import BetType
from src.constants._units import PAYOUT_UNIT_YEN
from src.preprocessing._return_processor import ReturnProcessor

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Strategy 階層（モジュール内部実装）
# ---------------------------------------------------------------------------


class _BettingStrategy(ABC):
    """購入券生成→照合→払戻計算の共通アルゴリズム。"""

    min_horses: int = 1

    def __init__(self, return_table: pd.DataFrame) -> None:
        self._table = return_table

    def place(self, race_id, umaban, amount: int):
        if umaban is None or len(umaban) < self.min_horses:
            logger.warning("betting_tickets: 例外 umaban=%s", umaban)
            return 0, 0, 0
        keys = self._expand(umaban)
        n_bets = len(keys)
        if n_bets == 0:
            return 0, 0, 0
        bet_amount = n_bets * amount
        return_amount = self._sum_returns(race_id, keys, amount)
        return n_bets, bet_amount, return_amount

    def _sum_returns(self, race_id, keys, amount: int) -> float:
        table_1R = self._table.loc[race_id]
        total = 0.0
        for i in range(self._loop_bound(len(table_1R) // 2)):
            win_value = table_1R[f"win_{i}"]
            if not self._is_valid_entry(win_value):
                continue
            for key in keys:
                if self._match(win_value, key):
                    total += table_1R[f"return_{i}"] * amount / PAYOUT_UNIT_YEN
        return total

    # サブクラスのフック ------------------------------------------------------

    def _loop_bound(self, length: int) -> int:
        """払戻テーブル走査の上限（既定は length そのもの）。"""
        return length

    def _is_valid_entry(self, win_value) -> bool:
        """`win_X` の値が「エントリあり」を示すか。"""
        return win_value != 0

    @abstractmethod
    def _expand(self, umaban: Sequence) -> list:
        """購入券のキー列を生成（int / sorted tuple / ordered tuple）。"""

    @abstractmethod
    def _match(self, win_value, key) -> bool:
        """`win_X` セルと購入券キーが一致するか。"""


class _SingleStrategy(_BettingStrategy):
    """単勝/複勝: 馬番単体での照合。`n_bets = len(umaban)`。"""

    def _expand(self, umaban):
        return list(umaban)

    def _match(self, win_value, key) -> bool:
        s = str(win_value).strip()
        # スペース区切りで複数値が混入している場合は最初の値を使う
        if " " in s:
            s = s.split()[0]
        return int(s) == key if s and s != "0" else False


class _ComboStrategy(_BettingStrategy):
    """馬連/ワイド/三連複 BOX: 順序なし組合せ（sorted tuple）。"""

    def __init__(self, return_table: pd.DataFrame, size: int) -> None:
        super().__init__(return_table)
        self._size = size
        self.min_horses = size

    def _expand(self, umaban):
        return list(combinations(umaban, self._size))

    def _match(self, win_value, key) -> bool:
        return tuple(sorted(win_value)) == key


class _PermStrategy(_BettingStrategy):
    """馬単 BOX/三連単 BOX: 順序あり順列（ordered tuple）。"""

    def __init__(self, return_table: pd.DataFrame, size: int) -> None:
        super().__init__(return_table)
        self._size = size
        self.min_horses = size

    def _expand(self, umaban):
        return list(permutations(umaban, self._size))

    def _match(self, win_value, key) -> bool:
        return tuple(win_value) == key


class _WakurenStrategy(_BettingStrategy):
    """枠連 BOX: 同枠（重複あり）対応のため combinations_with_replacement + dedup。

    既存の挙動を保持するため:
    - 走査ループは `range(length - 1)`（最終列を読まない）
    - 照合は `tuple(win_value)`（既に正順で格納されている前提・sorted しない）
    """

    min_horses = 2

    def _expand(self, wakuban):
        possible = combinations_with_replacement(wakuban, 2)
        unique = {tuple(sorted(c)) for c in possible}
        return list(unique)

    def _loop_bound(self, length: int) -> int:
        return length - 1

    def _is_valid_entry(self, win_value) -> bool:
        # 元実装は `tuple(win)` を真偽判定（空タプルは False）。再現する。
        return bool(tuple(win_value))

    def _match(self, win_value, key) -> bool:
        return tuple(win_value) == key


# ---------------------------------------------------------------------------
# ファサード（公開 API）
# ---------------------------------------------------------------------------


class BettingTickets:
    """馬券の買い方と、賭けた時のリターンを計算する（Strategy ディスパッチ）。"""

    def __init__(self, returnProcessor: ReturnProcessor) -> None:
        tables = returnProcessor.preprocessed_data
        self._tansho = _SingleStrategy(tables[BetType.TANSHO])
        self._fukusho = _SingleStrategy(tables[BetType.FUKUSHO])
        self._wakuren = _WakurenStrategy(tables[BetType.WAKUREN])
        self._umaren = _ComboStrategy(tables[BetType.UMAREN], size=2)
        self._umatan_box = _PermStrategy(tables[BetType.UMATAN], size=2)
        self._wide = _ComboStrategy(tables[BetType.WIDE], size=2)
        self._sanrenpuku = _ComboStrategy(tables[BetType.SANRENPUKU], size=3)
        self._sanrentan_box = _PermStrategy(tables[BetType.SANRENTAN], size=3)

    def bet_tansho(self, race_id: int, umaban: list, amount: int):
        return self._tansho.place(race_id, umaban, amount)

    def bet_fukusho(self, race_id: int, umaban: list, amount: int):
        return self._fukusho.place(race_id, umaban, amount)

    def bet_wakuren_box(self, race_id: int, wakuban: list, amount: int):
        return self._wakuren.place(race_id, wakuban, amount)

    def bet_umaren_box(self, race_id: int, umaban: list, amount: int):
        return self._umaren.place(race_id, umaban, amount)

    def bet_umatan_box(self, race_id: int, umaban: list, amount: int):
        return self._umatan_box.place(race_id, umaban, amount)

    def bet_wide_box(self, race_id: int, umaban: list, amount: int):
        return self._wide.place(race_id, umaban, amount)

    def bet_sanrenpuku_box(self, race_id: str, umaban: list, amount: int):
        return self._sanrenpuku.place(race_id, umaban, amount)

    def bet_sanrentan_box(self, race_id: str, umaban: list, amount: int):
        return self._sanrentan_box.place(race_id, umaban, amount)
