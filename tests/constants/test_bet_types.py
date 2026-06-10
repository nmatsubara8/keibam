"""BetType / COMBO_SIZE / ORDERED の整合性テスト。

BetType に登録された全 8 種について、COMBO_SIZE が（少なくとも _bet_policy で
看做される範囲では）参照可能であること、ORDERED は順序を区別するものだけを
含むことを固定する。WAKUREN を後から追加した際の漏れ（過去にあった）を防ぐ。
"""

from __future__ import annotations

import dataclasses

from src.constants._bet_types import COMBO_SIZE
from src.constants._bet_types import ORDERED
from src.constants._bet_types import BetType


def _all_bet_types() -> list[str]:
    return [f.default for f in dataclasses.fields(BetType)]


class TestBetTypeRegistry:
    def test_all_bet_types_have_combo_size(self):
        missing = [bt for bt in _all_bet_types() if bt not in COMBO_SIZE]
        assert missing == [], f"COMBO_SIZE に欠けている馬券種: {missing}"

    def test_wakuren_size_is_two(self):
        assert COMBO_SIZE[BetType.WAKUREN] == 2

    def test_ordered_subset_of_combo_size(self):
        assert ORDERED <= set(COMBO_SIZE.keys())

    def test_ordered_only_contains_umatan_and_sanrentan(self):
        # 順序を区別するのは馬単と三連単のみ（仕様）
        assert ORDERED == {BetType.UMATAN, BetType.SANRENTAN}

    def test_wakuren_not_in_ordered(self):
        assert BetType.WAKUREN not in ORDERED
