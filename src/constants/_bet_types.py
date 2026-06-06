"""馬券種の識別子と組合せサイズの単一の定義元。

Simulator / BettingTickets / policies / portfolio が同じ文字列を参照できるよう、
ここに一元化する（マジック文字列の散在を防ぐ）。
"""

import dataclasses


@dataclasses.dataclass(frozen=True)
class BetType:
    TANSHO: str = "tansho"
    FUKUSHO: str = "fukusho"
    UMAREN: str = "umaren"
    UMATAN: str = "umatan"
    WIDE: str = "wide"
    SANRENPUKU: str = "sanrenpuku"
    SANRENTAN: str = "sanrentan"


# 各馬券種で必要な馬の数（組合せのサイズ）
COMBO_SIZE = {
    BetType.TANSHO: 1,
    BetType.FUKUSHO: 1,
    BetType.UMAREN: 2,
    BetType.UMATAN: 2,
    BetType.WIDE: 2,
    BetType.SANRENPUKU: 3,
    BetType.SANRENTAN: 3,
}

# 順序を区別する馬券種（順列で生成する）。それ以外は順不同（組合せ）。
ORDERED = {BetType.UMATAN, BetType.SANRENTAN}
