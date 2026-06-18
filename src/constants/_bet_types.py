"""馬券種の識別子と組合せサイズの単一の定義元。

Simulator / BettingTickets / policies / portfolio が同じ文字列を参照できるよう、
ここに一元化する（マジック文字列の散在を防ぐ）。
"""

import dataclasses


@dataclasses.dataclass(frozen=True)
class BetType:
    TANSHO: str = "tansho"
    FUKUSHO: str = "fukusho"
    WAKUREN: str = "wakuren"
    UMAREN: str = "umaren"
    UMATAN: str = "umatan"
    WIDE: str = "wide"
    SANRENPUKU: str = "sanrenpuku"
    SANRENTAN: str = "sanrentan"


# 各馬券種で必要な馬の数（組合せのサイズ）。
# 注: WAKUREN は「枠」(2 頭以上を含むグループ) 単位の組合せだが、組合せサイズ自体は
# 2（2 つの枠）。Harville 由来の確率計算では未対応（src/policies/_harville.py）のため、
# WAKUREN を _bet_policy に渡すと未知の馬券種としてエラーになる。
COMBO_SIZE = {
    BetType.TANSHO: 1,
    BetType.FUKUSHO: 1,
    BetType.WAKUREN: 2,
    BetType.UMAREN: 2,
    BetType.UMATAN: 2,
    BetType.WIDE: 2,
    BetType.SANRENPUKU: 3,
    BetType.SANRENTAN: 3,
}

# 順序を区別する馬券種（順列で生成する）。それ以外は順不同（組合せ）。
# WAKUREN は順序なし（枠の昇順比較）なので含めない。
ORDERED = {BetType.UMATAN, BetType.SANRENTAN}


def canonical_combo(bet_type: str, combo) -> tuple:
    """組合せを照合用の正準タプルに正規化する。

    順不同券種（馬連/ワイド/三連複/枠連）は馬番（枠番）を昇順に並べ替え、
    順序券種（馬単/三連単）は順序を保持する。オッズ実績の lookup キーと
    EV 選定時の combo を一致させるための単一の正規化規則。
    """
    nums = tuple(int(x) for x in combo)
    return nums if bet_type in ORDERED else tuple(sorted(nums))


def combo_key(bet_type: str, combo) -> str:
    """canonical_combo を ``"3-7-11"`` 形式の文字列キーにする（DB/lookup 共通表現）。"""
    return "-".join(str(x) for x in canonical_combo(bet_type, combo))
