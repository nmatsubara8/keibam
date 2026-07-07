"""JRA 券種別の標準控除率（takeout）と払戻率の単一定義元。

出典: JRA 公式の標準払戻率（勝馬投票券の種類別）。JRA は幅を持たせた変動控除率を
採用するが、ここでは標準値を用いる。控除率 = 1 − 払戻率。

## 損失最小化における位置づけ

本リポジトリで 35 年データ + 元ネタ動画の全解析により確定した事実:
JRA 単勝市場は効率的（echo≈0.989, ΔR²≈0）で、公開データによる予測では市場を
出し抜けない。よって任意のベットの期待損益は

    E[P&L] = −(実効控除率) × 投資額

に収束する（自分のモデル勝率 p ≈ 市場含意確率 p_mkt のとき EV=p×o ≈ 1−takeout）。

この事実の下で「予測精度」は動かせないが、**券種選択**は動かせる。控除率の低い
券種（単勝/複勝 = 20%）ほど、同じ回転量あたりの期待損失が小さい。三連単（27.5%）は
最も高コスト。損失最小化の第一レバーがこの表である。

注: `_odds_dynamics.TAKEOUT_RATE`（=0.2, 単勝）は Harville 推定オッズ用の単一値。
本表は券種別に拡張したもので、両者は単勝で一致する。実効控除率を払戻実績から
逆算したい場合は `policies/_takeout_calibration` を使う（Harville バイアスも吸収）。
"""

from __future__ import annotations

from src.constants._bet_types import BetType

# 券種 → 標準控除率（takeout）。値が低いほど期待損失が小さい。
# 払戻率(=1-takeout): 単勝/複勝 80.0% / 枠連・馬連・ワイド 77.5% / 馬単・三連複 75.0% / 三連単 72.5%
TAKEOUT: dict[str, float] = {
    BetType.TANSHO: 0.200,
    BetType.FUKUSHO: 0.200,
    BetType.WAKUREN: 0.225,
    BetType.UMAREN: 0.225,
    BetType.WIDE: 0.225,
    BetType.UMATAN: 0.250,
    BetType.SANRENPUKU: 0.250,
    BetType.SANRENTAN: 0.275,
}

# 未知の券種に対する保守的な既定（単勝相当の最小控除率）。
DEFAULT_TAKEOUT = 0.200


def takeout(bet_type: str) -> float:
    """券種の標準控除率を返す（未知券種は DEFAULT_TAKEOUT）。"""
    return TAKEOUT.get(bet_type, DEFAULT_TAKEOUT)


def payout_rate(bet_type: str) -> float:
    """券種の標準払戻率（= 1 − 控除率）を返す。"""
    return 1.0 - takeout(bet_type)


def rank_by_takeout(bet_types=None) -> list[tuple[str, float]]:
    """券種を控除率の昇順（=損失が小さい順）に並べた (券種, 控除率) のリスト。

    bet_types=None のときは TAKEOUT の全券種。単勝/複勝が先頭、三連単が末尾になる。
    """
    items = TAKEOUT.items() if bet_types is None else ((bt, takeout(bt)) for bt in bet_types)
    return sorted(items, key=lambda kv: (kv[1], kv[0]))
