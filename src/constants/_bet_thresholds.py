"""期待値ベース馬券選定の既定パラメータ（KB 7.1 / 7.3）。

マジックナンバーをコードに散らさないため、馬券種別の期待値閾値・リスク管理上限を
ここに一元化する。変更時の影響範囲をこのファイルに限定する。
"""

import dataclasses


@dataclasses.dataclass(frozen=True)
class BetThresholds:
    """馬券種ごとの期待値（=確率×オッズ）閾値。KB 7.1 の例値を既定とする。"""

    TANSHO: float = 1.78
    FUKUSHO: float = 1.20
    UMAREN: float = 3.26
    UMATAN: float = 5.00
    WIDE: float = 2.00
    SANRENPUKU: float = 5.00
    SANRENTAN: float = 10.00


@dataclasses.dataclass(frozen=True)
class RiskLimits:
    """リスク管理の上限（KB 7.3）。"""

    # 1レースあたりの投票枚数上限（30枚未満）
    MAX_TICKETS_PER_RACE: int = 30
    # 低確率帯のノイズ足切り（この勝率未満の馬は組合せ生成から除外）
    MIN_WIN_PROB: float = 0.01
    # 複勝の的中圏（n着以内）
    FUKUSHO_PLACES: int = 3
