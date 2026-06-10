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
    # 期待値の上限（これを超える超高倍率馬券は的中確率が極小のため除外。KB 7.1）
    # 既定は無効（inf）。リスク集中を防ぎたい場合は DEFAULT_EV_MAX を指定する。
    EV_MAX: float = float("inf")


# 超高倍率馬券（三連単オッズ1000倍超など）を除外する EV 上限の推奨値（§7）。
# ExpectedValueBetPolicy の ev_max 引数の既定候補として参照する。
DEFAULT_EV_MAX: float = 50.0

# 統計的に信頼できる最小ベット数（これ未満の閾値点は分散が大きく信頼性が低い。§8）。
MIN_BETS_FOR_RELIABLE_STAT: int = 30


@dataclasses.dataclass(frozen=True)
class TrainingWeights:
    """Layer1 学習時のサンプル重み・不均衡補正パラメータ（§2 / §2h）。

    - SIGMOID_K: EV 境界 sigmoid 重み `1/(1+exp(-k*(EV-EV_CENTER)))` の鋭さ。
      大きいほど EV=1.0 付近で急峻に重みが立ち上がる（KB shard-43）。
    - EV_CENTER: sigmoid の中心。EV=1.0（回収率の損益分岐点）を境界に置く。
    - SCALE_POS_WEIGHT: 正例不足の補正係数（n_negative / n_positive ≈ 15）。
      LightGBM の `scale_pos_weight` / NN の `BCEWithLogitsLoss(pos_weight)` に渡す
      クラスレベルのマクロ補正。EV sigmoid（サンプルレベルのミクロ補正）と直交合成する。
    """

    SIGMOID_K: float = 5.0
    EV_CENTER: float = 1.0
    SCALE_POS_WEIGHT: float = 15.0
