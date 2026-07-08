"""「一撃」を狙う分散設計（bold play）— 損失最小化の数学的双対（純粋ロジック）。

## 位置づけ

`_loss_minimization` は **E[損失] を最小化**する（最適解 turnover=0＝賭けない）。本モジュールは
その双対で、**E[P&L]<0 を受け入れた上で「元手 B から目標 T に一度でも到達する確率を最大化」**する。
これは分散を最大化する設計（Dubins–Savage の bold play）であり、**エッジは一切主張しない**。
参加は娯楽であり、期待損益は常に −控除率。

## 核心（市場が fair-but-for-takeout: o = (1−t)/p のとき）

目標倍率 m = T/B に対し、

    1点集中(1発)で到達する確率 = (1−t) / m
    k 脚パーレイ(都度 let-it-ride)で到達する確率 = (1−t)^k / m

脚（bet 回数）を増やすほど控除が **累乗** で効くので到達確率は下がる。したがって **一撃の最適設計は
「1点集中・目標倍率ちょうどのオッズ（過剰に狙わない）」**。E[P&L] はどの設計でも −控除率で不変で、
操作できるのは「分布の形（＝到達確率と外れ確率のトレードオフ）」だけ。

レイヤ: policies（ドメイン）。I/O・乱数・グローバル状態を持たない純粋関数/DTO。
"""

from __future__ import annotations

import dataclasses
import math

DEFAULT_TAKEOUT = 0.20


# ---------------------------------------------------------------------------
# 不利ゲームの基本量
# ---------------------------------------------------------------------------


def is_subfair(win_prob: float, odds: float) -> bool:
    """p·o < 1（控除ぶん不利）なら True。効率市場のベットは常に subfair。"""
    return float(win_prob) * float(odds) < 1.0


def expected_pnl_rate(win_prob: float, odds: float) -> float:
    """単位投資あたり期待損益率 = p·o − 1（subfair なら負）。"""
    return float(win_prob) * float(odds) - 1.0


def fair_win_prob(odds: float, takeout: float = DEFAULT_TAKEOUT) -> float:
    """market が fair-but-for-takeout のときの含意勝率 p = (1−t)/o。"""
    return (1.0 - takeout) / float(odds)


def target_multiple(bankroll: float, target: float) -> float:
    """目標倍率 m = T / B（T>B を想定）。"""
    return float(target) / float(bankroll)


# ---------------------------------------------------------------------------
# 一撃の到達確率（閉形式）— market が fair-but-for-takeout 前提
# ---------------------------------------------------------------------------


def single_shot_reach_prob(multiple: float, takeout: float = DEFAULT_TAKEOUT) -> float:
    """1点集中（目標倍率 m のオッズに元手を全部）で T に到達する確率 = (1−t)/m。

    オッズ o=m を選べば当たりで payoff=o·B=T。当たる確率 p=(1−t)/m。m を大きく狙うほど確率は反比例で低下。
    """
    m = float(multiple)
    if m <= 0:
        return 0.0
    return max(0.0, min(1.0, (1.0 - takeout) / m))


def parlay_reach_prob(multiple: float, legs: int, takeout: float = DEFAULT_TAKEOUT) -> float:
    """k 脚パーレイ（等倍 m^(1/k) を都度 let-it-ride）で T に到達する確率 = (1−t)^k / m。

    脚を増やすほど (1−t)^k で控除が累乗に効き、到達確率は単調に下がる。
    """
    m = float(multiple)
    k = int(legs)
    if m <= 0 or k < 1:
        return 0.0
    return max(0.0, min(1.0, (1.0 - takeout) ** k / m))


def reach_prob_at_odds(bankroll: float, target: float, odds: float,
                       takeout: float = DEFAULT_TAKEOUT) -> float:
    """menu 上の単一オッズ o に1点集中したときの到達確率（o≥m 必須。無理なら 0）。

    o≥m なら当たりで o·B≥T。到達確率 = 含意勝率 (1−t)/o。**o を必要最小（≈m）にするほど高い**
    （過剰なオッズを狙うと当たり確率が下がって損）。
    """
    m = target_multiple(bankroll, target)
    if float(odds) < m:
        return 0.0
    return max(0.0, min(1.0, (1.0 - takeout) / float(odds)))


def optimal_legs() -> int:
    """一撃の到達確率を最大化する脚数は常に 1（脚を増やすほど控除が累乗で効くため）。"""
    return 1


def parlay_depth_for_target(multiple: float, per_leg_odds: float) -> int:
    """1脚のオッズが per_leg_odds のとき、倍率 m に届くのに必要な連勝数 = ceil(log m / log o)。"""
    m = float(multiple)
    o = float(per_leg_odds)
    if m <= 1:
        return 0
    if o <= 1:
        return math.inf  # type: ignore[return-value]
    return math.ceil(math.log(m) / math.log(o))


# ---------------------------------------------------------------------------
# staking: bet-to-target（当たりで丁度 T に届く賭け金）
# ---------------------------------------------------------------------------


def bet_to_target_stake(bankroll: float, target: float, odds: float, unit: float = 100.0) -> float:
    """当たりで丁度 T に到達する賭け金 = (T−B)/(o−1)。元手を超えるなら all-in（=B）。unit に丸める。

    Dubins–Savage の bold play を一般オッズに拡張した staking。オーバーシュート（T超）を避けて
    当たり確率（=賭け金が小さいほど…ではなく、当たり確率は o で決まる）を無駄にしない。
    """
    B, T, o = float(bankroll), float(target), float(odds)
    if o <= 1.0 or T <= B:
        return 0.0
    need = (T - B) / (o - 1.0)
    stake = min(B, need)
    return max(unit, math.floor(stake / unit) * unit) if stake >= unit else stake


# ---------------------------------------------------------------------------
# 設計レポート
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class BoldPlayDesign:
    """一撃設計の要約（元手・目標・控除から導く到達確率と期待損益）。"""

    bankroll: float
    target: float
    takeout: float = DEFAULT_TAKEOUT

    @property
    def multiple(self) -> float:
        return target_multiple(self.bankroll, self.target)

    @property
    def required_odds(self) -> float:
        """1点集中で届くのに必要なオッズ（=目標倍率 m）。"""
        return self.multiple

    @property
    def single_shot_prob(self) -> float:
        return single_shot_reach_prob(self.multiple, self.takeout)

    def parlay_prob(self, legs: int) -> float:
        return parlay_reach_prob(self.multiple, legs, self.takeout)

    @property
    def expected_pnl_rate(self) -> float:
        """設計に依らず期待損益率 = −控除率（分散を変えても平均は不変）。"""
        return -self.takeout

    def report(self) -> dict:
        return {
            "bankroll": self.bankroll,
            "target": self.target,
            "multiple": self.multiple,
            "required_odds_single_shot": self.required_odds,
            "single_shot_reach_prob": self.single_shot_prob,
            "parlay2_reach_prob": self.parlay_prob(2),
            "parlay4_reach_prob": self.parlay_prob(4),
            "optimal_legs": optimal_legs(),
            "expected_pnl_rate": self.expected_pnl_rate,
        }
