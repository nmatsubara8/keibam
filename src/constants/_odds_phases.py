"""段階オッズ取得のフェーズ定義（単一の定義元）。

オッズは締切直前まで大きく変動する（AI 参加の増加で顕著）。早期の生オッズで
期待値を計算すると締切時に乖離するため、複数フェーズでスナップショットを取得し、
`minutes_to_post`（締切までの残り分数）とともに保存する。

ここはドメイン知識（どのタイミングで取得するか）の一元化であり、I/O は持たない。
"""

import dataclasses


@dataclasses.dataclass(frozen=True)
class OddsPhase:
    """取得フェーズの識別子。scheduler の --phase 引数と対応する。"""

    PREV_DAY: str = "prev_day"  # 前日
    HOURS_BEFORE: str = "hours_before"  # 数時間前
    THIRTY_MIN: str = "thirty_min"  # 30分前
    JUST_BEFORE: str = "just_before"  # 直前


# 各フェーズが「締切まで何分以内」を対象とするかの上限（分）。
# minutes_to_post がこの値以下になった最初のフェーズに分類する（小さい順に評価）。
PHASE_MAX_MINUTES = {
    OddsPhase.JUST_BEFORE: 10,
    OddsPhase.THIRTY_MIN: 45,
    OddsPhase.HOURS_BEFORE: 360,  # 6 時間
    OddsPhase.PREV_DAY: 100000,  # それ以前（前日〜）
}

# 小さい順（締切に近い順）に評価するためのフェーズ列。
PHASE_ORDER = (
    OddsPhase.JUST_BEFORE,
    OddsPhase.THIRTY_MIN,
    OddsPhase.HOURS_BEFORE,
    OddsPhase.PREV_DAY,
)


def classify_phase(minutes_to_post: float) -> str:
    """締切までの残り分数から取得フェーズを判定する（純粋関数）。

    締切に最も近いフェーズの上限から順に評価し、最初に収まったフェーズを返す。
    負値（既に締切超過）は JUST_BEFORE 扱い。
    """
    for phase in PHASE_ORDER:
        if minutes_to_post <= PHASE_MAX_MINUTES[phase]:
            return phase
    return OddsPhase.PREV_DAY
