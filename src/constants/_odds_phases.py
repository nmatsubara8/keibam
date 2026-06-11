"""段階オッズ取得のフェーズ定義（単一の定義元）。

オッズは締切直前まで大きく変動する（AI 参加の増加で顕著）。早期の生オッズで
期待値を計算すると締切時に乖離するため、複数フェーズでスナップショットを取得し、
`minutes_to_post`（締切までの残り分数）とともに保存する。

フェーズはオッズ力学モデルのチェックポイント（発走 30 分前 / 10 分前 / 5 分前 / 直前）
に対応する。スナップショットの冪等 dedup キーが (race_id, bet_type, combo, phase) の
ため、フェーズの細かさ = 時点別に保持される系列の解像度になる。

ここはドメイン知識（どのタイミングで取得するか）の一元化であり、I/O は持たない。
"""

import dataclasses


@dataclasses.dataclass(frozen=True)
class OddsPhase:
    """取得フェーズの識別子。scheduler の --phase 引数と対応する。"""

    PREV_DAY: str = "prev_day"  # 前日
    HOURS_BEFORE: str = "hours_before"  # 数時間前
    THIRTY_MIN: str = "thirty_min"  # 30分前
    T10: str = "t10"  # 10分前
    T5: str = "t5"  # 5分前
    T0: str = "t0"  # 直前（締切 ≒ 確定オッズの代理）
    # 旧フェーズ名（〜2026-06 の粗い分類）。新規分類では使わないが、
    # 既存スナップショットの phase 値として現れうるため定数は残す。
    JUST_BEFORE: str = "just_before"


# 各フェーズが「締切まで何分以内」を対象とするかの上限（分）。
# minutes_to_post がこの値以下になった最初のフェーズに分類する（小さい順に評価）。
PHASE_MAX_MINUTES = {
    OddsPhase.T0: 2,
    OddsPhase.T5: 7,
    OddsPhase.T10: 12,
    OddsPhase.THIRTY_MIN: 45,
    OddsPhase.HOURS_BEFORE: 360,  # 6 時間
    OddsPhase.PREV_DAY: 100000,  # それ以前（前日〜）
}

# 小さい順（締切に近い順）に評価するためのフェーズ列。
PHASE_ORDER = (
    OddsPhase.T0,
    OddsPhase.T5,
    OddsPhase.T10,
    OddsPhase.THIRTY_MIN,
    OddsPhase.HOURS_BEFORE,
    OddsPhase.PREV_DAY,
)

# 時系列順（締切から遠い順）。オッズ力学モデルの系列構築・隣接遷移の定義に使う。
PHASE_TIMELINE = tuple(reversed(PHASE_ORDER))

# 旧フェーズ → 新フェーズの対応（既存データ読込時の正規化用）。
# just_before（≤10 分）は新分類では概ね t10 に相当する。
LEGACY_PHASE_ALIASES = {OddsPhase.JUST_BEFORE: OddsPhase.T10}


def classify_phase(minutes_to_post: float) -> str:
    """締切までの残り分数から取得フェーズを判定する（純粋関数）。

    締切に最も近いフェーズの上限から順に評価し、最初に収まったフェーズを返す。
    負値（既に締切超過）は T0 扱い。
    """
    for phase in PHASE_ORDER:
        if minutes_to_post <= PHASE_MAX_MINUTES[phase]:
            return phase
    return OddsPhase.PREV_DAY


def normalize_phase(phase: str) -> str:
    """旧フェーズ名を新フェーズへ正規化する（既存スナップショット読込用）。"""
    return LEGACY_PHASE_ALIASES.get(phase, phase)
