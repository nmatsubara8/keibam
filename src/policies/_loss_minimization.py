"""市場効率下での損失最小化ポリシー（純粋ロジック）。

## 前提（本リポジトリで確定済み）

JRA 単勝市場は効率的（echo≈0.989, ΔR²≈0）。公開データによる予測では市場を
出し抜けない。よって任意のベットの期待損益は

    E[P&L] = (EV − 1) × 投資額,   EV = 的中確率 × オッズ ≈ 1 − 控除率

に収束し、素の期待損失率は控除率に等しい。「予測で勝つ」は不可能なので、操作できる
レバーは次の 3 つだけ:

1. **券種選択** — 控除率の低い券種（単勝/複勝 20%）を選び、高い券種（三連単 27.5%）を
   避ける。同じ回転量あたりの期待損失を下げる。
2. **回転量 (turnover)** — 賭けなければ損失 0。**既定は「賭けない」**。許容損失予算から
   上限回転量 = 予算 / 控除率 を導く。
3. **資金配分** — エッジ≈0 ならケリー→0。賭ける場合もフラクショナル+上限でドローダウンを
   抑える（既存 `portfolio/_kelly` に委譲）。

本モジュールはエッジを主張しない。損失を「確実に生じるコスト」として最小化する運用設計で
あり、300%等の見かけの利益を生む**買い目閾値の後付け最適化を構造的に禁止する**
（`require_oos_threshold`）のが要点。

レイヤ: policies（ドメイン）。I/O・グローバル状態を持たない純粋関数/DTO。
"""

from __future__ import annotations

import dataclasses
from collections import defaultdict

from src.constants._bet_types import BetType
from src.constants._takeout import rank_by_takeout, takeout

# ---------------------------------------------------------------------------
# レバー1・2: 期待損失と回転量予算（純粋な算術）
# ---------------------------------------------------------------------------


def expected_pnl_rate(expected_value: float) -> float:
    """単位投資あたりの期待損益率 = EV − 1（負なら損失、正ならエッジ）。"""
    return float(expected_value) - 1.0


def market_loss_rate(bet_type: str) -> float:
    """市場並走（エッジ無し, EV≈1−控除率）での期待損失率 = 控除率。"""
    return takeout(bet_type)


def expected_loss(turnover: float, bet_type: str) -> float:
    """市場並走で回転量 ``turnover`` を賭けたときの期待損失（正の数）。"""
    return float(turnover) * takeout(bet_type)


def turnover_cap_for_loss_budget(loss_budget: float, bet_type: str) -> float:
    """許容期待損失 ``loss_budget`` を超えない上限回転量 = 予算 / 控除率。

    例: 単勝(控除0.2)で「期待損失 ¥2,000 まで許容」→ 上限回転量 ¥10,000。
    三連単(0.275)なら同予算で ¥7,273 しか回せない（高コストゆえ回転を抑える）。
    """
    t = takeout(bet_type)
    if t <= 0:
        return float("inf")
    return float(loss_budget) / t


def cheapest_bet_types(bet_types=None) -> list[tuple[str, float]]:
    """控除率の昇順（損失が小さい順）の (券種, 控除率)。単勝/複勝が先頭。"""
    return rank_by_takeout(bet_types)


# ---------------------------------------------------------------------------
# レバー1+ゲート: 「賭けない」を既定とする買い目ゲート
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class LossMinimizationConfig:
    """損失最小化ゲートの設定（既定は最も保守的 = ほぼ賭けない）。

    Attributes
    ----------
    allowed_bet_types : 許可する券種。既定は低控除の単勝/複勝のみ。
    max_takeout : これを超える控除率の券種を除外（既定 0.20 = 単勝/複勝相当まで）。
    ev_bar : EV の下限。これ以下（含む）の馬券は賭けない。既定 1.0（=期待値マイナスを排除）。
    ev_safety_margin : ev_bar への上乗せ。推定ノイズを吸収する余裕。**OOS で検証した値のみ**
        使うこと（in-sample で決めた margin は退化する。Benter §5 / 本調査の300%の主因）。
    min_probability : 極小確率（テール分散が大きく較正も不安定な帯）を除外する下限。
    require_oos_threshold : True のとき、閾値が OOS 由来でなければ**一切賭けない**。
        テスト集合上で回収率を最大化した閾値（後付け最適化）を運用禁止にする構造的ガード。
    """

    allowed_bet_types: tuple = (BetType.TANSHO, BetType.FUKUSHO)
    max_takeout: float = 0.20
    ev_bar: float = 1.0
    ev_safety_margin: float = 0.0
    min_probability: float = 0.0
    require_oos_threshold: bool = True


@dataclasses.dataclass(frozen=True)
class GateResult:
    """1 候補のゲート判定（賭けるか否かと理由）。"""

    allowed: bool
    reason: str


def required_ev(config: LossMinimizationConfig) -> float:
    """賭けるために超える必要がある EV = ev_bar + ev_safety_margin。"""
    return config.ev_bar + config.ev_safety_margin


def evaluate_candidate(
    candidate,
    config: LossMinimizationConfig,
    *,
    threshold_is_oos: bool,
) -> GateResult:
    """単一 BetCandidate を損失最小化ゲートに通す（既定は DENY）。

    Parameters
    ----------
    candidate : BetCandidate（bet_type/probability/odds/expected_value を持つ）。
    threshold_is_oos : この候補を採用した閾値/パラメータが OOS 検証済みか。
        False かつ config.require_oos_threshold なら無条件で賭けない。

    順に検査し、最初に外れた理由で DENY。全通過で ALLOW。
    """
    bt = candidate.bet_type

    if config.require_oos_threshold and not threshold_is_oos:
        return GateResult(False, "閾値がOOS由来でない（後付け最適化の疑い）→ 賭けない")

    if bt not in config.allowed_bet_types:
        return GateResult(False, f"許可券種外（{bt}）→ 低控除券種に限定")

    if takeout(bt) > config.max_takeout:
        return GateResult(False, f"控除率 {takeout(bt):.1%} > 上限 {config.max_takeout:.1%}")

    if candidate.probability < config.min_probability:
        return GateResult(
            False, f"的中確率 {candidate.probability:.3f} < 下限 {config.min_probability:.3f}（テール除外）"
        )

    need = required_ev(config)
    if candidate.expected_value <= need:
        return GateResult(
            False, f"EV {candidate.expected_value:.3f} ≤ 必要 {need:.3f}（期待損失ゆえ賭けない）"
        )

    return GateResult(True, f"通過: EV {candidate.expected_value:.3f} > {need:.3f}, 控除 {takeout(bt):.1%}")


def filter_candidates(
    candidates,
    config: LossMinimizationConfig,
    *,
    threshold_is_oos: bool,
) -> tuple[list, list[tuple[object, GateResult]]]:
    """候補群をゲートに通し、(採用リスト, [(候補, 判定)] の全記録) を返す。"""
    allowed, records = [], []
    for c in candidates:
        res = evaluate_candidate(c, config, threshold_is_oos=threshold_is_oos)
        records.append((c, res))
        if res.allowed:
            allowed.append(c)
    return allowed, records


def prefer_lowest_takeout_per_race(candidates) -> list:
    """レースごとに、通過候補のうち控除率が最小の券種だけを残す（同点は EV 最大）。

    損失最小化のタイブレーク: 同じ賭ける機会なら、より低コストの券種 1 本に集約する。
    レース単位で最も控除の低い券種群のみを採用（例: 単勝と三連単が両方通っても単勝を選ぶ）。
    """
    by_race: dict[object, list] = defaultdict(list)
    for c in candidates:
        by_race[c.race_id].append(c)

    out: list = []
    for _race, cs in by_race.items():
        min_t = min(takeout(c.bet_type) for c in cs)
        best = [c for c in cs if takeout(c.bet_type) == min_t]
        best.sort(key=lambda c: c.expected_value, reverse=True)
        out.extend(best)
    return out


# ---------------------------------------------------------------------------
# 統合: レバー1+2+3 を束ねるポリシー
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class LossMinimizingPolicy:
    """ゲート（券種/閾値）→ 低控除優先 → 回転量予算の順に適用する運用ポリシー。

    資金配分（レバー3）は既存 `portfolio/_kelly` に委譲する想定。本ポリシーは「どれを
    賭けるか（賭けないか）」と「総回転量の上限」までを決め、確信度/ケリーは後段に渡す。
    """

    config: LossMinimizationConfig = LossMinimizationConfig()

    def select(self, candidates, *, threshold_is_oos: bool) -> list:
        """賭けるべき候補を返す（既定は空=賭けない）。"""
        allowed, _ = filter_candidates(candidates, self.config, threshold_is_oos=threshold_is_oos)
        return prefer_lowest_takeout_per_race(allowed)

    def loss_budget_report(self, loss_budget: float) -> list[dict]:
        """許容損失予算に対する券種別の上限回転量と控除率の一覧（意思決定支援）。"""
        rows = []
        for bt, t in cheapest_bet_types(self.config.allowed_bet_types):
            rows.append(
                {
                    "bet_type": bt,
                    "takeout": t,
                    "turnover_cap": turnover_cap_for_loss_budget(loss_budget, bt),
                    "expected_loss_at_cap": loss_budget,
                }
            )
        return rows
