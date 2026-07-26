"""損失最小化フレームワークの数値デモ（データ不要・純算術）。

市場効率は確定済み（echo≈0.989, ΔR²≈0, 元ネタ動画の300%も再現しない見かけ）。
「予測で勝つ」が不可能な以上、操作できるのは損失の出方だけ。本スクリプトは 3 レバー
（券種選択・回転量予算・賭けないゲート）を数値で提示し、運用方針の意思決定を支援する。

エッジは一切主張しない。E[P&L] = (EV−1)×投資額 ≒ −控除率×回転量 を前提に、
「同じ参加をするなら損失をいくらに抑えられるか」を示す。

使い方:
    python verify_loss_minimization.py                 # 既定（損失予算¥2,000）
    python verify_loss_minimization.py --loss-budget 5000
"""

from __future__ import annotations

import argparse
import sys

from src.constants._bet_types import BetType
from src.policies._bet_candidate import BetCandidate
from src.policies._loss_minimization import (
    LossMinimizationConfig,
    LossMinimizingPolicy,
    cheapest_bet_types,
    evaluate_candidate,
    expected_loss,
    turnover_cap_for_loss_budget,
)

_JP = {
    BetType.TANSHO: "単勝",
    BetType.FUKUSHO: "複勝",
    BetType.WAKUREN: "枠連",
    BetType.UMAREN: "馬連",
    BetType.WIDE: "ワイド",
    BetType.UMATAN: "馬単",
    BetType.SANRENPUKU: "三連複",
    BetType.SANRENTAN: "三連単",
}


def _rule(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def lever1_bet_type_selection(turnover: float) -> None:
    _rule(f"レバー1: 券種選択 — 回転量 ¥{turnover:,.0f} あたりの期待損失（控除率順）")
    print(f"{'券種':<8}{'控除率':>8}{'払戻率':>8}{'期待損失':>12}")
    print("-" * 40)
    for bt, t in cheapest_bet_types():
        loss = expected_loss(turnover, bt)
        print(f"{_JP[bt]:<8}{t:>7.1%}{1 - t:>8.1%}{'¥' + format(loss, ',.0f'):>12}")
    print("\n→ 単勝/複勝(控除20%)が最小コスト。三連単(27.5%)は単勝の約1.4倍の期待損失。")
    print("  『どの券種を買うか』だけで、同じ参加でも期待損失は 20%〜27.5% の幅で動く。")


def lever2_turnover_budget(loss_budget: float) -> None:
    _rule(f"レバー2: 回転量予算 — 許容期待損失 ¥{loss_budget:,.0f} に対する上限回転量")
    print(f"{'券種':<8}{'控除率':>8}{'上限回転量':>14}{'(予算/控除率)':>16}")
    print("-" * 48)
    for bt, t in cheapest_bet_types():
        cap = turnover_cap_for_loss_budget(loss_budget, bt)
        print(f"{_JP[bt]:<8}{t:>7.1%}{'¥' + format(cap, ',.0f'):>14}{'= ' + format(loss_budget, ',.0f') + ' / ' + format(t, '.3f'):>16}")
    print("\n→ 『いくらまで負けてよいか(予算)』を先に決め、控除率で割れば上限回転量が出る。")
    print("  予算を守る限り、期待損失はどの券種でも予算どおり。低控除ほど多く回せる。")
    print("  ※ 既定の合理解は turnover=0（賭けない）。予算はあくまで娯楽参加の許容枠。")


def lever3_gate_default_no_bet() -> None:
    _rule("レバー3: 『賭けない』を既定とするゲート — 後付け最適化を構造的に禁止")

    cfg = LossMinimizationConfig(ev_safety_margin=0.05)  # 必要EV=1.05
    # 代表的な候補（すべて市場並走モデルの出力を想定）
    samples = [
        ("単勝・EV0.83(市場並走)", BetType.TANSHO, 0.83, 0.30, True),
        ("単勝・EV1.10・OOS閾値", BetType.TANSHO, 1.10, 0.28, True),
        ("単勝・EV1.30・in-sample閾値", BetType.TANSHO, 1.30, 0.25, False),
        ("三連単・EV3.0・OOS閾値", BetType.SANRENTAN, 3.0, 0.002, True),
        ("複勝・EV1.08・OOS閾値", BetType.FUKUSHO, 1.08, 0.55, True),
    ]
    print(f"{'候補':<28}{'判定':>6}  理由")
    print("-" * 72)
    for label, bt, ev, prob, oos in samples:
        c = BetCandidate(
            race_id="demo", bet_type=bt, combo=(1,), probability=prob, odds=ev / prob,
            expected_value=ev,
        )
        res = evaluate_candidate(c, cfg, threshold_is_oos=oos)
        mark = "賭ける" if res.allowed else "見送り"
        print(f"{label:<28}{mark:>6}  {res.reason}")
    print("\n→ 既定は DENY。EV≤必要値・高控除券種・in-sample閾値・極小確率は自動で見送り。")
    print("  特に『in-sample閾値』を無条件で弾く点が要（300%の主因＝閾値の後付け最適化の封じ）。")


def summary(loss_budget: float) -> None:
    _rule("まとめ: 損失最小化の運用方針（エッジ非依存）")
    policy = LossMinimizingPolicy(LossMinimizationConfig(ev_safety_margin=0.05))
    print("採用ポリシー設定:")
    print(f"  許可券種      : {[_JP[b] for b in policy.config.allowed_bet_types]}（低控除のみ）")
    print(f"  上限控除率    : {policy.config.max_takeout:.1%}")
    print(f"  必要EV        : {policy.config.ev_bar + policy.config.ev_safety_margin:.2f}（=1.0+安全余裕）")
    print(f"  OOS閾値必須   : {policy.config.require_oos_threshold}（後付け最適化を禁止）")
    print("\n損失予算に対する券種別の上限回転量:")
    for row in policy.loss_budget_report(loss_budget):
        print(f"  {_JP[row['bet_type']]:<6} 控除{row['takeout']:.1%}  上限回転 ¥{row['turnover_cap']:,.0f}"
              f"  期待損失 ¥{row['expected_loss_at_cap']:,.0f}")
    print("\n結論:")
    print("  ・予測で市場は破れない → 期待損益は必ずマイナス（≈ −控除率×回転量）。")
    print("  ・唯一の合理的最適化は『損失の最小化』: 低控除券種・回転量上限・賭けない既定。")
    print("  ・資金配分はエッジ≈0ゆえケリー→0。賭けるならフラクショナル+上限でDD抑制"
          "（portfolio/_kelly に委譲）。")


def main() -> int:
    ap = argparse.ArgumentParser(description="損失最小化フレームワークの数値デモ")
    ap.add_argument("--loss-budget", type=float, default=2000.0, help="許容期待損失（円）")
    ap.add_argument("--turnover", type=float, default=10000.0, help="レバー1の例示回転量（円）")
    args = ap.parse_args()

    print("市場効率下の損失最小化 — 3レバーの数値デモ（エッジ非依存・データ不要）")
    lever1_bet_type_selection(args.turnover)
    lever2_turnover_budget(args.loss_budget)
    lever3_gate_default_no_bet()
    summary(args.loss_budget)
    return 0


if __name__ == "__main__":
    sys.exit(main())
