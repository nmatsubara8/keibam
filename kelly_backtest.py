"""単勝エッジのフラクショナル・ケリー資産成長バックテスト。

validate_edge.py で「単勝は EV 閾値↑で回収率↑（0.86→3.72）・プラセボはフラット」
＝本物のエッジが確認された。本スクリプトは、その確立済みエッジを「賭け方（資金配分）」
で運用したときの資産成長を測る。フラットベット（金額固定）と、確信度スケール付き
フラクショナル・ケリー（src.portfolio._kelly）を、同じモデル・同じ検証区間・同じ実払戻で
比較し、最終資産・対数成長率・最大ドローダウンを出す。

ポイント:
  - 既存の検証は回収率（=払戻/投資）だけを見ており、資金成長や破産リスクは測れない。
  - ケリーは各ベットの個別エッジとオッズに応じて賭け金を最適化し、対数成長率を最大化する。
  - フルケリーは分散が大きいため、推定誤差に対する保険として分数（1/4〜1/2）を併せて掃引する。

レースを時系列順に処理し、各レースで「現在の bankroll」を使って配分→実払戻で複利更新する
（= 真の複利ケリー。配分は bankroll の成長に追従する）。

実行:
  python kelly_backtest.py                       # 最新モデル・直近20%で掃引
  python kelly_backtest.py --version v1           # モデル指定
  python kelly_backtest.py --ev-floor 1.2         # EV 下限（賭ける最低エッジ）
  python kelly_backtest.py --selftest             # 合成データで配線のみ検証（データ不要）
"""

from __future__ import annotations

import argparse
import logging
import math

logger = logging.getLogger(__name__)

# 掃引するケリー比率（None = フラットベット基準）。
_FRACTIONS = [None, 0.1, 0.25, 0.5, 1.0]


def _fmt(x: float, nd: int = 3) -> str:
    return "—" if x is None else f"{x:.{nd}f}"


def _build_candidates(table_1race, race_id, ev_floor: float, ev_max: float):
    """1レース分の score_table 行から単勝 BetCandidate を作る。

    prob はレース内で正規化（Σ=1）した勝率を用いる（EV 診断と整合し、ケリーの p として
    妥当な確率にする）。EV = prob*odds が (ev_floor, ev_max] のものだけ採用する。
    """
    from src.constants._results_cols import ResultsCols
    from src.policies._bet_candidate import BetCandidate
    from src.policies._score_policy import CURRENT_ODDS
    from src.policies._score_policy import PROB

    probs = table_1race[PROB].astype(float)
    total = probs.sum()
    if not (total > 0):
        return []
    out = []
    for _, row in table_1race.iterrows():
        p = float(row[PROB]) / total
        odds = float(row[CURRENT_ODDS])
        if not (math.isfinite(odds) and odds > 0 and 0.0 < p < 1.0):
            continue
        ev = p * odds
        if ev_floor < ev <= ev_max:
            out.append(
                BetCandidate(
                    race_id=race_id,
                    bet_type="tansho",
                    combo=(int(row[ResultsCols.UMABAN]),),
                    probability=p,
                    odds=odds,
                    expected_value=ev,
                )
            )
    return out


def run_backtest(race_groups, settle_fn, optimizer, initial_bankroll: float, flat_unit: float):
    """レースを時系列順に処理し資産推移を返す。

    race_groups : (race_id, candidates) を時系列順に並べた iterable。
    settle_fn   : (race_id, umaban, stake_yen) -> (bet_amount, return_amount)。実払戻で決済。
    optimizer   : None ならフラット（各ベット flat_unit 円固定）、それ以外は allocate(cands, bankroll)。
    """
    bankroll = initial_bankroll
    peak = bankroll
    max_dd = 0.0
    n_bets = n_hits = 0
    total_staked = total_return = 0.0

    for race_id, candidates in race_groups:
        if not candidates or bankroll <= 0:
            continue
        if optimizer is None:
            staked = [(c, float(flat_unit)) for c in candidates]
        else:
            allocated = optimizer.allocate(candidates, bankroll)
            staked = [(c, c.stake) for c in allocated]
        for c, stake in staked:
            stake = int(round(stake))
            if stake <= 0:
                continue
            bet_amount, return_amount = settle_fn(race_id, c.combo[0], stake)
            if bet_amount <= 0:
                continue
            bankroll += return_amount - bet_amount
            total_staked += bet_amount
            total_return += return_amount
            n_bets += 1
            if return_amount > 0:
                n_hits += 1
            peak = max(peak, bankroll)
            max_dd = max(max_dd, peak - bankroll)
            if bankroll <= 0:
                break

    growth = math.log(bankroll / initial_bankroll) if bankroll > 0 else float("-inf")
    return {
        "final_bankroll": bankroll,
        "log_growth": growth,
        "max_drawdown_ratio": (max_dd / peak) if peak > 0 else 0.0,
        "return_rate": (total_return / total_staked) if total_staked > 0 else 0.0,
        "n_bets": n_bets,
        "hit_rate": (n_hits / n_bets) if n_bets else 0.0,
    }


def _print_table(rows):
    print(f"\n{'戦略':<14}{'買い目':>9}{'的中率':>9}{'回収率':>9}"
          f"{'最終資産':>14}{'対数成長':>10}{'最大DD':>9}")
    print("-" * 74)
    for label, r in rows:
        print(f"{label:<14}{r['n_bets']:>9}{_fmt(r['hit_rate']):>9}{_fmt(r['return_rate']):>9}"
              f"{r['final_bankroll']:>14,.0f}{_fmt(r['log_growth']):>10}"
              f"{_fmt(r['max_drawdown_ratio']):>9}")
    print("-" * 74)


def _sorted_race_groups(score_table, ev_floor: float, ev_max: float):
    """score_table を race_id 昇順（=時系列）で (race_id, candidates) に変換。"""
    groups = []
    for race_id in sorted(set(score_table.index)):
        sub = score_table.loc[[race_id]]
        groups.append((race_id, _build_candidates(sub, race_id, ev_floor, ev_max)))
    return groups


def main() -> None:
    from src.constants._logging_config import setup_logging

    setup_logging()
    ap = argparse.ArgumentParser(description="単勝エッジのフラクショナル・ケリー資産成長バックテスト")
    ap.add_argument("--version", default=None, help="モデルのバージョン名（既定は最新）")
    ap.add_argument("--test-frac", type=float, default=0.2)
    ap.add_argument("--bankroll", type=float, default=100000.0, help="初期資金")
    ap.add_argument("--flat-unit", type=float, default=100.0, help="フラット基準の1ベット円")
    ap.add_argument("--ev-floor", type=float, default=1.1, help="賭ける EV 下限（損益分岐は約1.10）")
    ap.add_argument("--ev-max", type=float, default=100.0, help="賭ける EV 上限（超高倍率の除外）")
    ap.add_argument("--per-bet-cap", type=float, default=0.05, help="1ベット上限（bankroll比）")
    ap.add_argument("--max-race-ratio", type=float, default=0.5, help="1レース総投資上限（bankroll比）")
    ap.add_argument("--selftest", action="store_true", help="合成データで配線のみ検証（データ不要）")
    args = ap.parse_args()

    if args.selftest:
        _selftest()
        return

    from app._data_loader import find_model_paths
    from app._data_loader import load_model_by_version
    from app._data_loader import load_model_from_path
    from app._model_compare import recent_race_slice
    from app._model_eval import _load_return_processor
    from app._model_eval import load_featured_data
    from src.policies._score_policy import ExpectedValueScorePolicy
    from src.portfolio._kelly import KellyPortfolioOptimizer
    from src.simulation._betting_tickets import BettingTickets

    featured = load_featured_data()
    rp = _load_return_processor()
    if featured is None or featured.empty or rp is None:
        logger.error("featured_data / return_tables が読み込めません")
        return

    if args.version:
        ai = load_model_by_version(args.version)
        label = args.version
    else:
        paths = find_model_paths("models")
        if not paths:
            logger.error("models/ に学習済みモデル（*_keibam.pickle）がありません")
            return
        ai = load_model_from_path(paths[0])
        label = paths[0]

    test_slice = recent_race_slice(featured, args.test_frac)
    score_table = ai.calc_score(test_slice, ExpectedValueScorePolicy)
    groups = _sorted_race_groups(score_table, args.ev_floor, args.ev_max)
    n_races = len(groups)
    n_cand = sum(len(c) for _, c in groups)

    print("=" * 74)
    print("単勝エッジ フラクショナル・ケリー資産成長バックテスト")
    print("=" * 74)
    print(f"モデル={label} / 検証 {n_races} レース / EV下限={args.ev_floor} / "
          f"初期資金={args.bankroll:,.0f}円 / 候補 {n_cand} 件")

    tickets = BettingTickets(rp)

    def settle_fn(race_id, umaban, stake):
        _, bet_amount, return_amount = tickets.bet_tansho(race_id, [umaban], stake)
        return bet_amount, return_amount

    rows = []
    for fr in _FRACTIONS:
        if fr is None:
            opt = None
            lab = f"フラット{int(args.flat_unit)}円"
        else:
            opt = KellyPortfolioOptimizer(
                kelly_fraction_ratio=fr,
                per_bet_cap_ratio=args.per_bet_cap,
                max_daily_ratio=args.max_race_ratio,
            )
            lab = f"ケリー×{fr}"
        rows.append((lab, run_backtest(groups, settle_fn, opt, args.bankroll, args.flat_unit)))

    _print_table(rows)
    print("\n判定:")
    print(" - ケリーがフラットより対数成長↑かつ最大DDが許容範囲 → 採用価値あり")
    print(" - フルケリー(×1.0)でDDが過大 → 分数(×0.25〜0.5)が実戦的な運用点")
    print("=" * 74)


def _selftest() -> None:
    """合成データで配分→決済→複利の配線を検証（実データ不要、この環境で実行可）。"""
    from src.policies._bet_candidate import BetCandidate
    from src.portfolio._kelly import KellyPortfolioOptimizer

    # 真の勝率 0.5・オッズ 2.5 → EV=1.25 の +エッジ馬を毎レース1頭。勝者は決定的に交互。
    rng_hits = [True, False] * 500  # 的中率50%（真の勝率と一致）

    def make_groups():
        for i in range(1000):
            c = BetCandidate(
                race_id=i, bet_type="tansho", combo=(1,),
                probability=0.5, odds=2.5, expected_value=1.25,
            )
            yield (i, [c])

    hit_map = {i: rng_hits[i] for i in range(1000)}

    def settle_fn(race_id, umaban, stake):
        # 的中なら払戻 = stake*2.5、外れは0。
        return stake, (stake * 2.5 if hit_map[race_id] else 0.0)

    opt = KellyPortfolioOptimizer(kelly_fraction_ratio=0.5, per_bet_cap_ratio=0.05, max_daily_ratio=0.5)
    res_kelly = run_backtest(list(make_groups()), settle_fn, opt, 100000.0, 100.0)
    res_flat = run_backtest(list(make_groups()), settle_fn, None, 100000.0, 100.0)

    print("[selftest] +EV(1.25) 合成市場・的中率50%・1000レース")
    print(f"  フラット : 最終資産 {res_flat['final_bankroll']:>12,.0f}  "
          f"対数成長 {_fmt(res_flat['log_growth'])}  回収率 {_fmt(res_flat['return_rate'])}")
    print(f"  ケリー0.5: 最終資産 {res_kelly['final_bankroll']:>12,.0f}  "
          f"対数成長 {_fmt(res_kelly['log_growth'])}  回収率 {_fmt(res_kelly['return_rate'])}")

    assert res_kelly["n_bets"] == 1000, "全レースで賭けられているはず"
    assert res_flat["return_rate"] > 1.2, "回収率は理論EV(1.25)近傍のはず"
    assert res_kelly["final_bankroll"] > 100000.0, "+EVなのでケリーは増えるはず"
    assert res_kelly["log_growth"] > res_flat["log_growth"], "複利ケリーはフラットより成長が速いはず"
    print("[selftest] PASS — 配分→決済→複利の配線は健全")


if __name__ == "__main__":
    main()
