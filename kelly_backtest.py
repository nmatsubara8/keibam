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


def _candidates_by_race(score_table, ev_floor: float, ev_max: float, max_odds: float):
    """検証済み ExpectedValueBetPolicy で単勝候補を選び、(race_id, [cand]) を時系列順に返す。

    買い目選択は validate_edge.py / backtest_bet_type と同一ロジック（Harville 勝率×実オッズ、
    EV 閾値）。本スクリプトは選択は変えず「サイジング（フラット vs ケリー）」だけを差し替える。
    max_odds でオッズ上限フィルタ（EV>1.1 が拾う -EV な人気薄を除外する運用レバー）。
    """
    from src.constants._results_cols import ResultsCols
    from src.policies._bet_policy import ExpectedValueBetPolicy
    from src.policies._odds_provider import HistoricalOddsProvider
    from src.policies._score_policy import CURRENT_ODDS

    odds_provider = HistoricalOddsProvider.from_score_table(
        score_table, ResultsCols.UMABAN, CURRENT_ODDS, takeout=0.2
    )
    policy = ExpectedValueBetPolicy(
        odds_provider, {"tansho": ev_floor}, bet_types=["tansho"], ev_max=ev_max
    )
    by_race: dict = {}
    for cand in policy.select(score_table):
        if cand.odds > max_odds:
            continue
        by_race.setdefault(cand.race_id, []).append(cand)
    return [(rid, by_race[rid]) for rid in sorted(by_race)]


def run_backtest(race_groups, settle_fn, optimizer, initial_bankroll: float, flat_unit: float,
                 max_bet_yen: float = float("inf")):
    """レースを時系列順に処理し資産推移を返す。

    race_groups : (race_id, candidates) を時系列順に並べた iterable。
    settle_fn   : (race_id, umaban, stake_yen) -> (bet_amount, return_amount)。実払戻で決済。
    optimizer   : None ならフラット（各ベット flat_unit 円固定）、それ以外は allocate(cands, bankroll)。
    """
    bankroll = initial_bankroll
    peak = bankroll
    max_dd_ratio = 0.0
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
            # 流動性制約（pari-mutuel プールに無制限には突っ込めない）＝絶対上限で頭打ち。
            # これがないと複利が青天井に発散し float64 でも inf 化する。
            stake = min(float(stake), max_bet_yen)
            if not math.isfinite(stake) or stake < 1:
                continue
            stake = int(round(stake))
            bet_amount, return_amount = settle_fn(race_id, c.combo[0], stake)
            if bet_amount <= 0:
                continue
            bankroll += return_amount - bet_amount
            total_staked += bet_amount
            total_return += return_amount
            n_bets += 1
            if return_amount > 0:
                n_hits += 1
            # 最大ドローダウンは「その時点の peak 比」で取る。絶対額を最終 peak で割ると
            # 複利で peak が巨大化した分だけ過小評価される（旧バグ）。
            if bankroll > peak:
                peak = bankroll
            elif peak > 0:
                max_dd_ratio = max(max_dd_ratio, (peak - bankroll) / peak)
            if bankroll <= 0:
                max_dd_ratio = 1.0
                break

    growth = math.log(bankroll / initial_bankroll) if bankroll > 0 else float("-inf")
    return {
        "final_bankroll": bankroll,
        "log_growth": growth,
        "max_drawdown_ratio": max_dd_ratio,
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


def _make_settle_fn(tickets):
    """単勝決済を float64 で行う overflow-safe な settle_fn を返す。

    bet_tansho を amount=100 で叩いて払戻倍率（=的中なら単勝オッズ、外れ 0）を取り、
    実ステークとの掛け算は Python float64 で行う（払戻テーブルが float32 のため、
    巨大ステークを直接渡すと float32 オーバーフローで infに化けるのを回避）。
    """
    def settle_fn(race_id, umaban, stake):
        n, ba, ra = tickets.bet_tansho(race_id, [umaban], 100)
        if n == 0 or ba == 0:
            return 0.0, 0.0  # 払戻テーブルに無い等 → 賭けなかった扱い
        multiple = float(ra) / 100.0  # 1円あたり払戻（的中=odds, 外れ=0）
        return float(stake), float(stake) * multiple
    return settle_fn


def _print_edge_by_odds(groups, settle_fn, flat_unit: float):
    """フラット100円ベットで、オッズ帯ごとの買い目数・的中率・回収率を出す。

    回収率がどのオッズ帯（人気馬か人気薄か）に由来するかを切り分ける。ケリーは低オッズ
    （人気馬）に厚く賭けるため、エッジが人気薄に偏っているならケリーは不利になる。
    """
    buckets = [(1.0, 3.0), (3.0, 7.0), (7.0, 15.0), (15.0, 50.0), (50.0, float("inf"))]
    agg = {b: {"n": 0, "hit": 0, "stake": 0.0, "ret": 0.0} for b in buckets}
    for race_id, cands in groups:
        for c in cands:
            for lo, hi in buckets:
                if lo <= c.odds < hi:
                    ba, ra = settle_fn(race_id, c.combo[0], flat_unit)
                    if ba <= 0:
                        break
                    a = agg[(lo, hi)]
                    a["n"] += 1
                    a["stake"] += ba
                    a["ret"] += ra
                    if ra > 0:
                        a["hit"] += 1
                    break
    print(f"\n[エッジの所在] オッズ帯別（フラット{int(flat_unit)}円）")
    print(f"  {'オッズ帯':<12}{'買い目':>9}{'的中率':>9}{'回収率':>9}")
    for lo, hi in buckets:
        a = agg[(lo, hi)]
        if a["n"] == 0:
            continue
        hr = a["hit"] / a["n"]
        rr = a["ret"] / a["stake"] if a["stake"] > 0 else 0.0
        hi_s = "∞" if hi == float("inf") else f"{hi:.0f}"
        print(f"  {f'{lo:.0f}–{hi_s}':<12}{a['n']:>9}{_fmt(hr):>9}{_fmt(rr):>9}")


def _print_by_year(groups, settle_fn, flat_unit: float):
    """（オッズ絞り後の）候補を年度別にフラット集計し、回収率の時系列頑健性を見る。

    回収率はスケール不変で複利の発散に影響されないため、年度別の頑健性指標として適切。
    どの年度でも回収率>1なら「特定年の偶然でない本物のエッジ」。
    """
    years: dict = {}
    for race_id, cands in groups:
        y = str(race_id)[:4]
        a = years.setdefault(y, {"n": 0, "hit": 0, "stake": 0.0, "ret": 0.0, "races": set()})
        a["races"].add(race_id)
        for c in cands:
            ba, ra = settle_fn(race_id, c.combo[0], flat_unit)
            if ba <= 0:
                continue
            a["n"] += 1
            a["stake"] += ba
            a["ret"] += ra
            if ra > 0:
                a["hit"] += 1
    print(f"\n[年度別頑健性] フラット{int(flat_unit)}円・オッズ絞り後")
    print(f"  {'年度':<8}{'レース':>8}{'買い目':>9}{'的中率':>9}{'回収率':>9}")
    print("  " + "-" * 43)
    year_rates = []
    for y in sorted(years):
        a = years[y]
        if a["n"] == 0:
            continue
        hr = a["hit"] / a["n"]
        rr = a["ret"] / a["stake"] if a["stake"] > 0 else 0.0
        year_rates.append(rr)
        mark = " ◎" if rr > 1.0 else ""
        print(f"  {y:<8}{len(a['races']):>8}{a['n']:>9}{_fmt(hr):>9}{_fmt(rr):>9}{mark}")
    print("  " + "-" * 43)
    # 判定は実数に基づく（回収率に関わらず「本物のエッジ」と誤表示しない）。
    if year_rates and all(r > 1.0 for r in year_rates):
        print("  → 全年度で回収率>1 → 時系列に頑健なエッジの可能性（要 walk-forward 確認）")
    else:
        n_neg = sum(1 for r in year_rates if r <= 1.0)
        print(f"  → {n_neg}/{len(year_rates)} 年度で回収率≤1（負け）。時系列に頑健なエッジは確認されない")


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
    ap.add_argument("--max-odds", type=float, default=float("inf"),
                    help="オッズ上限。これ超の人気薄を除外（例: 15 で3–15倍帯に限定）")
    ap.add_argument("--max-bet-yen", type=float, default=1_000_000.0,
                    help="1ベット絶対上限（流動性制約。複利発散と inf 化を防ぐ）")
    ap.add_argument("--by-year", action="store_true",
                    help="（オッズ絞り後の）戦略を年度別フラット集計し回収率の頑健性を見る")
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
    groups = _candidates_by_race(score_table, args.ev_floor, args.ev_max, args.max_odds)
    n_races = len(groups)
    n_cand = sum(len(c) for _, c in groups)

    odds_cap = "なし" if math.isinf(args.max_odds) else f"{args.max_odds:.0f}倍"
    print("=" * 74)
    print("単勝エッジ フラクショナル・ケリー資産成長バックテスト")
    print("=" * 74)
    print(f"モデル={label} / 検証 {n_races} レース / EV下限={args.ev_floor} / オッズ上限={odds_cap} / "
          f"初期資金={args.bankroll:,.0f}円 / 1ベット上限={args.max_bet_yen:,.0f}円 / 候補 {n_cand} 件")

    tickets = BettingTickets(rp)
    settle_fn = _make_settle_fn(tickets)

    _print_edge_by_odds(groups, settle_fn, args.flat_unit)

    if args.by_year:
        _print_by_year(groups, settle_fn, args.flat_unit)
        print("=" * 74)
        return

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
        rows.append((lab, run_backtest(groups, settle_fn, opt, args.bankroll,
                                       args.flat_unit, args.max_bet_yen)))

    _print_table(rows)
    print("\n判定:")
    print(" - [エッジの所在] で回収率>1のオッズ帯に --max-odds を合わせるのが最大の改善")
    print("   （例: 3–15倍が強ければ `--max-odds 15`。15倍超の -EV 人気薄を除外する）")
    print(" - ケリーがフラットより対数成長↑かつ最大DDが許容範囲 → サイジング採用価値あり")
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
