"""walk-forward OOS バックテスト: 各時間チャンクを、それ以前のデータだけで学習したモデルで検証。

漏洩も in-sample 楽観も無い honest な成績を、時系列に沿って測る土台。
レースを発走日順に --folds 個のチャンクへ分け、fold k（k>=1）を「fold 0..k-1 で学習した
モデル」で予測・単勝バックテストする（拡張窓 walk-forward）。fold 0 は初期学習のみで評価しない。
各 fold と通算（プール）の回収率・的中率・損益を出す。

これにより「直近20%だけ」でなく時系列全体で honest にエッジを評価でき、今後の施策
（特徴量追加・別券種・別モデル）も同じ枠組みで比較できる。

実行:
  python walk_forward.py                          # 5分割・単勝・EV1.1・単一LightGBM（速い）
  python walk_forward.py --folds 6 --ev-floor 1.2
  python walk_forward.py --stacking               # 本番同等のスタッキングで（やや遅い）
"""

from __future__ import annotations

import argparse
import sys
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
warnings.filterwarnings("ignore", message="X does not have valid feature names", category=UserWarning)


def _fmt(x):
    return f"{x:.3f}" if isinstance(x, (int, float)) else str(x)


def main():
    ap = argparse.ArgumentParser(description="walk-forward OOS バックテスト（時系列 honest 評価）")
    ap.add_argument("--folds", type=int, default=5, help="時間チャンク数（既定5＝4回評価）")
    ap.add_argument("--bet-type", default="tansho")
    ap.add_argument("--ev-floor", type=float, default=1.1, help="賭ける EV 下限（既定1.1）")
    ap.add_argument("--ev-max", type=float, default=100.0, help="賭ける EV 上限（既定100）")
    ap.add_argument("--temperature", type=float, default=1.0)
    ap.add_argument("--takeout", type=float, default=0.2)
    ap.add_argument("--stacking", action="store_true", help="本番同等スタッキングで学習（既定は単一LightGBM）")
    ap.add_argument("--by-odds", action="store_true",
                    help="全fold プールの OOS 回収率をオッズ帯別に出す（『中人気にエッジ』の honest 検証）")
    args = ap.parse_args()

    import pandas as pd

    from app._bet_type_optimizer import backtest_bet_type
    from app._model_eval import load_featured_data
    from src.constants._local_paths import LocalPaths
    from src.policies._bet_type_params import BetTypeParams
    from src.preprocessing._return_processor import ReturnProcessor
    from src.training._keiba_ai_factory import KeibaAIFactory

    featured = load_featured_data()
    if featured is None or featured.empty:
        print("featured_data がありません")
        return
    rp = ReturnProcessor(LocalPaths.RAW_RETURN_TABLES_PATH)

    # レース単位で発走日順に並べ、--folds 個のチャンクへ分割（レースを跨いで割らない）。
    race_date = pd.to_datetime(featured["date"]).groupby(level=0).first().sort_values()
    ordered = list(race_date.index)
    n = len(ordered)
    if n < args.folds * 2:
        print(f"レース数 {n} が少なすぎます（--folds {args.folds}）")
        return
    bounds = [round(i * n / args.folds) for i in range(args.folds + 1)]
    chunks = [ordered[bounds[i]:bounds[i + 1]] for i in range(args.folds)]

    params = BetTypeParams(ev_threshold=args.ev_floor, temperature=args.temperature,
                           prob_scale=1.0, ev_max=args.ev_max)

    # --by-odds: kelly_backtest の検証済み候補選択・決済を再利用して全 fold をオッズ帯別に集計。
    ODDS_BUCKETS = [(1.0, 3.0), (3.0, 7.0), (7.0, 15.0), (15.0, 50.0), (50.0, float("inf"))]
    band_acc = settle = None
    if args.by_odds:
        from kelly_backtest import _make_settle_fn
        from src.simulation._betting_tickets import BettingTickets
        band_acc = {b: {"n": 0, "hit": 0, "stake": 0.0, "ret": 0.0} for b in ODDS_BUCKETS}
        settle = _make_settle_fn(BettingTickets(rp))

    mode = "スタッキング" if args.stacking else "単一LightGBM"
    print("=" * 78)
    print(f"walk-forward OOS / 券種={args.bet_type} / EV[{args.ev_floor},{args.ev_max}] "
          f"/ 温度={args.temperature} / 学習={mode} / {args.folds}分割")
    print(f"  {'評価fold期間':<26}{'学習R':>8}{'評価R':>8}{'買い目':>8}"
          f"{'的中率':>8}{'回収率':>8}{'損益':>10}")
    print("-" * 78)

    tot_bet = tot_ret = 0.0
    fold_rrs = []
    for k in range(1, args.folds):
        train_races = [r for c in chunks[:k] for r in c]
        eval_races = chunks[k]
        train = featured.loc[train_races]
        fold = featured.loc[eval_races]
        d0 = pd.to_datetime(fold["date"]).min().date()
        d1 = pd.to_datetime(fold["date"]).max().date()
        label = f"{d0}〜{d1}"
        try:
            ai = KeibaAIFactory.create(train, test_size=0.1, valid_size=0.2)
            if args.stacking:
                ai.train_with_stacking(with_tuning=False)
            else:
                ai.train_without_tuning()
            summary, _ = backtest_bet_type(ai, fold, rp, args.bet_type, params, takeout=args.takeout)
        except Exception as e:  # noqa: BLE001
            print(f"  {label:<26}  学習/評価失敗: {e}")
            continue
        nb = summary.get("n_bets", 0)
        hr = summary.get("hit_rate", 0.0)
        rr = summary.get("return_rate", 0.0)
        tb = summary.get("total_bet_amount", 0.0)
        profit = summary.get("profit", 0.0)
        tot_bet += tb
        tot_ret += profit + tb
        if nb:
            fold_rrs.append(rr)
        print(f"  {label:<26}{len(set(train_races)):>9}{len(set(eval_races)):>9}{nb:>8}"
              f"{_fmt(hr):>8}{_fmt(rr):>8}{profit:>10,.0f}")

        # この fold の OOS 候補をオッズ帯別に集計（プールに加算）。
        if args.by_odds:
            from kelly_backtest import _candidates_by_race
            from src.policies._score_policy import ExpectedValueScorePolicy
            st = ai.calc_score(fold, ExpectedValueScorePolicy)
            for rid, cands in _candidates_by_race(st, args.ev_floor, args.ev_max, float("inf")):
                for c in cands:
                    ba, ra = settle(rid, c.combo[0], 100)
                    if ba <= 0:
                        continue
                    for lo, hi in ODDS_BUCKETS:
                        if lo <= c.odds < hi:
                            a = band_acc[(lo, hi)]
                            a["n"] += 1
                            a["stake"] += ba
                            a["ret"] += ra
                            if ra > 0:
                                a["hit"] += 1
                            break

    print("-" * 78)
    pooled = tot_ret / tot_bet if tot_bet else 0.0
    print(f"  通算（プールOOS）: 投資 {tot_bet:,.0f} / 払戻 {tot_ret:,.0f} / "
          f"回収率 {pooled:.3f} / 損益 {tot_ret - tot_bet:,.0f}")
    if fold_rrs and all(r > 1.0 for r in fold_rrs) and pooled > 1.0:
        print("  → 全 fold かつ通算で回収率>1 → 時系列に頑健なエッジの可能性（要件数・分散も確認）")
    else:
        n_neg = sum(1 for r in fold_rrs if r <= 1.0)
        print(f"  → {n_neg}/{len(fold_rrs)} fold で回収率≤1、通算 {pooled:.3f}。"
              "honest なエッジは確認されない")

    if args.by_odds:
        print("\n[オッズ帯別 OOS（全 fold プール・フラット100円）]")
        print(f"  {'オッズ帯':<12}{'買い目':>9}{'的中率':>9}{'回収率':>9}")
        any_edge = False
        for lo, hi in ODDS_BUCKETS:
            a = band_acc[(lo, hi)]
            if a["n"] == 0:
                continue
            hr_b = a["hit"] / a["n"]
            rr_b = a["ret"] / a["stake"] if a["stake"] > 0 else 0.0
            any_edge = any_edge or rr_b > 1.0
            hi_s = "∞" if hi == float("inf") else f"{hi:.0f}"
            mark = " ◎" if rr_b > 1.0 else ""
            print(f"  {f'{lo:.0f}–{hi_s}':<12}{a['n']:>9}{_fmt(hr_b):>9}{_fmt(rr_b):>9}{mark}")
        print("  ※ ◎(回収率>1)の帯があれば、そこに OOS エッジの候補。全て≤1なら exploitable な帯は無い。")
    print("=" * 78)


if __name__ == "__main__":
    main()
