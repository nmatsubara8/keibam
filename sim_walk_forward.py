"""Phase 3: エージェント×モンテカルロの勝率が「市場implied確率を越えるか」を前進検証する。

各レースで featured→RaceField→monte_carlo で sim 勝率を出し、市場（単勝オッズの逆数を
オーバーラウンド除去して正規化）と突き合わせる。判定は3点:
  1. log-loss(sim) vs log-loss(market)  … 実際の勝ち馬をどちらが当てるか（低いほど良い）
  2. EV ベット回収率 … sim勝率×オッズ>閾値 の馬を100円均等買い→着順1で決済。>1 なら妙味検出
  3. プラシーボ … 能力をレース内シャッフルした sim（機構だけ・情報なし）との比較

sim が市場に log-loss で勝てず EV 回収も≤1 なら、この生成モデルに賭けエッジは無い
（＝これまでの壁と同じ）。既存の行動因子検証(manji)と同じ「市場ベンチマーク×プラシーボ」流儀。

前進安全: field_from_featured は as-of 特徴のみ使用（着順・単勝を入力にしない）。学習は無し
（v1 はヒューリスティック）。実行例:
  python sim_walk_forward.py --limit 12000 --n-sim 800 --max-year 2021
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))


def _logloss(p, eps=1e-6):
    import numpy as np
    return float(-np.log(np.clip(p, eps, 1.0)))


def main():
    ap = argparse.ArgumentParser(description="ABS×モンテカルロ 市場ベンチマーク前進検証")
    ap.add_argument("--limit", type=int, default=8000, help="直近Nレースに絞る")
    ap.add_argument("--max-year", type=int, default=None, help="この年までに限定")
    ap.add_argument("--n-sim", type=int, default=800)
    ap.add_argument("--T", type=int, default=100)
    ap.add_argument("--ability-spread", type=float, default=0.20)
    ap.add_argument("--ability-sigma", type=float, default=0.35,
                    help="各simで能力を μ±(σ) から引き直す幅。小さいと確率が潰れ log-loss 爆発、"
                         "大きすぎると一様化。市場に合う値を --ability-sigma で掃引して較正する")
    ap.add_argument("--ev-threshold", type=float, default=1.10, help="EV>この値で購入")
    ap.add_argument("--min-odds", type=float, default=1.0)
    ap.add_argument("--max-odds", type=float, default=100.0)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--placebo", action="store_true", help="能力シャッフル対照も測る")
    args = ap.parse_args()

    import numpy as np
    import pandas as pd

    from app._model_eval import load_featured_data
    from src.constants._results_cols import ResultsCols
    from src.simulation._agent_race import SimConfig, monte_carlo
    from src.simulation._sim_params import field_from_featured

    featured = load_featured_data()
    if featured is None or featured.empty:
        print("featured_data がありません")
        return

    date = pd.to_datetime(featured["date"]).groupby(level=0).first().sort_values()
    order = list(date.index)
    if args.max_year:
        order = [r for r in order if str(r)[:4].isdigit() and int(str(r)[:4]) <= args.max_year]
    if args.limit and len(order) > args.limit:
        order = order[-args.limit:]
    featured = featured.loc[order]
    cfg = SimConfig(T=args.T)
    rng = np.random.default_rng(args.seed)

    print("=" * 76)
    print(f"ABS×モンテカルロ 市場ベンチマーク / {len(order):,}レース / n_sim={args.n_sim} / "
          f"能力spread={args.ability_spread}")
    print("-" * 76)

    ll_sim = ll_mkt = ll_plc = 0.0
    n_races = 0
    # EV ベット集計
    bet_n = bet_hit = 0
    bet_stake = bet_ret = 0.0
    plc_stake = plc_ret = 0.0

    for i, rid in enumerate(order):
        rd = featured.loc[[rid]] if not isinstance(featured.loc[rid], pd.DataFrame) else featured.loc[rid]
        if len(rd) < 2:
            continue
        odds = pd.to_numeric(rd[ResultsCols.TANSHO_ODDS], errors="coerce").to_numpy()
        rank = pd.to_numeric(rd[ResultsCols.RANK], errors="coerce").to_numpy()
        if not np.isfinite(odds).all() or np.nanmin(odds) <= 0:
            continue
        winner = np.where(rank == 1)[0]
        if len(winner) != 1:
            continue
        w = int(winner[0])

        field = field_from_featured(rd, ability_spread=args.ability_spread)
        sim = monte_carlo(field, n_sim=args.n_sim, cfg=cfg, seed=int(rng.integers(1 << 30)),
                          ability_sigma=args.ability_sigma)
        p_sim = sim["win"]
        # 市場implied（オーバーラウンド除去）
        inv = 1.0 / odds
        p_mkt = inv / inv.sum()

        ll_sim += _logloss(p_sim[w])
        ll_mkt += _logloss(p_mkt[w])
        n_races += 1

        # EV ベット（sim勝率×オッズ>閾値、オッズ帯フィルタ）
        ev = p_sim * odds
        buy = (ev > args.ev_threshold) & (odds >= args.min_odds) & (odds <= args.max_odds)
        for j in np.where(buy)[0]:
            bet_n += 1
            bet_stake += 100.0
            if j == w:
                bet_hit += 1
                bet_ret += 100.0 * odds[j]

        # プラシーボ（真のnull）: 能力・脚質・スタミナを一様化して情報を完全に壊す。
        # 物理ランダムのみの sim → 勝率はほぼ一様。実 sim がこれを上回らなければ情報寄与なし。
        if args.placebo:
            from src.simulation._agent_race import STYLE_STALKER, RaceField
            m = len(field.ability)
            pf = RaceField(np.ones(m), np.full(m, STYLE_STALKER, dtype=int),
                           np.ones(m), field.noise)
            ps = monte_carlo(pf, n_sim=args.n_sim, cfg=cfg, seed=int(rng.integers(1 << 30)),
                             ability_sigma=args.ability_sigma)["win"]
            ll_plc += _logloss(ps[w])
            pev = ps * odds
            pbuy = (pev > args.ev_threshold) & (odds >= args.min_odds) & (odds <= args.max_odds)
            for j in np.where(pbuy)[0]:
                plc_stake += 100.0
                if j == w:
                    plc_ret += 100.0 * odds[j]

        if (i + 1) % 2000 == 0:
            print(f"  ...{i+1:,} レース処理")

    if n_races == 0:
        print("有効レースがありません")
        return

    print("-" * 76)
    print(f"[予測精度 (log-loss, 低いほど良い) / {n_races:,}レース]")
    print(f"  sim   : {ll_sim / n_races:.4f}")
    print(f"  market: {ll_mkt / n_races:.4f}   ← 市場ベースライン")
    if args.placebo:
        print(f"  placebo(能力シャッフル): {ll_plc / n_races:.4f}")
    beats = ll_sim < ll_mkt
    print(f"  → sim は市場を {'上回る' if beats else '下回る'}"
          f"（Δ={ll_mkt/n_races - ll_sim/n_races:+.4f}）")

    print(f"\n[EV ベット回収率（sim勝率×オッズ>{args.ev_threshold}）]")
    rr = bet_ret / bet_stake if bet_stake else 0.0
    hr = bet_hit / bet_n if bet_n else 0.0
    print(f"  買い目 {bet_n:,} / 的中率 {hr:.3f} / 回収率 {rr:.3f}")
    if args.placebo and plc_stake:
        print(f"  placebo 回収率: {plc_ret / plc_stake:.3f}")

    print("-" * 76)
    if beats and rr > 1.0:
        print("★ sim が log-loss で市場を上回り、かつ EV 回収>1。生成モデルにエッジの可能性。")
    elif rr > 1.0:
        print("EV 回収>1 だが log-loss は市場に劣る。偶然/分散の可能性大。要プラシーボ確認。")
    else:
        print("→ sim は市場を越えない。ABS/モンテカルロにも賭けエッジは無い（壁と同じ）。")
    print("=" * 76)


if __name__ == "__main__":
    main()
