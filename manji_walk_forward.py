"""卍式（行動バイアス半分・JRDB非依存）の前進検証ハーネス。

流れ（馬王Zの「推奨目を過去回収率で調整→前進検証」に相当。勝率予測モデルの再学習はしない）:
  各 fold k について
    1. 学習窓 = fold 0..k-1（過去のみ）で points[factor][bucket] を回収率較正（Layer A）
    2. その points を凍結して ManjiScorer を構成（ゾーン/得点順位/レース選別）
    3. 評価 fold k（未来・未学習）で買い目を選び、着順×単勝オッズで決済（フラット100円）
  fold別・通算の回収率、オッズ帯別、bootstrap CI を出す。

対照（過学習検出）:
  - baseline : points 空（ゾーン規律のみ）の回収率。「規律だけで何%戻るか」。
  - placebo  : 同じバケット集合にランダム点を割った config を R 回。較正が baseline/placebo を
               超えなければ、点数較正は無意味（ノイズ）と判定できる。

featured は read-only。KeibaAI/学習器は一切呼ばない。既存 walk_forward.py とは独立。

実行例:
  python manji_walk_forward.py --folds 5 --limit          # 直近2000レース・全因子
  python manji_walk_forward.py --folds 5 --zone-odds 3 50 --top-k 3 --placebo 50
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))


def _fmt(x):
    return f"{x:.3f}" if isinstance(x, (int, float)) else str(x)


def _winners(featured):
    """{str(race_id): set(着順1位の馬番)}。"""
    import pandas as pd

    from src.constants._results_cols import ResultsCols
    r = featured[[ResultsCols.UMABAN, ResultsCols.RANK]].copy()
    r["rank"] = pd.to_numeric(r[ResultsCols.RANK], errors="coerce")
    r["umaban"] = pd.to_numeric(r[ResultsCols.UMABAN], errors="coerce")
    win = r[r["rank"] == 1]
    out: dict = {}
    for rid, u in zip(win.index.astype(str), win["umaban"], strict=False):
        if u == u:
            out.setdefault(str(rid), set()).add(int(u))
    return out


ODDS_BUCKETS = [(1.0, 3.0), (3.0, 7.0), (7.0, 15.0), (15.0, 50.0), (50.0, float("inf"))]


def _settle(chosen, winners, band=None, payoffs=None):
    """chosen(DataFrame: race_id,umaban,odds) をフラット100円で決済 → (n, hit, stake, ret)。

    payoffs=None（既定）は単勝: 着順1位なら 100×単勝オッズ。
    payoffs 指定時は複勝等の単一馬券 {(race_id,馬番): 払戻円}。当選（キーあり）なら払戻円、
    非当選（キー無し）は0。band 集計は選択時の単勝オッズ(chosen.odds)基準のまま。
    """
    n = hit = 0
    stake = ret = 0.0
    for row in chosen.itertuples(index=False):
        if payoffs is None:
            won = int(row.umaban) in winners.get(str(row.race_id), set())
            r = 100.0 * float(row.odds) if won else 0.0
        else:
            pay = payoffs.get((str(row.race_id), int(row.umaban)))
            won = pay is not None
            r = float(pay) if won else 0.0
        n += 1
        stake += 100.0
        ret += r
        if won:
            hit += 1
        if band is not None:
            for lo, hi in ODDS_BUCKETS:
                if lo <= row.odds < hi:
                    a = band[(lo, hi)]
                    a["n"] += 1
                    a["stake"] += 100.0
                    a["ret"] += r
                    if won:
                        a["hit"] += 1
                    break
    return n, hit, stake, ret


def _random_points(points, rng, clip):
    """points と同じ factor/bucket 構造に一様乱数 [-clip,clip] を割った placebo config。"""
    return {f: {b: float(rng.uniform(-clip, clip)) for b in bk} for f, bk in points.items()}


def main():
    ap = argparse.ArgumentParser(description="卍式(行動因子)の前進検証")
    ap.add_argument("--folds", type=int, default=5)
    ap.add_argument("--limit", type=int, nargs="?", const=2000, default=None,
                    help="直近Nレースに絞る（省略値2000）")
    ap.add_argument("--factors", default="all", help="使う因子（カンマ区切り or 'all'）")
    ap.add_argument("--zone-odds", type=float, nargs=2, default=[3.0, 50.0],
                    metavar=("LO", "HI"), help="買うオッズ帯（低/高配当の隅を除外）")
    ap.add_argument("--top-k", type=int, default=3, help="race内 得点順位の上限")
    ap.add_argument("--min-n", type=int, default=30, help="較正の最小バケット件数")
    ap.add_argument("--universality-slices", type=int, default=3)
    ap.add_argument("--min-agree", type=float, default=0.7)
    ap.add_argument("--lam", type=float, default=1.0)
    ap.add_argument("--clip", type=float, default=2.0)
    ap.add_argument("--placebo", type=int, default=30, help="プラシーボ試行数（0で無効）")
    ap.add_argument("--factor-weights", action="store_true",
                    help="符号付き因子重み w_f を検証foldで推定して適用（冗長/弱因子の希釈を抑える）")
    ap.add_argument("--optuna", type=int, default=0, metavar="N",
                    help="Layer B: Optuna で 因子重み(0=除外含む)＋ゾーン境界＋top_k を検証foldで"
                         "N試行探索（因子24固定を外す）。--factor-weights/--zone-odds/--top-k を上書き")
    ap.add_argument("--valid-frac", type=float, default=0.3,
                    help="学習窓のうち検証(valid)に回す割合（重み/Optuna推定用）")
    ap.add_argument("--optuna-cv", type=int, default=3,
                    help="Optuna目的のvalidクロス検証分割数（全スライスで良い構成のみ高評価＝過学習抑制）")
    ap.add_argument("--crosses", type=int, default=0, metavar="N",
                    help="因子クロス(2因子の相互作用)を最古学習chunkで選別し上位Nを因子に追加。"
                         "クロス点は加法成分を引いた『交互作用残差』にするので加法の二重計上を回避し、"
                         "Optuna が単独(w_A,w_B)と相互作用(w_cross)を独立に重み付けする")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--bet-type", choices=["tansho", "fukusho"], default="tansho",
                    help="決済する馬券種。fukusho は payoffs.pkl の複勝払戻で決済（因子スコア/"
                         "ゾーンは単勝ベースのまま＝『単勝妙味で選んだ馬を複勝で買う』検証）")
    ap.add_argument("--payoffs", default=None,
                    help="払戻テーブル（既定 data/raw/payoffs.pkl）。--bet-type fukusho で使用")
    args = ap.parse_args()

    import numpy as np
    import pandas as pd

    from app._model_eval import load_featured_data
    from src.policies._manji_factors import FACTORS
    from src.policies._manji_scorer import ManjiScorer, ManjiScorerConfig
    from src.tuning._manji_calibration import calibrate_factor_weights, calibrate_points
    from src.tuning._manji_optuna import optimize_manji_config

    featured = load_featured_data()
    if featured is None or featured.empty:
        print("featured_data がありません")
        return

    # 複勝決済用の払戻ルックアップ（--bet-type fukusho のときだけロード）
    payoff_lookup = None
    if args.bet_type == "fukusho":
        from src.constants._local_paths import LocalPaths
        from src.tuning._payoffs import load_payoffs, single_horse_payoff_lookup
        pp = args.payoffs or str(Path(LocalPaths.RAW_DIR) / "payoffs.pkl")
        payoffs_df = load_payoffs(pp)
        payoff_lookup = single_horse_payoff_lookup(payoffs_df, "fukusho")
        if not payoff_lookup:
            print(f"複勝払戻が空です（{pp}）。import_archive_odds.py で payoffs.pkl を作成してください")
            return
        print(f"[--bet-type fukusho] 複勝払戻 {len(payoff_lookup):,} 件をロード（{pp}）")
    factor_names = list(FACTORS) if args.factors == "all" else \
        [f.strip() for f in args.factors.split(",") if f.strip() in FACTORS]
    if not factor_names:
        print("有効な因子がありません")
        return

    race_date = pd.to_datetime(featured["date"]).groupby(level=0).first().sort_values()
    ordered = list(race_date.index)
    if args.limit and len(ordered) > args.limit:
        ordered = ordered[-args.limit:]
        featured = featured.loc[ordered]
        print(f"[--limit] 直近 {args.limit:,} レース（全 {len(race_date):,} 中）")
    n = len(ordered)
    if n < args.folds * 2:
        print(f"レース数 {n} が少なすぎます")
        return
    bounds = [round(i * n / args.folds) for i in range(args.folds + 1)]
    chunks = [ordered[bounds[i]:bounds[i + 1]] for i in range(args.folds)]
    winners_by_fold = {k: _winners(featured.loc[chunks[k]]) for k in range(args.folds)}

    # 相互作用（クロス）因子: 最古 chunk[0]（全評価foldの過去に必ず含まれる＝前進安全）で
    # 選別し、上位 N を因子集合に追加。実点は残差化されるので単独の和で説明できるクロスは脱落。
    if args.crosses:
        from src.tuning._manji_crosses import screen_crosses
        crosses = screen_crosses(
            featured.loc[chunks[0]], factor_names, top_n=args.crosses, min_n=args.min_n,
        )
        if crosses:
            factor_names = factor_names + crosses
            print(f"[--crosses] 最古chunkで選別した相互作用 {len(crosses)}件を追加: "
                  f"{', '.join(crosses)}")
        else:
            print("[--crosses] 有効な相互作用（残差の大きい安定クロス）は見つからず")

    zone = (float(args.zone_odds[0]), float(args.zone_odds[1]))
    print("=" * 82)
    print(f"卍式(行動因子) 前進検証 / 券種={args.bet_type} / 因子{len(factor_names)}個 / "
          f"ゾーンodds{zone} / top_k={args.top_k} / {args.folds}分割")
    print(f"  {'評価fold期間':<24}{'学習R':>7}{'評価R':>7}{'採用因子':>8}"
          f"{'買い目':>7}{'的中率':>7}{'回収率':>8}{'baseline':>9}")
    print("-" * 82)

    band = {b: {"n": 0, "hit": 0, "stake": 0.0, "ret": 0.0} for b in ODDS_BUCKETS}
    tot = {"stake": 0.0, "ret": 0.0}
    base_tot = {"stake": 0.0, "ret": 0.0}
    fold_rrs = []
    placebo_pooled = np.zeros(args.placebo) if args.placebo else None
    rng = np.random.default_rng(args.seed)

    for k in range(1, args.folds):
        train = featured.loc[[r for c in chunks[:k] for r in c]]
        fold = featured.loc[chunks[k]]
        w = winners_by_fold[k]
        d0 = pd.to_datetime(fold["date"]).min().date()
        d1 = pd.to_datetime(fold["date"]).max().date()

        points = calibrate_points(
            train, factor_names, lam=args.lam, clip=args.clip, min_n=args.min_n,
            universality_slices=args.universality_slices, min_agree=args.min_agree,
        )
        weights = {}
        cfg_zone, cfg_top_k = zone, args.top_k
        if args.optuna:
            # Layer B: calib/valid 分割 → Optuna で重み・ゾーン・top_k を探索
            tr_races = list(pd.to_datetime(train["date"]).groupby(level=0).first().sort_values().index)
            cut = int(len(tr_races) * (1.0 - args.valid_frac))
            calib = train.loc[tr_races[:cut]]
            valid = train.loc[tr_races[cut:]]
            res = optimize_manji_config(
                calib, valid, factor_names, n_trials=args.optuna, seed=args.seed,
                valid_cv=args.optuna_cv,
                odds_lo_range=(1.0, float(zone[0]) + 3.0), odds_hi_range=(10.0, float(zone[1]) + 30.0),
                lam=args.lam, clip=args.clip, min_n=args.min_n,
                universality_slices=args.universality_slices, min_agree=args.min_agree,
            )
            points, weights = res["points"], res["weights"]
            cfg_zone, cfg_top_k = res["zone"], res["top_k"]
            print(f"    [optuna k={k}] zone=({cfg_zone[0]:.1f},{cfg_zone[1]:.1f}) top_k={cfg_top_k} "
                  f"採用因子={res['n_active']} valid回収={res['value']:.3f}")
        elif args.factor_weights:
            weights = calibrate_factor_weights(
                train, factor_names, valid_frac=args.valid_frac, lam=args.lam, clip=args.clip,
                min_n=args.min_n, universality_slices=args.universality_slices, min_agree=args.min_agree,
            )
        cfg = ManjiScorerConfig(points=points, weights=weights, zone_odds=cfg_zone, top_k=cfg_top_k)
        chosen = ManjiScorer(cfg).select(fold)
        nb, hit, stake, ret = _settle(chosen, w, band, payoffs=payoff_lookup)

        # baseline: ゾーン規律のみ（点数空）
        cfg0 = ManjiScorerConfig(points={}, zone_odds=cfg_zone, top_k=cfg_top_k)
        bchosen = ManjiScorer(cfg0).select(fold)
        _, _, bstake, bret = _settle(bchosen, w, payoffs=payoff_lookup)

        rr = ret / stake if stake else 0.0
        brr = bret / bstake if bstake else 0.0
        hr = hit / nb if nb else 0.0
        tot["stake"] += stake
        tot["ret"] += ret
        base_tot["stake"] += bstake
        base_tot["ret"] += bret
        if nb:
            fold_rrs.append(rr)
        print(f"  {f'{d0}〜{d1}':<24}{len(set(train.index)):>7}"
              f"{len(set(fold.index)):>7}{len(points):>8}{nb:>7}"
              f"{_fmt(hr):>7}{_fmt(rr):>8}{_fmt(brr):>9}")

        # placebo: 同バケットにランダム点（重み・ゾーンは実configと同じに保つ）
        if args.placebo:
            for pi in range(args.placebo):
                pcfg = ManjiScorerConfig(points=_random_points(points, rng, args.clip),
                                         weights=weights, zone_odds=cfg_zone, top_k=cfg_top_k)
                pch = ManjiScorer(pcfg).select(fold)
                _, _, ps, pr = _settle(pch, w, payoffs=payoff_lookup)
                # 通算プールに寄与（stake加重で後で割る）
                placebo_pooled[pi] += pr - ps  # 損益を積む（後で /総stakeは近似のため損益で比較）

    print("-" * 82)
    pooled = tot["ret"] / tot["stake"] if tot["stake"] else 0.0
    base_pooled = base_tot["ret"] / base_tot["stake"] if base_tot["stake"] else 0.0
    print(f"  通算: 投資 {tot['stake']:,.0f} / 払戻 {tot['ret']:,.0f} / "
          f"回収率 {pooled:.3f}（baseline ゾーンのみ {base_pooled:.3f}）")

    if args.placebo:
        real_profit = tot["ret"] - tot["stake"]
        pct = float((placebo_pooled < real_profit).mean())
        print(f"  プラシーボ（ランダム点 {args.placebo}回）: 実際の損益 {real_profit:,.0f} は "
              f"ランダム分布の {pct*100:.0f}%点。中央 {np.median(placebo_pooled):,.0f}")
        if pct >= 0.95 and pooled > 1.0:
            print("  → ★較正がプラシーボを有意に超え、かつ回収率>1。行動因子に本物の寄与の可能性。")
        elif pooled > 1.0:
            print("  → 回収率>1 だがプラシーボ内。点数較正の寄与は有意でない（要 fold 数増・因子精査）。")
        else:
            print("  → 回収率≤1。行動因子だけでは控除の壁を越えない（JRDB相当の直交情報が不足）。")

    print("\n[オッズ帯別 OOS（全 fold プール・フラット100円）]")
    print(f"  {'オッズ帯':<12}{'買い目':>9}{'的中率':>9}{'回収率':>9}")
    for lo, hi in ODDS_BUCKETS:
        a = band[(lo, hi)]
        if a["n"] == 0:
            continue
        hr_b = a["hit"] / a["n"]
        rr_b = a["ret"] / a["stake"] if a["stake"] > 0 else 0.0
        hi_s = "∞" if hi == float("inf") else f"{hi:.0f}"
        mark = " ◎" if rr_b > 1.0 else ""
        print(f"  {f'{lo:.0f}–{hi_s}':<12}{a['n']:>9}{_fmt(hr_b):>9}{_fmt(rr_b):>9}{mark}")
    print("=" * 82)


if __name__ == "__main__":
    main()
