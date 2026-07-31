"""物理シムの着順標本から券種別「買い方（戦略）」の回収率を測り、前進検証で戦略を選ぶ。

■ 何を測るか
各レースで `monte_carlo(return_orders=True)` を1回回し、着順標本 top3_orders から券種確率を
**同時頻度** で直接推定（`aggregate_ticket_probabilities`）。戦略テンプレ（S1〜S6・単勝/複勝）
ごとに買い目を生成し、JRDB HJC の確定払戻（全8券種・100%）で決済する。sim はレースにつき
1回で、全戦略はその結果を共有する（買い方だけが違う）＝効率的。

■ 既存資産の再利用（再発明しない）
  着順標本   : `monte_carlo(..., return_orders=True)`（物理シム）
  決済        : `JrdbHjcReturnSource` + `BettingTickets`（8券種・厳密照合）
  集計/指標   : `_backtest.settle_candidates`→`BetTypeStats`（roi/roi_ex_top=除最大/reliable/top_share）
  本スクリプト固有: 戦略テンプレ適用・年別・レース単位 bootstrap CI・前進検証での戦略選択。

■ ファットテール規律（三連単ほど必須）
三連単の回収率は万馬券1本で激変する。判定は素の ROI 単独では行わず、
  ①除最大1件 ROI（roi_ex_top）②的中信頼（reliable）③レース単位 bootstrap CI の下限
  ④年別 ROI の符号安定 を併記する。単一年の高 ROI は採用しない。

■ 前進検証（戦略のリーク回避）
「過去年で最良戦略を決め→固定し→次年で評価」を隣接年で回す。過去年の最良戦略が翌年でも
プラス圏を保つかで、戦略選択自体の過学習を検出する。rank_bonus を使う場合、featured の
rank_bonus は単一スナップ全期間＝leak なので ROI は過去探索用（live には transfer しない）。

使い方:
  python scripts/sim_ticket_strategy_roi.py --db path/to.db --limit 8000 --n-sim 800
  python scripts/sim_ticket_strategy_roi.py --db path/to.db --walk-forward --n-sim 800
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _sim_race_probs(rd, *, n_sim, cfg, ability_spread, ability_sigma, rank_gain, seed):
    """1レースを sim して (rank馬番リスト, 券種確率dict) を返す。無効レースは (None, None)。"""
    import numpy as np
    import pandas as pd

    from src.constants._results_cols import ResultsCols
    from src.simulation._agent_race import monte_carlo
    from src.simulation._sim_params import field_from_featured
    from src.simulation._ticket_backtest import aggregate_ticket_probabilities, sim_rank

    if len(rd) < 3:
        return None, None, None
    umaban = pd.to_numeric(rd[ResultsCols.UMABAN], errors="coerce").to_numpy()
    if not np.isfinite(umaban).all():
        return None, None, None
    umaban = umaban.astype(int)
    field = field_from_featured(rd, ability_spread=ability_spread, rank_gain=rank_gain)
    out = monte_carlo(field, n_sim=n_sim, cfg=cfg, seed=seed, ability_sigma=ability_sigma,
                      return_orders=True)
    probs = aggregate_ticket_probabilities(out["top3_orders"], umaban)
    rank = sim_rank(out["win"], umaban)
    return rank, probs, umaban


def _run_strategies(featured, order, ret_src, strategies, *, n_sim, T, ability_spread,
                    ability_sigma, rank_gain, seed):
    """全レースを sim し、各戦略の候補を集めて {戦略名: (per_bet_type, per_race)} を返す。"""
    import numpy as np
    import pandas as pd

    from src.simulation._agent_race import SimConfig
    from src.simulation._backtest import settle_candidates
    from src.simulation._ticket_backtest import build_candidates, settle_per_race

    cfg = SimConfig(T=T)
    rng = np.random.default_rng(seed)
    cands_by_strat = {name: [] for name in strategies}
    n_ok = 0
    for i, rid in enumerate(order):
        rd = featured.loc[[rid]] if not isinstance(featured.loc[rid], pd.DataFrame) else featured.loc[rid]
        rank, probs, _ = _sim_race_probs(rd, n_sim=n_sim, cfg=cfg, ability_spread=ability_spread,
                                         ability_sigma=ability_sigma, rank_gain=rank_gain,
                                         seed=int(rng.integers(1 << 30)))
        if rank is None:
            continue
        n_ok += 1
        for name, strat in strategies.items():
            cands_by_strat[name].extend(build_candidates(rid, rank, probs, strat))
        if (i + 1) % 2000 == 0:
            print(f"  ...{i + 1:,} レース sim 済", file=sys.stderr)
    result = {}
    for name, cands in cands_by_strat.items():
        per_bt = settle_candidates(cands, ret_src)
        per_race = settle_per_race(cands, ret_src)
        result[name] = (per_bt, per_race)
    return result, n_ok


def _strategy_line(name, per_bt, per_race):
    """1戦略の要約行（素ROI・除最大ROI・信頼・bootstrap CI・年別安定）を組む。"""
    from src.simulation._ticket_backtest import race_bootstrap_ci, roi_by_year

    n_bets = sum(s.n_bets for s in per_bt.values())
    n_hits = sum(s.n_hits for s in per_bt.values())
    stake = sum(s.stake for s in per_bt.values())
    returned = sum(s.returned for s in per_bt.values())
    max_ret = max((s.max_return for s in per_bt.values()), default=0.0)
    roi = returned / stake if stake else 0.0
    roi_ex = (returned - max_ret) / stake if stake else 0.0
    ci = race_bootstrap_ci(per_race, n_boot=1000)
    years = roi_by_year(per_race)
    yr_pos = sum(1 for v in years.values() if v >= 1.0)
    hit_rate = n_hits / n_bets if n_bets else 0.0
    return {
        "name": name, "n_bets": n_bets, "hit_rate": hit_rate, "roi": roi, "roi_ex": roi_ex,
        "ci_lo": ci["lo"], "ci_hi": ci["hi"], "yr_pos": yr_pos, "yr_tot": len(years),
        "years": years,
    }


def _print_table(rows):
    print(f"{'戦略':<22}{'点数':>8}{'的中率':>8}{'ROI':>8}{'除最大':>8}"
          f"{'CI下限':>8}{'CI上限':>8}{'年+/計':>8}")
    print("-" * 86)
    for r in sorted(rows, key=lambda x: -x["roi_ex"]):
        print(f"{r['name']:<22}{r['n_bets']:>8,}{r['hit_rate']:>8.1%}{r['roi']:>8.1%}"
              f"{r['roi_ex']:>8.1%}{r['ci_lo']:>8.2f}{r['ci_hi']:>8.2f}"
              f"{str(r['yr_pos'])+'/'+str(r['yr_tot']):>8}")
    print("\n※ 判定は ROI 単独でなく『除最大ROI・CI下限>1・年別+の多さ』で。三連単の素ROIは"
          "万馬券1本で激変する。CI下限が1未満なら統計的に黒字とは言えない。")


def main() -> int:
    from app._model_eval import load_featured_data
    from src.simulation._ticket_backtest import STRATEGY_TEMPLATES

    ap = argparse.ArgumentParser(description="券種別 買い方(戦略) ROI ＋ 前進検証での戦略選択")
    ap.add_argument("--db", default=None, help="SQLite（raw_jrdb_hjc 読込）")
    ap.add_argument("--featured", default=None, help="featured pkl（既定=本番）。rank_bonus 列可")
    ap.add_argument("--limit", type=int, default=8000)
    ap.add_argument("--max-year", type=int, default=None)
    ap.add_argument("--n-sim", type=int, default=800)
    ap.add_argument("--T", type=int, default=100)
    ap.add_argument("--ability-spread", type=float, default=0.20)
    ap.add_argument("--ability-sigma", type=float, default=0.35)
    ap.add_argument("--rank-gain", type=float, default=0.0, help="rank_bonus の加減点強さ(leak注意)")
    ap.add_argument("--walk-forward", action="store_true", help="過去年で戦略選択→翌年で評価")
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    import pandas as pd

    from src.simulation._jrdb_return_source import JrdbHjcReturnSource
    from src.storage._db import get_engine

    featured = load_featured_data(args.featured) if args.featured else load_featured_data()
    if featured is None or featured.empty:
        print("featured がありません", file=sys.stderr)
        return 1
    engine = get_engine(args.db)
    try:
        hjc = pd.read_sql("SELECT * FROM raw_jrdb_hjc", engine)
    except Exception as e:  # noqa: BLE001
        print(f"raw_jrdb_hjc を読めません（HJC 取込済みか確認）: {e}", file=sys.stderr)
        return 1
    if hjc.empty:
        print("raw_jrdb_hjc が空です。HJC を取り込んでください。", file=sys.stderr)
        return 1
    ret_src = JrdbHjcReturnSource(engine=None, hjc=hjc)
    print(f"[HJC] 払戻レコード {len(hjc):,} 行 → 8券種の確定払戻源を構築")

    # ① 規律: rank_bonus は live 予測専用。バックテスト（本スクリプト）で rank_gain!=0 はリーク。
    from src.simulation._rank_bonus import assert_live_only
    assert_live_only(args.rank_gain, context="券種ROIバックテスト")

    date = pd.to_datetime(featured["date"]).groupby(level=0).first().sort_values()
    order = list(date.index)
    if args.max_year:
        order = [r for r in order if str(r)[:4].isdigit() and int(str(r)[:4]) <= args.max_year]
    if args.limit and len(order) > args.limit:
        order = order[-args.limit:]
    featured = featured.loc[order]

    strategies = {k: v for k, v in STRATEGY_TEMPLATES.items() if k != "S0_skip"}
    kw = dict(n_sim=args.n_sim, T=args.T, ability_spread=args.ability_spread,
              ability_sigma=args.ability_sigma, rank_gain=args.rank_gain, seed=args.seed)

    if not args.walk_forward:
        print(f"[全期間] {len(order):,}レース / n_sim={args.n_sim} / rank_gain={args.rank_gain}")
        res, n_ok = _run_strategies(featured, order, ret_src, strategies, **kw)
        print(f"有効 sim レース {n_ok:,}\n")
        _print_table([_strategy_line(n, pb, pr) for n, (pb, pr) in res.items()])
        return 0

    # 前進検証: 隣接年で「過去年の最良(除最大ROI)戦略 → 翌年評価」
    years = sorted({str(r)[:4] for r in order if str(r)[:4].isdigit()})
    print(f"[前進検証] 年 {years} で 過去年→翌年 を評価\n")
    picks = []
    for tr, te in zip(years, years[1:]):
        tr_order = [r for r in order if str(r)[:4] == tr]
        te_order = [r for r in order if str(r)[:4] == te]
        if len(tr_order) < 50 or len(te_order) < 50:
            continue
        tr_res, _ = _run_strategies(featured.loc[tr_order], tr_order, ret_src, strategies, **kw)
        tr_rows = [_strategy_line(n, pb, pr) for n, (pb, pr) in tr_res.items()]
        # 過去年での選択規準: 除最大ROI 最大（フロック依存を避ける）かつ点数十分
        elig = [r for r in tr_rows if r["n_bets"] >= 100] or tr_rows
        best = max(elig, key=lambda r: r["roi_ex"])
        te_res, _ = _run_strategies(featured.loc[te_order], te_order, ret_src,
                                    {best["name"]: strategies[best["name"]]}, **kw)
        te_line = _strategy_line(best["name"], *te_res[best["name"]])
        picks.append((tr, te, best, te_line))
        print(f"  {tr}→{te}: 選択『{best['name']}』(train除最大ROI {best['roi_ex']:.1%}) → "
              f"test ROI {te_line['roi']:.1%} / 除最大 {te_line['roi_ex']:.1%} / "
              f"CI[{te_line['ci_lo']:.2f},{te_line['ci_hi']:.2f}]")
    if picks:
        te_roi = [p[3]["roi_ex"] for p in picks]
        pos = sum(1 for v in te_roi if v >= 1.0)
        print(f"\n翌年評価: 除最大ROI が黒字(≥1.0)の年 {pos}/{len(picks)}。"
              "過半かつ CI 下限>1 の年があってはじめて『買い方に持続エッジ』の候補。"
              "そうでなければ、これも市場効率の壁の再確認。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
