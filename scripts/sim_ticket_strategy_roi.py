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
    """1レースを sim して (rank馬番リスト, 券種確率dict, umaban, winner馬番) を返す。無効は (None,..)。"""
    import numpy as np
    import pandas as pd

    from src.constants._results_cols import ResultsCols
    from src.simulation._agent_race import monte_carlo
    from src.simulation._sim_params import field_from_featured
    from src.simulation._ticket_backtest import (
        aggregate_ticket_probabilities, sim_rank, validate_ranking,
    )

    if len(rd) < 3:
        return None, None, None, None
    umaban = pd.to_numeric(rd[ResultsCols.UMABAN], errors="coerce").to_numpy()
    if not np.isfinite(umaban).all():
        return None, None, None, None
    umaban = umaban.astype(int)
    rank_arr = pd.to_numeric(rd[ResultsCols.RANK], errors="coerce").to_numpy()
    win_mask = np.where(rank_arr == 1)[0]
    winner = int(umaban[win_mask[0]]) if len(win_mask) == 1 else None
    field = field_from_featured(rd, ability_spread=ability_spread, rank_gain=rank_gain)
    out = monte_carlo(field, n_sim=n_sim, cfg=cfg, seed=seed, ability_sigma=ability_sigma,
                      return_orders=True)
    probs = aggregate_ticket_probabilities(out["top3_orders"], umaban)
    rank = sim_rank(out["win"], umaban)
    validate_ranking(rank, str(rd.index[0]))          # ROI 以前のデータ整合性ガード（重複馬番検出）
    return rank, probs, umaban, winner


def _run_strategies(featured, order, ret_src, strategies, *, n_sim, T, ability_spread,
                    ability_sigma, rank_gain, seed):
    """全レースを sim し、各戦略の候補＋レース情報を返す。

    返す: ({戦略名: (per_bet_type, per_race)}, n_ok, all_cands, sim_top_by_race)。
    all_cands=全戦略の候補を連結（同時運用ポートフォリオ用）、sim_top_by_race={race_id: sim1位馬番}。
    """
    import numpy as np
    import pandas as pd

    from src.simulation._agent_race import SimConfig
    from src.simulation._backtest import settle_candidates
    from src.simulation._ticket_backtest import build_candidates, settle_per_race

    from src.simulation._ticket_backtest import TANSHO, s4_point_audit

    cfg = SimConfig(T=T)
    rng = np.random.default_rng(seed)
    cands_by_strat = {name: [] for name in strategies}
    sim_top_by_race: dict = {}
    calib: list = []                # (p1_of_sim_top, sim_top_が勝ったか) ＝ S9 閾値の校正確認用
    s4_field: dict = {}             # S4 の頭数別レース数（点数不足理由の実データ確認）
    n_ok = 0
    for i, rid in enumerate(order):
        rd = featured.loc[[rid]] if not isinstance(featured.loc[rid], pd.DataFrame) else featured.loc[rid]
        rank, probs, _, winner = _sim_race_probs(rd, n_sim=n_sim, cfg=cfg,
                                                 ability_spread=ability_spread,
                                                 ability_sigma=ability_sigma, rank_gain=rank_gain,
                                                 seed=int(rng.integers(1 << 30)))
        if rank is None:
            continue
        n_ok += 1
        sim_top_by_race[str(rid)] = rank[0]
        if winner is not None:
            calib.append((float(probs.get(TANSHO, {}).get(rank[0], 0.0)), int(winner == rank[0])))
        aud = s4_point_audit(rank)
        s4_field[aud["actual"]] = s4_field.get(aud["actual"], 0) + 1
        for name, strat in strategies.items():
            cands_by_strat[name].extend(build_candidates(rid, rank, probs, strat))
        if (i + 1) % 2000 == 0:
            print(f"  ...{i + 1:,} レース sim 済", file=sys.stderr)
    result = {}
    all_cands = []
    for name, cands in cands_by_strat.items():
        per_bt = settle_candidates(cands, ret_src)
        per_race = settle_per_race(cands, ret_src)
        result[name] = (per_bt, per_race)
        all_cands.extend(cands)
    return result, n_ok, all_cands, sim_top_by_race, calib, s4_field


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


def _load_oz_win_odds(oz_dir, race_set):
    """OZ .txt 群 → {race_id: {馬番: 前売り単勝オッズ}}（購入時点）。race_set に限定。"""
    import glob

    from src.jrdb._odds import parse_odds
    files = sorted(glob.glob(f"{oz_dir}/OZ*.txt") + glob.glob(f"{oz_dir}/oz*.txt"))
    out: dict = {}
    for fp in files:
        try:
            long_df = parse_odds(fp, "OZ")
        except Exception:  # noqa: BLE001
            continue
        if long_df is None or long_df.empty:
            continue
        tan = long_df[long_df["bet"] == "tansho"]
        for rid, g in tan.groupby("race_id"):
            rid = str(rid)
            if rid not in race_set:
                continue
            od = {int(c): float(o) for c, o in zip(g["combo"], g["odds"], strict=False)
                  if str(c).isdigit() and o and float(o) > 0}
            if od:
                out.setdefault(rid, {}).update(od)
    return out


def _single_candidates(bet_type, pick_by_race):
    """{race_id: 馬番} → その馬1点を買う BetCandidate 群（単勝/複勝の対照用）。"""
    from src.policies._bet_candidate import BetCandidate
    return [BetCandidate(race_id=str(rid), bet_type=bet_type, combo=(int(u),),
                         probability=0.0, odds=0.0, expected_value=0.0)
            for rid, u in pick_by_race.items()]


def _arm_roi(per_race):
    st = sum(d["stake"] for d in per_race.values())
    rt = sum(d["returned"] for d in per_race.values())
    hits = sum(d["n_hits"] for d in per_race.values())
    n = len(per_race)
    return (rt / st if st else 0.0), (hits / n if n else 0.0), n


def _market_control(oz_dir, featured, order, sim_top, ret_src):
    """[市場対照] 購入時点の単勝1番人気 vs Sim1位 を 単勝/複勝で並べ、paired ΔROI CI・一致率を出す。

    リーク規律: 市場1番人気は OZ 前売り（購入時点）で決定・確定払戻(HJC)で精算。sim1位は as-of sim。
    OZ 無指定/被りゼロなら測定不能を明示（確定オッズでの代用はしない）。
    """
    from src.constants._bet_types import BetType
    from src.simulation._ticket_backtest import (
        market_favorite, paired_delta_roi_ci, settle_per_race,
    )
    print("[市場対照] 購入時点(前売り)の単勝1番人気 vs Sim1位（確定払戻で精算）")
    if not oz_dir:
        print("  → OZ 前売りオッズ未指定(--oz-dir)。市場対照は測定不能。"
              "確定オッズでの1番人気代用は方針違反（リーク）なので行わない。")
        return
    win_odds = _load_oz_win_odds(oz_dir, set(map(str, order)))
    fav = market_favorite(win_odds)
    common = [r for r in fav if r in sim_top]
    if not common:
        print(f"  → OZ と評価レースの被りが0（OZ {len(fav):,} / sim {len(sim_top):,}）。測定不能。")
        return
    mkt_pick = {r: fav[r] for r in common}
    sim_pick = {r: sim_top[r] for r in common}
    agree = sum(1 for r in common if mkt_pick[r] == sim_pick[r]) / len(common)

    arms = {}
    for label, bt, pick in (("Market-W", BetType.TANSHO, mkt_pick),
                            ("Sim-W", BetType.TANSHO, sim_pick),
                            ("Market-P", BetType.FUKUSHO, mkt_pick),
                            ("Sim-P", BetType.FUKUSHO, sim_pick)):
        pr = settle_per_race(_single_candidates(bt, pick), ret_src)
        roi, hit, n = _arm_roi(pr)
        avg_odds = (sum(win_odds[r].get(pick[r], 0.0) for r in common if r in win_odds)
                    / len(common))
        arms[label] = {"roi": roi, "hit": hit, "n": n, "avg_odds": avg_odds, "per_race": pr}

    print(f"  評価レース(共通) {len(common):,} / Sim1位=市場1番人気の一致率 {agree:.1%}"
          + ("  ← 90%超＝シムは市場順位の再表現に近い" if agree >= 0.90 else ""))
    print(f"  {'':12}{'ROI':>8}{'的中率':>8}{'平均オッズ':>10}{'決済N':>7}")
    for label in ("Market-W", "Sim-W", "Market-P", "Sim-P"):
        a = arms[label]
        print(f"  {label:<12}{a['roi']:>8.1%}{a['hit']:>8.1%}{a['avg_odds']:>10.2f}{a['n']:>7,}")
    for tag, sim_l, mkt_l in (("単勝", "Sim-W", "Market-W"), ("複勝", "Sim-P", "Market-P")):
        d = paired_delta_roi_ci(arms[sim_l]["per_race"], arms[mkt_l]["per_race"], n_boot=2000)
        sig = "有意" if (d["lo"] > 0 or d["hi"] < 0) else "有意でない"
        print(f"  {tag} ΔROI(Sim−Market)={d['delta']:+.1%}  95%CI[{d['lo']:+.1%},{d['hi']:+.1%}] "
              f"→ {sig}（0を跨がなければ純増分あり）")
    print("  読み: 一致率≥90%かつ ΔROI CI が 0 を跨ぐなら、シムは市場1番人気の再表現で純増分なし"
          "（＝市場効率の壁）。ΔROI CI が有意に正なら初めて『市場を超える選別』の候補。")


def _print_total(all_cands, ret_src, order):
    """[券種グループ別 TOTAL] と [ALL TOTAL]（全戦略を同時に全購入した仮想ポートフォリオ）。"""
    from src.simulation._ticket_backtest import (
        BET_GROUP_ORDER, portfolio_metrics, settle_tickets_detailed,
    )
    rows = settle_tickets_detailed(all_cands, ret_src)
    m = portfolio_metrics(rows, race_order=[str(r) for r in order])
    print("\n[券種グループ別 TOTAL]（三連系の大量投資が全券種合算を支配するのを切り分け）")
    print(f"  {'グループ':<14}{'点数':>9}{'的中率':>8}{'投資':>12}{'払戻':>13}{'ROI':>8}")
    for g in BET_GROUP_ORDER:
        d = m["by_group"].get(g)
        if not d:
            continue
        hit = d["n_hits"] / d["n_bets"] if d["n_bets"] else 0.0
        print(f"  {g:<14}{d['n_bets']:>9,}{hit:>8.1%}{d['stake']:>12,.0f}"
              f"{d['returned']:>13,.0f}{d['roi']:>8.1%}")
    print("\n[ALL TOTAL]（全8戦略を同時に全購入した仮想ポートフォリオ・投資額加重ROI）")
    print(f"  投資={m['total_stake']:,.0f}  払戻={m['total_return']:,.0f}  損益={m['profit']:+,.0f}")
    print(f"  ROI={m['roi']:.1%}  除最大1={m['roi_ex_top1']:.1%}  除上位5={m['roi_ex_top5']:.1%}")
    print(f"  購入レース数={m['n_races']:,}  総点数={m['n_tickets']:,}  "
          f"1レース平均投資={m['avg_stake_per_race']:,.0f}円  最大DD={m['max_dd']:,.0f}円")
    yr = "  ".join(f"{y}:{v:.1%}" for y, v in m["by_year"].items())
    print(f"  年別TOTAL ROI: {yr}")
    print("  ※注意: 控除率は投資額に対する割合なので、点数を増やすこと自体が1円あたり控除率を"
          "上げるわけではない。ROI低下の主因は複合——シムの2・3着順位付けが弱い／組合せ確率が"
          "未校正／点数拡大で低確率・低EV券が増える／三連系の高分散／券種ごとの高控除／最大払戻依存。"
          "『買い目拡大で低品質な組合せへの投資が増え、より高控除・高分散の券種でもあるため全体ROIが低下』が適切。")


def _print_s4_audit(s4_field: dict):
    """[データ整合性] S4 の実点数内訳。8点でないレースは小頭数が原因（重複ではない）ことを示す。"""
    total = sum(s4_field.values())
    full = s4_field.get(8, 0)
    short = total - full
    print("[データ整合性] S4 三連単の実点数内訳（正常な順位列なら 6頭以上で必ず 8点）")
    for pts in sorted(s4_field):
        tag = "(=full)" if pts == 8 else "(小頭数)"
        print(f"  {pts}点: {s4_field[pts]:,}レース {tag}")
    print(f"  → 8点 {full:,} / 8点未満 {short:,}。不足は third_slots が頭数を超える小頭数が原因で、"
          "重複馬番ではない（各レースで validate_ranking 済）。")


def _print_calibration(calib: list):
    """[校正] Sim1位の予測勝率 p1 を帯別に集計し実勝率と比べる（S9 の p1>=0.5 閾値の妥当性）。"""
    print("\n[校正] Sim1位の予測勝率 p1 帯別 実勝率（S9 の p1≥0.50 が『強い軸』判定になっているか）")
    if not calib:
        print("  勝ち馬情報が無く測定不能。")
        return
    bins = [(0.0, 0.2), (0.2, 0.3), (0.3, 0.4), (0.4, 0.5), (0.5, 0.6), (0.6, 0.8), (0.8, 1.01)]
    print(f"  {'予測p1帯':<12}{'レース数':>8}{'平均予測':>9}{'実勝率':>8}")
    for lo, hi in bins:
        sub = [(p, w) for p, w in calib if lo <= p < hi]
        if not sub:
            continue
        pm = sum(p for p, _ in sub) / len(sub)
        wr = sum(w for _, w in sub) / len(sub)
        print(f"  [{lo:.1f},{hi:.1f}){'':<3}{len(sub):>8,}{pm:>9.3f}{wr:>8.3f}")
    hi_bin = [(p, w) for p, w in calib if p >= 0.5]
    if hi_bin:
        wr = sum(w for _, w in hi_bin) / len(hi_bin)
        print(f"  → p1≥0.50 の {len(hi_bin):,}レースの実勝率 {wr:.3f}。"
              + ("0.5 近傍なら閾値は妥当。" if wr >= 0.45 else
                 "予測ほど勝てておらず（過信）、S9 の 0.50 閾値は『強い軸』を選べていない。"))
    else:
        print("  → p1≥0.50 のレースが無い（sim 勝率が潰れ気味）。閾値を下げて帯別に再確認。")


def _print_rank_joint(res: dict):
    """[rank↔joint] 同点数の rank 版と joint 版を並べ、ΔROI(joint−rank) の paired CI で価値を測る。"""
    from src.simulation._ticket_backtest import RANK_JOINT_PAIRS, paired_delta_roi_ci
    print("\n[rank↔joint] 同点数で『MC の順位依存構造を使う価値』を直接比較（ΔROI=joint−rank）")
    print(f"  {'対照(券種)':<20}{'rank ROI':>10}{'joint ROI':>11}{'ΔROI':>9}{'95%CI':>18}")
    for rank_name, joint_name in RANK_JOINT_PAIRS:
        if rank_name not in res or joint_name not in res:
            continue
        _, pr_rank = res[rank_name]
        _, pr_joint = res[joint_name]
        d = paired_delta_roi_ci(pr_joint, pr_rank, n_boot=2000)
        ci = f"[{d['lo']:+.1%},{d['hi']:+.1%}]"
        print(f"  {rank_name.split('_')[0]+'/'+joint_name.split('_')[0]:<20}"
              f"{d['roi_mkt']:>10.1%}{d['roi_sim']:>11.1%}{d['delta']:>+9.1%}{ci:>18}")
    print("  読み: ΔROI CI が有意に正なら『MC の同時確率は周辺順位より価値がある』。0を跨ぐなら"
          "順位以上の情報は使えていない（＝物理シムの2・3着構造は ROI に効かない）。")


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
    ap.add_argument("--oz-dir", default=None,
                    help="JRDB OZ 前売りオッズの .txt フォルダ。市場1番人気対照(購入時点)を有効化。"
                         "無指定なら市場対照は測定不能（確定オッズでの代用は方針違反なので行わない）")
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

    strategies = dict(STRATEGY_TEMPLATES)          # S0_skip も対照として含める
    kw = dict(n_sim=args.n_sim, T=args.T, ability_spread=args.ability_spread,
              ability_sigma=args.ability_sigma, rank_gain=args.rank_gain, seed=args.seed)

    if not args.walk_forward:
        print(f"[全期間] {len(order):,}レース / n_sim={args.n_sim} / rank_gain={args.rank_gain}")
        res, n_ok, all_cands, sim_top, calib, s4_field = _run_strategies(
            featured, order, ret_src, strategies, **kw)
        print(f"有効 sim レース {n_ok:,}\n")
        # [データ整合性] S4 点数不足の実データ内訳（重複ではなく小頭数が原因か確認）
        _print_s4_audit(s4_field)
        # [校正] Sim1位の予測勝率 p1 と実勝率（S9 の p1>=0.5 閾値に意味があるか）
        _print_calibration(calib)
        # [市場対照] 購入時点オッズで市場1番人気を決め、Sim1位と単勝/複勝で並べる
        _market_control(args.oz_dir, featured, order, sim_top, ret_src)
        # [戦略別]
        print("\n[戦略別]")
        _print_table([_strategy_line(n, pb, pr) for n, (pb, pr) in res.items()])
        # [rank↔joint] 同点数で「MC の順位依存構造を使う価値」を直接比較
        _print_rank_joint(res)
        # [券種グループ別 TOTAL] と [ALL TOTAL]（全戦略を同時に全購入した仮想ポートフォリオ）
        _print_total(all_cands, ret_src, order)
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
        tr_res, *_ = _run_strategies(featured.loc[tr_order], tr_order, ret_src, strategies, **kw)
        tr_rows = [_strategy_line(n, pb, pr) for n, (pb, pr) in tr_res.items()]
        # 過去年での選択規準: 除最大ROI 最大（フロック依存を避ける）かつ点数十分
        elig = [r for r in tr_rows if r["n_bets"] >= 100] or tr_rows
        best = max(elig, key=lambda r: r["roi_ex"])
        te_res, *_ = _run_strategies(featured.loc[te_order], te_order, ret_src,
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
