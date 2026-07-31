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


def _model_n_features(eff):
    """fit 済みモデルが期待する特徴量数を辿る（較正ラッパー越しも試す）。無ければ None。"""
    for cand in (eff, getattr(eff, "_base_model", None), getattr(eff, "base_estimator", None),
                 getattr(eff, "estimator", None)):
        if cand is None:
            continue
        for g in (lambda m: int(m.n_features_in_), lambda m: int(m.booster_.num_feature())):
            try:
                n = g(cand)
                if n:
                    return n
            except Exception:  # noqa: BLE001
                continue
    return None


def _lgbm_probs(model, featured, order):
    """本番 LightGBM の較正勝率で (top_by_race, calib) を作る＝MC の代替確率源（差し当たり用）。

    バックテスト featured は結果・メタ列を余分に持つため、モデルの学習特徴量(feat_cols)に厳密整合
    してから較正モデル(effective_model)で直接予測する（score_policy の部分 drop 経由だと余分列が
    残り LightGBM の列数不一致で落ちるため）。top_by_race={race_id: 予測1位馬番}、
    calib=[(年, 勝率ベクトル, 勝者index)]。
    """
    import numpy as np
    import pandas as pd

    from app._model_eval import _DROP_FOR_TRAIN
    from src.constants._results_cols import ResultsCols
    eff = getattr(model, "effective_model", model)
    n_expect = _model_n_features(eff)
    # モデル入力の列名は、学習時に確定した datasets.X_base_train.columns（=588の実列名・学習列順）を
    # 正典とする。現 featured は学習後に増えた特徴量（例 rank_bonus 等）を含み得るので、学習列だけを
    # 厳密に選ぶ（新規列を除外・順序固定）。取れない場合のみ featured − _DROP_FOR_TRAIN へフォールバック。
    feat_names = None
    ds = getattr(model, "datasets", None)
    if ds is not None:
        try:
            feat_names = list(ds.X_base_train.columns)
        except Exception:  # noqa: BLE001
            feat_names = None
    if feat_names:
        missing = [c for c in feat_names if c not in featured.columns]
        print(f"  [契約診断] model_class={type(eff).__name__} n_features_in_={n_expect} "
              f"学習列数={len(feat_names)} featured列数={featured.shape[1]} 不足={len(missing)} "
              f"（学習列を厳密選択）", file=sys.stderr)
        print(f"  [契約診断] 学習列 先頭: {feat_names[:8]}", file=sys.stderr)
        if missing:
            raise ValueError(f"特徴量不一致: 学習{len(feat_names)}列中 {len(missing)}列が featured に"
                             f"無い（0補完は市場検証で禁止）。先頭20: {missing[:20]}")
        X_model = featured.reindex(columns=feat_names)   # 学習列だけ・学習順（新規19列を除外）
    else:
        X_model = featured.drop(list(_DROP_FOR_TRAIN), axis=1, errors="ignore")
        print(f"  [契約診断] datasets 不在→fallback: model入力列数={X_model.shape[1]}"
              f"（featured−_DROP_FOR_TRAIN）", file=sys.stderr)
    if n_expect and X_model.shape[1] != n_expect:
        raise ValueError(
            f"特徴量数不一致: モデル入力 {X_model.shape[1]}列 ≠ 学習 {n_expect}列。差分"
            f"{X_model.shape[1] - n_expect:+d}（学習列選択後も不一致＝datasets とモデルの版ずれを疑う）")
    prob = np.asarray(eff.predict_proba(X_model.values))[:, 1]
    rank_arr = pd.to_numeric(featured[ResultsCols.RANK], errors="coerce").to_numpy()
    tbl = pd.DataFrame({"_rid": featured.index.astype(str),
                        "uma": pd.to_numeric(featured[ResultsCols.UMABAN], errors="coerce"),
                        "prob": prob, "rank": rank_arr})
    # [正常性スモーク] 保存時 Place AUC≈0.807-0.809 を再現できるかで特徴整合を検証（≒0.5 なら破損）。
    from src.simulation._bet_eval import _auc
    valid = tbl.dropna(subset=["rank"])
    place_auc = _auc(list(zip(valid["prob"], (valid["rank"] <= 3).astype(int), strict=False)))
    win_auc = _auc(list(zip(valid["prob"], (valid["rank"] == 1).astype(int), strict=False)))
    smoke = {"place_auc": place_auc, "win_auc": win_auc, "n": int(len(valid)),
             "n_feat": int(X_model.shape[1])}
    winners = _race_winners(featured)
    top_by_race: dict = {}
    calib: list = []
    for rid, g in tbl.groupby("_rid"):
        umas = [int(u) for u in g["uma"] if pd.notna(u)]
        probs = [float(p) for u, p in zip(g["uma"], g["prob"], strict=False) if pd.notna(u)]
        if not umas:
            continue
        top_by_race[rid] = umas[int(np.argmax(probs))]
        w = winners.get(rid)
        if w is not None and w in umas:
            calib.append((rid[:4], probs, umas.index(w)))
    return top_by_race, calib, smoke


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
        rank, probs, umaban, winner = _sim_race_probs(rd, n_sim=n_sim, cfg=cfg,
                                                      ability_spread=ability_spread,
                                                      ability_sigma=ability_sigma,
                                                      rank_gain=rank_gain,
                                                      seed=int(rng.integers(1 << 30)))
        if rank is None:
            continue
        n_ok += 1
        sim_top_by_race[str(rid)] = rank[0]
        if winner is not None and winner in list(umaban):
            p_vec = [float(probs.get(TANSHO, {}).get(int(u), 0.0)) for u in umaban]
            w_idx = list(umaban).index(winner)
            calib.append((str(rid)[:4], p_vec, w_idx))   # (年, 勝率ベクトル, 勝者index)
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


def _load_tyb_win_odds(engine, race_set):
    """raw_jrdb_tyb → {race_id:{馬番:直前単勝オッズ}}。TYB は発走15分前頃(≈T-15)更新＝購入時点・リーク無。

    JRDB TYB(直前情報)の tansho_odds(ZZZ9.9・直前単勝)を市場1番人気決定に使う。年度パック 1999-2025 で
    全レースをカバー＝カバレッジ問題(raw_odds_snapshots 3.9%)を解消。決済は HJC 確定払戻(後決済)のまま。
    """
    import pandas as pd
    from sqlalchemy import text
    try:
        cols = pd.read_sql(text("SELECT * FROM raw_jrdb_tyb LIMIT 1"), engine).columns
    except Exception as e:  # noqa: BLE001
        print(f"  [warn] raw_jrdb_tyb を読めません（TYB 取込済みか確認）: {e}", file=sys.stderr)
        return {}
    if not {"race_id", "umaban", "tansho_odds"}.issubset(set(cols)):
        print(f"  [warn] raw_jrdb_tyb に race_id/umaban/tansho_odds が無い: {list(cols)}",
              file=sys.stderr)
        return {}
    df = pd.read_sql(text("SELECT race_id, umaban, tansho_odds FROM raw_jrdb_tyb"), engine)
    df["race_id"] = df["race_id"].astype(str).str.split(".").str[0]
    df = df[df["race_id"].isin(race_set)]
    df["umaban"] = pd.to_numeric(df["umaban"], errors="coerce")
    df["tansho_odds"] = pd.to_numeric(df["tansho_odds"], errors="coerce")
    df = df.dropna(subset=["umaban", "tansho_odds"])
    df = df[(df["tansho_odds"] > 0) & (df["umaban"] > 0)]
    out: dict = {}
    for rid, g in df.groupby("race_id"):
        out[str(rid)] = {int(u): float(o) for u, o in zip(g["umaban"], g["tansho_odds"],
                                                          strict=False)}
    return out


def _load_db_win_odds(engine, race_set, *, table="raw_odds_snapshots", target_mtp=15):
    """raw_odds_snapshots → {race_id: {馬番: 購入時点単勝}}。締切 target_mtp 分前に最も近いスナップを採る。

    購入時点オッズ＝発走前スナップ。各(race_id,馬番)で minutes_to_post が target_mtp(既定T-15)に
    最も近い（かつ >=0＝発走前の）行を選ぶ。確定オッズは使わない（リーク回避）。列が違えば空返し。
    """
    import pandas as pd
    from sqlalchemy import text
    try:
        cols = pd.read_sql(text(f"SELECT * FROM {table} LIMIT 1"), engine).columns
    except Exception as e:  # noqa: BLE001
        print(f"  [warn] {table} を読めません: {e}", file=sys.stderr)
        return {}
    need = {"race_id", "bet_type", "combo", "odds"}
    if not need.issubset(set(cols)):
        print(f"  [warn] {table} に必要列 {need} が無い（実列: {list(cols)}）。", file=sys.stderr)
        return {}
    has_mtp = "minutes_to_post" in cols
    sel = "race_id, combo, odds" + (", minutes_to_post" if has_mtp else "")
    df = pd.read_sql(text(f"SELECT {sel} FROM {table} WHERE bet_type='tansho'"), engine)
    if df.empty:
        return {}
    df["race_id"] = df["race_id"].astype(str).str.split(".").str[0]
    df = df[df["race_id"].isin(race_set)]
    df["umaban"] = pd.to_numeric(df["combo"], errors="coerce")
    df["odds"] = pd.to_numeric(df["odds"], errors="coerce")
    df = df.dropna(subset=["umaban", "odds"])
    df = df[(df["odds"] > 0) & (df["umaban"] > 0)]
    if has_mtp:
        mtp = pd.to_numeric(df["minutes_to_post"], errors="coerce")
        df = df[mtp >= 0]
        df = df.assign(_key=(pd.to_numeric(df["minutes_to_post"], errors="coerce") - target_mtp).abs())
        df = df.sort_values("_key")                    # target_mtp に近い順→重複keep firstで採用
    out: dict = {}
    for rid, g in df.groupby("race_id"):
        g = g.drop_duplicates("umaban", keep="first")
        out[str(rid)] = {int(u): float(o) for u, o in zip(g["umaban"], g["odds"], strict=False)}
    return out


def _race_winners(featured):
    """featured → {race_id: 勝ち馬番}（着順==1 が一意のレースのみ）。代表性監査・部分群集計に使う。"""
    import pandas as pd

    from src.constants._results_cols import ResultsCols
    df = pd.DataFrame({"rid": featured.index.astype(str),
                       "rank": pd.to_numeric(featured[ResultsCols.RANK], errors="coerce").to_numpy(),
                       "uma": pd.to_numeric(featured[ResultsCols.UMABAN], errors="coerce").to_numpy()})
    out: dict = {}
    for rid, g in df.groupby("rid"):
        w = g[g["rank"] == 1]
        if len(w) == 1 and pd.notna(w["uma"].iloc[0]):
            out[rid] = int(w["uma"].iloc[0])
    return out


def _dist_str(keys, top=6):
    """キー列 → 上位カテゴリの 'k:割合' 文字列（代表性の目視用）。"""
    from collections import Counter
    c = Counter(keys)
    n = sum(c.values()) or 1
    items = sorted(c.items(), key=lambda kv: -kv[1])[:top]
    return "  ".join(f"{k}:{v / n:.0%}" for k, v in items)


def _print_coverage_audit(featured, order, comparable, sim_top, winners, pick_label="Sim1位"):
    """[代表性監査] 市場比較可能レースが全評価レースを代表しているか（偏りの検出）。"""
    import pandas as pd
    nby = pd.Series(featured.index.astype(str)).value_counts().to_dict()   # race_id→頭数
    allids = [str(r) for r in order]
    comp = [str(r) for r in comparable]
    cov = len(comp) / len(allids) if allids else 0.0
    print(f"\n[代表性監査] 市場比較 {len(comp):,} / 全評価 {len(allids):,}（カバレッジ {cov:.1%}）"
          "＝この比較が全体を代表しているか")

    def _row(label, ids):
        yrs = [i[:4] for i in ids]
        trk = [i[4:6] for i in ids]                       # race_id の場コード相当
        fs = [nby.get(i, 0) for i in ids]
        fs_bucket = ["≤7" if n <= 7 else "8-12" if n <= 12 else "13+" for n in fs]
        hit = [1 for i in ids if i in sim_top and winners.get(i) == sim_top[i]]
        hr = len(hit) / len(ids) if ids else 0.0
        avg_fs = sum(fs) / len(fs) if fs else 0.0
        print(f"  {label}: n={len(ids):,} {pick_label}的中={hr:.1%} 平均頭数={avg_fs:.1f}")
        print(f"    年 {_dist_str(yrs)}")
        print(f"    場 {_dist_str(trk)}")
        print(f"    頭数 {_dist_str(fs_bucket)}")

    _row("全評価", allids)
    _row("市場比較", comp)
    print(f"  → 市場比較の年/場/頭数分布・{pick_label}的中率が全評価と大きく違えば偏った標本"
          "（＝ΔROIの判定はさらに不確か）。カバレッジを上げて再監査すること。")


def _market_rank_of(win_odds_race, horse):
    """購入時点オッズでの人気順位（1=1番人気）。horse が無ければ None。"""
    ranked = sorted(win_odds_race.items(), key=lambda kv: kv[1])
    for i, (u, _o) in enumerate(ranked, 1):
        if int(u) == int(horse):
            return i
    return None


def _arm_stats_detailed(per_race, win_odds, pick):
    """単一群の ROI/的中/平均オッズ/除最大ROI/年別ROI を per_race から算出。"""
    st = sum(d["stake"] for d in per_race.values())
    rt = sum(d["returned"] for d in per_race.values())
    hits = sum(d["n_hits"] for d in per_race.values())
    n = len(per_race)
    max_ret = max((d["returned"] for d in per_race.values()), default=0.0)
    roi = rt / st if st else 0.0
    roi_ex = (rt - max_ret) / st if st else 0.0
    avg_odds = (sum(win_odds[r].get(pick[r], 0.0) for r in per_race if r in win_odds and r in pick)
                / n) if n else 0.0
    yr: dict = {}
    for rid, d in per_race.items():
        y = str(rid)[:4]
        a = yr.setdefault(y, [0.0, 0.0])
        a[0] += d["stake"]
        a[1] += d["returned"]
    by_year = {y: (v[1] / v[0] if v[0] else 0.0) for y, v in sorted(yr.items())}
    return {"n": n, "roi": roi, "hit": hits / n if n else 0.0, "avg_odds": avg_odds,
            "roi_ex": roi_ex, "by_year": by_year}


def _print_market_subgroups(win_odds, sim_top, ret_src, pick_label="Sim1位"):
    """[市場×選択差] 予測1位が市場で何番人気かで A/B/C に分け、単勝/複勝成績を出す。

    A=予測1位が市場1番人気 / B=市場1番人気でない / C=市場4番人気以下。一致率の中身を見る。
    """
    from src.constants._bet_types import BetType
    from src.simulation._ticket_backtest import settle_per_race
    common = [r for r in sim_top if r in win_odds]
    if not common:
        return
    mr = {r: _market_rank_of(win_odds[r], sim_top[r]) for r in common}
    groups = {
        f"A:{pick_label}=市場1番人気": [r for r in common if mr[r] == 1],
        f"B:{pick_label}≠市場1番人気": [r for r in common if mr[r] not in (None, 1)],
        f"C:{pick_label}が市場4番人気以下": [r for r in common if mr[r] is not None and mr[r] >= 4],
    }
    print(f"\n[市場×選択差] {pick_label}の市場人気順位別 単勝/複勝成績（一致率の中身・確定払戻で精算）")
    print(f"  {'群':<24}{'件数':>6}{'単ROI':>8}{'単的中':>7}{'複ROI':>8}{'平均O':>7}"
          f"{'単除最大':>9}")
    for label, ids in groups.items():
        if not ids:
            print(f"  {label:<24}{'0':>6}  （該当なし）")
            continue
        pick = {r: sim_top[r] for r in ids}
        prw = settle_per_race(_single_candidates(BetType.TANSHO, pick), ret_src)
        prp = settle_per_race(_single_candidates(BetType.FUKUSHO, pick), ret_src)
        w = _arm_stats_detailed(prw, win_odds, pick)
        p = _arm_stats_detailed(prp, win_odds, pick)
        yr = "  ".join(f"{y}:{v:.0%}" for y, v in w["by_year"].items())
        print(f"  {label:<24}{w['n']:>6,}{w['roi']:>8.1%}{w['hit']:>7.1%}{p['roi']:>8.1%}"
              f"{w['avg_odds']:>7.1f}{w['roi_ex']:>9.1%}   年別単{yr}")
    print(f"  → B/C（市場と違う選択）で十分件数かつ単勝ROIが市場対照を超えるなら、{pick_label}源は"
          "弱い『人気薄選定器』の可能性。ただし少件数では探索的参考（要 walk-forward＋最低的中件数）。")


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


def _market_control(win_odds, sim_top, ret_src, source_label="市場", pick_label="Sim1位"):
    """[市場対照] 購入時点の単勝1番人気 vs 予測1位 を 単勝/複勝で並べ、paired ΔROI CI・一致率を出す。

    リーク規律: 市場1番人気は購入時点オッズ(win_odds)で決定・確定払戻(HJC)で精算。sim1位は as-of sim。
    win_odds={race_id:{馬番:購入時点単勝}}。空なら測定不能を明示（確定オッズでの代用はしない）。
    """
    from src.constants._bet_types import BetType
    from src.simulation._ticket_backtest import (
        market_favorite, paired_delta_roi_ci, settle_per_race,
    )
    sw, sp = f"{pick_label[:3]}-W", f"{pick_label[:3]}-P"
    print(f"[市場対照] 購入時点({source_label})の単勝1番人気 vs {pick_label}（確定払戻で精算）")
    if not win_odds:
        print("  → 購入時点オッズが無い（--tyb / --oz-dir / --odds-db 未指定 or 被りゼロ）。測定不能。"
              "確定オッズでの1番人気代用は方針違反（リーク）なので行わない。")
        return
    fav = market_favorite(win_odds)
    common = [r for r in fav if r in sim_top]
    if not common:
        print(f"  → 市場と評価レースの被りが0（市場 {len(fav):,} / 予測 {len(sim_top):,}）。測定不能。")
        return
    mkt_pick = {r: fav[r] for r in common}
    sim_pick = {r: sim_top[r] for r in common}
    agree = sum(1 for r in common if mkt_pick[r] == sim_pick[r]) / len(common)

    arms = {}
    for label, bt, pick in (("Market-W", BetType.TANSHO, mkt_pick),
                            (sw, BetType.TANSHO, sim_pick),
                            ("Market-P", BetType.FUKUSHO, mkt_pick),
                            (sp, BetType.FUKUSHO, sim_pick)):
        pr = settle_per_race(_single_candidates(bt, pick), ret_src)
        roi, hit, n = _arm_roi(pr)
        avg_odds = (sum(win_odds[r].get(pick[r], 0.0) for r in common if r in win_odds)
                    / len(common))
        arms[label] = {"roi": roi, "hit": hit, "n": n, "avg_odds": avg_odds, "per_race": pr}

    print(f"  評価レース(共通) {len(common):,} / {pick_label}=市場1番人気の一致率 {agree:.1%}"
          + ("  ← 90%超＝予測は市場順位の再表現に近い" if agree >= 0.90 else ""))
    print(f"  {'':12}{'ROI':>8}{'的中率':>8}{'平均オッズ':>10}{'決済N':>7}")
    for label in ("Market-W", sw, "Market-P", sp):
        a = arms[label]
        print(f"  {label:<12}{a['roi']:>8.1%}{a['hit']:>8.1%}{a['avg_odds']:>10.2f}{a['n']:>7,}")
    for tag, sim_l, mkt_l in (("単勝", sw, "Market-W"), ("複勝", sp, "Market-P")):
        d = paired_delta_roi_ci(arms[sim_l]["per_race"], arms[mkt_l]["per_race"], n_boot=2000)
        sig = "有意" if (d["lo"] > 0 or d["hi"] < 0) else "有意でない"
        print(f"  {tag} ΔROI(予測−Market)={d['delta']:+.1%}  95%CI[{d['lo']:+.1%},{d['hi']:+.1%}] "
              f"→ {sig}（0を跨がなければ純増分あり）")
    print("  読み: 一致率≥90%かつ ΔROI CI が 0 を跨ぐなら、予測は市場1番人気の再表現で純増分なし"
          "（＝市場効率の壁）。ΔROI CI が有意に正なら初めて『市場を超える選別』の候補。")


def _print_total(all_cands, ret_src, order, n_strategies):
    """[券種グループ別 TOTAL] と [ALL TOTAL]（全戦略を同時に全購入した仮想ポートフォリオ）。

    金額は円で表示する（決済単位は 1点=1単位=100円なので stake/return は 100円単位→×100で円）。
    """
    from src.constants._units import PAYOUT_UNIT_YEN as U
    from src.simulation._ticket_backtest import (
        BET_GROUP_ORDER, portfolio_metrics, settle_tickets_detailed,
    )
    rows = settle_tickets_detailed(all_cands, ret_src)
    m = portfolio_metrics(rows, race_order=[str(r) for r in order])
    print("\n[券種グループ別 TOTAL]（三連系の大量投資が全券種合算を支配するのを切り分け・金額は円）")
    print(f"  {'グループ':<14}{'点数':>9}{'的中率':>8}{'投資(円)':>13}{'払戻(円)':>14}{'ROI':>8}")
    for g in BET_GROUP_ORDER:
        d = m["by_group"].get(g)
        if not d:
            continue
        hit = d["n_hits"] / d["n_bets"] if d["n_bets"] else 0.0
        print(f"  {g:<14}{d['n_bets']:>9,}{hit:>8.1%}{d['stake'] * U:>13,.0f}"
              f"{d['returned'] * U:>14,.0f}{d['roi']:>8.1%}")
    n_races = m["n_races"]
    tickets = m["n_tickets"]
    print(f"\n[ALL TOTAL]（全{n_strategies}戦略を同時に全購入した仮想ポートフォリオ・投資額加重ROI・"
          f"1点=100円）")
    print(f"  総点数={tickets:,}点  投資={m['total_stake'] * U:,.0f}円  "
          f"払戻={m['total_return'] * U:,.0f}円  損益={m['profit'] * U:+,.0f}円")
    print(f"  ROI={m['roi']:.1%}  除最大1={m['roi_ex_top1']:.1%}  除上位5={m['roi_ex_top5']:.1%}")
    avg_pts = tickets / n_races if n_races else 0.0
    print(f"  購入レース数={n_races:,}  1レース平均={avg_pts:.1f}点／{m['avg_stake_per_race'] * U:,.0f}円  "
          f"最大DD={m['max_dd'] * U:,.0f}円")
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


def _reliability_print(rel, indent="    "):
    for r in rel:
        print(f"{indent}[{r['lo']:.1f},{r['hi']:.1f}){'':<2}{r['n']:>8,}{r['pred']:>10.3f}"
              f"{r['act']:>9.3f}")


def _print_calibration(calib: list, pick_label="Sim1位"):
    """[校正] 予測1位の予測勝率の帯別実勝率＋walk-forward temperature scaling（過信の矯正）。

    calib=[(年, 勝率ベクトル, 勝者index)]。生の過信を示し、過去年で T を fit→翌年へ固定して
    校正後の信頼度改善（NLL/ECE）を出す。確率ベース戦略(S9/EV/joint閾値)は校正後にのみ有効。
    """
    from src.simulation._prob_calibration import (
        ece_top, fit_temperature, nll, reliability_top,
    )
    print(f"\n[校正] {pick_label}の予測勝率 帯別実勝率（生）＋ walk-forward temperature scaling")
    if not calib:
        print("  勝ち馬情報が無く測定不能。")
        return
    races_all = [(p, w) for _y, p, w in calib]
    print(f"  生（校正なし）:{'':<6}{'レース数':>8}{'平均予測':>10}{'実勝率':>9}")
    _reliability_print(reliability_top(races_all, T=1.0))
    print(f"  生 ECE(top)={ece_top(races_all):.3f}  ← 大きいほど過信。確率値を使う規則は校正前は無効。")

    years = sorted({y for y, _p, _w in calib})
    if len(years) < 2:
        print("  年が1つで walk-forward 校正不可（過去年→翌年が要る）。")
        return
    tr, te = years[0], years[-1]
    train = [(p, w) for y, p, w in calib if y == tr]
    test = [(p, w) for y, p, w in calib if y == te]
    if len(train) < 100 or len(test) < 100:
        print(f"  学習{tr}/評価{te}が薄く校正不可。")
        return
    T = fit_temperature(train)
    ece_raw, ece_cal = ece_top(test, T=1.0), ece_top(test, T=T)
    at_bound = T <= 0.31 or T >= 29.9
    # temperature 採用の可否: ECE が有意に改善し、かつ T が境界に張り付いていないときだけ採用。
    adopt = (ece_cal < ece_raw - 0.002) and not at_bound
    print(f"\n  walk-forward: {tr} で T を fit={T:.2f} → {te} で検証"
          + ("（T境界張付＝最適が範囲外）" if at_bound else ""))
    print(f"  {te} 校正前 NLL={nll(test, 1.0):.4f} ECE={ece_raw:.3f} / "
          f"温度後 NLL={nll(test, T):.4f} ECE={ece_cal:.3f}")
    if not adopt:
        reason = ("既に良好で改善なし" if ece_raw < 0.03 else
                  "ECE悪化" if ece_cal >= ece_raw else "T境界張付")
        print(f"  → temperature scaling 非採用（{reason}）。生確率で評価する。")
        T = 1.0
    else:
        print(f"  {te} 校正後 {pick_label} 帯別:{'':<2}{'レース数':>8}{'平均予測':>10}{'実勝率':>9}")
        _reliability_print(reliability_top(test, T=T))
    # p1≥0.50 は採用した尺度（生 or 温度後）で、同一 test 集合の「トップ馬が実際に勝った率」を報告。
    # ＝reliability_top と同一定義（勝者index の合計ではなく 1着一致の 0/1）。表と整合させる。
    from src.simulation._prob_calibration import apply_temperature
    hi = []
    for p, w in test:
        pc = apply_temperature(p, T)
        j = max(range(len(pc)), key=lambda k: pc[k])
        if pc[j] >= 0.5:
            hi.append(1 if j == w else 0)
    scale = "温度後" if adopt else "生"
    if hi:
        print(f"  → {te}・{scale}確率で p1≥0.50 は {len(hi):,}レース（トップ馬の実勝率 {sum(hi)/len(hi):.3f}）。"
              "この帯が 0.5 近傍なら S9 の 0.50 閾値に意味。")
    else:
        print(f"  → {te}・{scale}確率では p1≥0.50 のレースが無い。S9 の 0.50 閾値は実質無効"
              "（閾値を分位ベースに変えるか S9 見送り）。")


def apply_temp_top(p_array, T):
    """校正後の最上位確率（S9 閾値判定用の小ヘルパ）。"""
    from src.simulation._prob_calibration import apply_temperature
    pc = apply_temperature(p_array, T)
    return max(pc) if pc else 0.0


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
    ap.add_argument("--prob-source", choices=("mc", "lgbm"), default="mc",
                    help="確率源。mc=物理モンテカルロ / lgbm=本番LightGBM較正勝率。"
                         "lgbm は市場対照・校正・A/B/C のみ実行（MC専用の券種戦略グリッドはスキップ）")
    ap.add_argument("--model-version", default=None, help="lgbm 時のモデル版名（既定=最新統合）")
    ap.add_argument("--walk-forward", action="store_true", help="過去年で戦略選択→翌年で評価")
    ap.add_argument("--tyb", action="store_true",
                    help="JRDB TYB(直前情報)の直前単勝オッズ(≈T-15)で市場対照を有効化。年度パック"
                         "1999-2025で全レースをカバー＝カバレッジ問題を解消。最優先の市場源。")
    ap.add_argument("--oz-dir", default=None,
                    help="JRDB OZ 前売りオッズの .txt フォルダ。市場1番人気対照(購入時点)を有効化。")
    ap.add_argument("--odds-db", action="store_true",
                    help="DB の購入時点オッズ(raw_odds_snapshots)で市場対照を有効化。"
                         "優先順位は --tyb > --oz-dir > --odds-db。いずれも無指定なら市場対照は測定不能"
                         "（確定オッズでの代用は方針違反なので行わない）")
    ap.add_argument("--odds-table", default="raw_odds_snapshots", help="購入時点オッズのDBテーブル名")
    ap.add_argument("--target-mtp", type=int, default=15, help="採用する締切前分数(T-N)。既定15")
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

    # 市場対照の購入時点オッズ源（優先 --tyb > --oz-dir > --odds-db）
    race_set = set(map(str, order))
    if args.tyb:
        win_odds, src_lbl = _load_tyb_win_odds(engine, race_set), "TYB直前(≈T-15)"
    elif args.oz_dir:
        win_odds, src_lbl = _load_oz_win_odds(args.oz_dir, race_set), "OZ前売り"
    elif args.odds_db:
        win_odds = _load_db_win_odds(engine, race_set, table=args.odds_table,
                                     target_mtp=args.target_mtp)
        src_lbl = f"{args.odds_table}(T-{args.target_mtp})"
    else:
        win_odds, src_lbl = {}, "市場"

    if not args.walk_forward and args.prob_source == "lgbm":
        # 確率源＝本番 LightGBM。市場対照・校正・A/B/C のみ（MC専用の券種戦略グリッドはスキップ）。
        from app._data_loader import (
            find_combined_model_paths, find_model_paths, load_model_by_version,
            load_model_from_path,
        )
        if args.model_version:
            model, mpath = load_model_by_version(args.model_version), args.model_version
        else:
            paths = find_combined_model_paths("models") or find_model_paths("models")
            if not paths:
                print("本番モデルが見つかりません（models/ を確認）", file=sys.stderr)
                return 1
            mpath = paths[0]
            model = load_model_from_path(mpath)
        print(f"[全期間・LightGBM] {len(order):,}レース / model={mpath}")
        top_by_race, calib, smoke = _lgbm_probs(model, featured, order)
        pl = "LGBM1位"
        # [正常性スモーク] 特徴整合が壊れていれば AUC≒0.5 になる。0.807 近傍で初めて ROI を信頼できる。
        pa, wa = smoke["place_auc"], smoke["win_auc"]
        print(f"\n[正常性スモーク] Place AUC={pa:.3f} / Win AUC={wa:.3f}"
              f"（保存時 Place≈0.807-0.809 と一致すれば特徴整合OK・{smoke['n']:,}頭・{smoke['n_feat']}特徴量）")
        if pa is None or pa < 0.65:
            print("  ✗ AUC が低すぎる＝特徴量が正しく渡っていない。市場対照/ROIは無効なので中止。"
                  "学習時と同一の前処理・列順でモデル入力を再構成する必要がある。", file=sys.stderr)
            return 1
        print("  ✓ AUC 正常域。以降の市場対照・校正・A/B/C を本番モデル評価として採用可。")
        _print_calibration(calib, pick_label=pl)
        _market_control(win_odds, top_by_race, ret_src, src_lbl, pick_label=pl)
        if win_odds:
            winners = _race_winners(featured)
            comparable = [r for r in win_odds if r in top_by_race]
            _print_coverage_audit(featured, order, comparable, top_by_race, winners, pick_label=pl)
            _print_market_subgroups(win_odds, top_by_race, ret_src, pick_label=pl)
        print("\n※ 券種戦略グリッド/TOTAL は MC 専用（joint 確率が要る）ためスキップ。"
              "LightGBM は勝ち馬順位付けが強く、市場対照＝『本番モデルは市場を超えるか』の本命検証。")
        return 0

    if not args.walk_forward:
        print(f"[全期間] {len(order):,}レース / n_sim={args.n_sim} / rank_gain={args.rank_gain}")
        res, n_ok, all_cands, sim_top, calib, s4_field = _run_strategies(
            featured, order, ret_src, strategies, **kw)
        print(f"有効 sim レース {n_ok:,}\n")
        # [データ整合性] S4 点数不足の実データ内訳（重複ではなく小頭数が原因か確認）
        _print_s4_audit(s4_field)
        # [校正] Sim1位の予測勝率 p1 と実勝率（S9 の p1>=0.5 閾値に意味があるか）
        _print_calibration(calib)
        # [市場対照] 購入時点オッズ(TYB/OZ/DB)で市場1番人気を決め、Sim1位と対照
        _market_control(win_odds, sim_top, ret_src, src_lbl)
        if win_odds:
            winners = _race_winners(featured)
            comparable = [r for r in win_odds if r in sim_top]
            _print_coverage_audit(featured, order, comparable, sim_top, winners)   # 代表性監査
            _print_market_subgroups(win_odds, sim_top, ret_src)                    # A/B/C 部分群
        # [戦略別]
        print("\n[戦略別]")
        _print_table([_strategy_line(n, pb, pr) for n, (pb, pr) in res.items()])
        # [rank↔joint] 同点数で「MC の順位依存構造を使う価値」を直接比較
        _print_rank_joint(res)
        # [券種グループ別 TOTAL] と [ALL TOTAL]（買い目を出した戦略数で表示）
        n_buying = sum(1 for _n, (pb, _pr) in res.items()
                       if sum(s.n_bets for s in pb.values()) > 0)
        _print_total(all_cands, ret_src, order, n_buying)
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
