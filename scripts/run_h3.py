"""H3: 馬別・発走前適性（H3a ペース / H3b 終い）の生成＋探索的OOS評価（P2と別の新規仮説）。

**freeze 済（続19・gate A/B 通過）**: H3a=fold内 calibrated P(z_actual|pace_yosou)×strictly-prior な
SED実測ペース状態別の馬別市場残差 r=(人気−着順)/(N−1)（λ=5・ijo≠0除外）。H3b=race内 上り(ato3f)
percentile s=0.5−pct の縮約（λ=5・ato3f 1特徴のみ）。履歴/予想欠測→0（中立）。

主ソースは SED（ketto+race_id+ymd＋race_pace/chakujun/kakutei_ninki/ato3f）＋KYI（pace_yosou）。
horse_results は使わない（race_id 無・ペース疎のため・続17-18）。**本番生成時に全target行でリークを
assert**（max(source_ymd)<target_ymd・race_id≠target・同日履歴=0／違反1件で停止）。

評価は事前固定: H3a vs market / H3b vs market・Holm family=2・JRA-only nar_rows=0・rolling-origin・
L2=1.0・MES=0.001・ECE許容+0.005・開催場×日 block bootstrap・race-weighted ΔNLL・B=20,000/seed=0。
**2018-2026 は探索的OOS**（P2結果を見て発案＝confirmatory でない）。結果を見て hard化/λ/窓/前後半差/
合成を同期間で試さない。B frozen 残差ヘッドは無変更で別レーン待機。featured/SED/KYI はローカル成果物。
純部は tests/features/test_strictly_prior.py で検証済。

使い方: python scripts/run_h3.py [--n-boot 20000 --seed 0]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

JRA_PLACES = {f"{i:02d}" for i in range(1, 11)}


def _precompute_sed(sed):
    """SED に H3 の per-row 中間量を付与: _z(race_pace), _perf(市場残差), _agari_pct(race内上りpct), _d(日付)。"""
    import pandas as pd
    from src.features._strictly_prior import (sed_market_perf, sed_pace_state,
                                              sed_race_percentile_ato3f)
    rid = "race_id" if "race_id" in sed.columns else "race_key"
    df = sed.copy()
    df["_rid"] = df[rid].astype(str)
    df["_z"] = sed_pace_state(df["race_pace"]).to_numpy()
    df["_perf"] = sed_market_perf(df.rename(columns={rid: "race_id"})).to_numpy()
    df["_agari_pct"] = sed_race_percentile_ato3f(df.rename(columns={rid: "race_id"})).to_numpy()
    ymd = "ymd" if "ymd" in df.columns else "date"
    df["_d"] = pd.to_datetime(df[ymd].astype(str), errors="coerce", format="%Y%m%d")
    df["ketto"] = df["ketto"].astype(str)
    return df[["ketto", "_rid", "_d", "_z", "_perf", "_agari_pct"]]


def _race_pace_yosou(kyi):
    """KYI から race単位 pace_yosou（非空を優先・空は欠測）。返す dict race_id→H/M/S。"""
    import pandas as pd
    if "pace_yosou" not in kyi.columns or "race_id" not in kyi.columns:
        return {}
    v = kyi[["race_id", "pace_yosou"]].copy()
    v["race_id"] = v["race_id"].astype(str)
    v["pace_yosou"] = v["pace_yosou"].astype(str).str.strip().replace("", pd.NA)
    v = v.dropna(subset=["pace_yosou"]).drop_duplicates("race_id")
    return dict(zip(v["race_id"], v["pace_yosou"]))


def build_h3_records(sed, kyi, featured, *, min_train_years=3):
    """eligible target(featured: year>=2015 & JRA & 有効馬番)へ H3a/H3b を strictly-prior 生成。

    fold内 calibration（学習年の KYI×SED race_pace）を使い、各 target 行で**全件リーク assert**。
    返す (records, audit)。records: {race_id, year, winner, odds, feats:{馬番:{h3a,h3b}}}。
    """
    import numpy as np
    import pandas as pd
    from src.constants._results_cols import ResultsCols
    from src.features._strictly_prior import (assert_strictly_prior, fit_pace_calibration,
                                              h3a_pace_aptitude, h3b_lap_aptitude)

    sp = _precompute_sed(sed)
    pace_fc = _race_pace_yosou(kyi)
    # ketto ごとに時系列ソートした履歴（strictly-prior スライス用）
    sp = sp.sort_values(["ketto", "_d"])
    hist_by_ketto = {k: g.reset_index(drop=True) for k, g in sp.groupby("ketto")}

    # target（featured eligible）＋ SED から ketto/ymd を (race_id,馬番) で引く。
    # .to_numpy() で index を剥がし race_id を純粋な列にする（index/列の曖昧回避）。
    rid = featured.index.astype(str).to_numpy()
    rid_s = pd.Series(rid)
    tgt = pd.DataFrame({
        "race_id": rid,
        "umaban": pd.to_numeric(featured.get(ResultsCols.UMABAN), errors="coerce").to_numpy(),
        "odds": pd.to_numeric(featured.get(ResultsCols.TANSHO_ODDS), errors="coerce").to_numpy(),
        "rank": pd.to_numeric(featured.get(ResultsCols.RANK), errors="coerce").to_numpy(),
        "year": pd.to_numeric(rid_s.str[:4], errors="coerce").to_numpy(),
        "place": rid_s.str[4:6].to_numpy(),
    })
    elig = (tgt["year"] >= 2015) & tgt["place"].isin(JRA_PLACES) & tgt["umaban"].notna()
    tgt = tgt[elig.to_numpy()].copy()
    # SED から (race_id,馬番)->(ketto,_d) を引く
    sedm = sed.copy()
    rid_c = "race_id" if "race_id" in sedm.columns else "race_key"
    sedm["race_id"] = sedm[rid_c].astype(str)
    sedm["umaban"] = pd.to_numeric(sedm["umaban"], errors="coerce")
    sedm["ketto"] = sedm["ketto"].astype(str)
    ymd = "ymd" if "ymd" in sedm.columns else "date"
    sedm["_d"] = pd.to_datetime(sedm[ymd].astype(str), errors="coerce", format="%Y%m%d")
    key2 = (sedm[["race_id", "umaban", "ketto", "_d"]]
            .rename(columns={"_d": "sdate"})     # itertuples は先頭_の列名を壊すので valid 名へ
            .drop_duplicates(["race_id", "umaban"]))
    tgt = tgt.merge(key2, on=["race_id", "umaban"], how="inner")

    years = sorted(y for y in tgt["year"].dropna().unique())
    test_years = years[min_train_years:]
    # fold ごと calibration（学習年のみ）
    cal_by_testyear = {}
    for ty in test_years:
        train_mask = sp["_d"].dt.year < ty
        tr = sp[train_mask.to_numpy()]
        # race単位: race_id→(pace_yosou, race_pace実測z)
        rp = tr.drop_duplicates("_rid")[["_rid", "_z"]]
        fy = [pace_fc.get(r) for r in rp["_rid"]]
        cal_by_testyear[ty] = fit_pace_calibration(fy, rp["_z"].to_numpy())

    records = {}
    audit = {"n_target": 0, "n_leak_assert": 0, "max_source_ymd": None, "n_h3a_nonzero": 0,
             "n_h3b_nonzero": 0}
    max_src = None
    for row in tgt.itertuples(index=False):
        ty = int(row.year)
        if ty not in cal_by_testyear:      # 学習確保年（テスト対象外）
            continue
        audit["n_target"] += 1
        hist = hist_by_ketto.get(row.ketto)
        h3a = h3b = 0.0
        if hist is not None and len(hist):
            prior = hist[(hist["_d"] < row.sdate) & (hist["_rid"] != row.race_id)]
            if len(prior):
                ms = assert_strictly_prior(prior, row.sdate, row.race_id,
                                           date_col="_d", race_id_col="_rid")
                audit["n_leak_assert"] += 1
                if ms is not None and (max_src is None or ms > max_src):
                    max_src = ms
                h3a = h3a_pace_aptitude(prior, pace_fc.get(row.race_id),
                                        cal_by_testyear[ty])
                h3b = h3b_lap_aptitude(prior)
        if h3a != 0.0:
            audit["n_h3a_nonzero"] += 1
        if h3b != 0.0:
            audit["n_h3b_nonzero"] += 1
        o = float(row.odds) if row.odds == row.odds and row.odds > 0 else None
        if o is None:
            continue
        rec = records.setdefault(row.race_id, {"race_id": row.race_id, "year": ty,
                                               "winner": None, "odds": {}, "feats": {}})
        ub = int(row.umaban)
        rec["odds"][ub] = o
        rec["feats"][ub] = {"h3a": float(h3a), "h3b": float(h3b)}
        if row.rank == 1:
            rec["winner"] = ub
    audit["max_source_ymd"] = None if max_src is None else str(max_src.date())
    out = [r for r in records.values() if r["winner"] is not None and len(r["odds"]) >= 3]
    return out, audit


def _eval_feature(records, feat_name, *, l2, min_train_years, n_boot, seed):
    """market vs market+単一H3特徴 の rolling-origin OOS。返す (bb, pooled, folds)。"""
    from src.policies._market_residual import market_probs
    from src.policies._residual_head import fit_residual_head, residual_win_probs
    from src.simulation._model_compare import block_bootstrap_ci, race_nll
    from src.simulation._rolling_origin import rolling_origin_compare, rolling_origin_folds

    def fit_b(_t):
        return None

    def prob_b(_p, r):
        return market_probs(r["odds"])

    def fit_c(train):
        return fit_residual_head(train, [feat_name], l2=l2)

    def prob_c(theta, r):
        if not theta or all(v == 0 for v in theta.values()):
            return market_probs(r["odds"])
        return residual_win_probs(r["odds"], r["feats"], theta)

    res = rolling_origin_compare(records, fit_b, prob_b, fit_c, prob_c,
                                 min_train_years=min_train_years, k_extra_params=1, n_boot=n_boot,
                                 seed=seed)
    dnll, blocks = [], []
    for train, test, _y in rolling_origin_folds(records, min_train_years=min_train_years):
        theta = fit_c(train)
        for r in test:
            if r.get("winner") is None:
                continue
            pb, pc = prob_b(None, r), prob_c(theta, r)
            if pb and pc:
                dnll.append(race_nll(pc, r["winner"]) - race_nll(pb, r["winner"]))
                blocks.append(str(r["race_id"])[:10])
    bb = block_bootstrap_ci(dnll, blocks, n_boot=max(2000, n_boot), seed=seed)
    return bb, res["pooled"], res["folds"]


def main() -> int:
    from app._model_eval import load_featured_data
    from src.features._strictly_prior import H3_SHRINK_K
    from src.simulation._model_compare import holm_correction
    from src.training._provenance import assert_jra_only

    ap = argparse.ArgumentParser(description="H3 馬別発走前適性（生成＋探索的OOS・事前登録freeze済）")
    ap.add_argument("--featured", default=None)
    ap.add_argument("--l2", type=float, default=1.0)
    ap.add_argument("--mes", type=float, default=1e-3)
    ap.add_argument("--min-train-years", type=int, default=3)
    ap.add_argument("--n-boot", type=int, default=20000)
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    feat = load_featured_data(args.featured) if args.featured else load_featured_data()
    if feat is None or feat.empty:
        print("featured を読めません（ローカルで実行）", file=sys.stderr)
        return 2
    try:
        from src.jrdb._store import JrdbStore
        store = JrdbStore()
        sed, kyi = store.read("SED"), store.read("KYI")
    except Exception as e:  # noqa: BLE001
        print(f"SED/KYI を読めません（JRDB 取込が必要）: {e}", file=sys.stderr)
        return 2

    print("=" * 78)
    print("H3 生成（SED+KYI・ketto・strictly-prior 全件assert）＋探索的OOS評価（freeze済）")
    print(f"[freeze] H3a=calibrated P(z)×市場残差(λ={H3_SHRINK_K})  H3b=race内上りpct(λ={H3_SHRINK_K})  "
          f"L2={args.l2} MES={args.mes} B={args.n_boot} seed={args.seed}")
    records, audit = build_h3_records(sed, kyi, feat, min_train_years=args.min_train_years)
    print(f"[生成] target={audit['n_target']:,}  leak-assert通過={audit['n_leak_assert']:,}  "
          f"max_source_ymd={audit['max_source_ymd']}  H3a非ゼロ={audit['n_h3a_nonzero']:,}  "
          f"H3b非ゼロ={audit['n_h3b_nonzero']:,}")
    print(f"[records] {len(records):,} レース（勝ち馬確定・3頭以上）")
    if not records:
        print("records 空。", file=sys.stderr)
        return 3

    # provenance: JRA限定を実データで強制
    try:
        nar = assert_jra_only([r["race_id"] for r in records])
        print(f"[provenance] JRA限定確定 nar_rows={nar}（fail-closed）")
    except RuntimeError as e:
        print(f"[FAIL-CLOSED] {e}", file=sys.stderr)
        return 4

    ECE_TOL = 5e-3
    results = {}
    for fn in ("h3a", "h3b"):
        bb, pooled, folds = _eval_feature(records, fn, l2=args.l2,
                                          min_train_years=args.min_train_years,
                                          n_boot=args.n_boot, seed=args.seed)
        results[fn] = {"bb": bb, "pooled": pooled, "folds": folds}

    holm = {h["name"]: h for h in holm_correction(
        [(fn, results[fn]["bb"].get("p_improve", float("nan"))) for fn in ("h3a", "h3b")], alpha=0.05)}

    b_used = max(2000, args.n_boot)
    print("\n=== H3 探索的OOS（Holm family=2・開催場×日 block・B={:,}/seed={}）===".format(b_used, args.seed))
    print(f"  最小到達可能 p=1/(B+1)={1.0/(b_used+1):.2e}  estimand=race-weighted 平均ΔNLL")
    print(f"  {'特徴':<6}{'ΔNLL(mean)':>13}{'95%CI':>26}{'p':>9}{'p_Holm':>9}{'ΔECE':>11}{'判定':>16}")
    for fn in ("h3a", "h3b"):
        bb, pooled = results[fn]["bb"], results[fn]["pooled"]
        de = pooled.get("d_ece")
        h = holm.get(fn, {})
        sig = (bb["hi"] < 0) and h.get("reject", False)
        practical = abs(bb["mean"]) >= args.mes
        ece_ok = (de is None) or (de <= ECE_TOL)
        verdict = ("✅採用候補" if sig and practical and ece_ok
                   else "🟡統計のみ<MES" if sig and ece_ok else "❌改善なし")
        ci = f"[{bb['lo']:+.6f},{bb['hi']:+.6f}]"
        de_s = f"{de:+.6f}" if isinstance(de, float) else "n/a"
        print(f"  {fn:<6}{bb['mean']:>+13.6f}{ci:>26}{h.get('p', float('nan')):>9.4f}"
              f"{h.get('p_holm', float('nan')):>9.4f}{de_s:>11}{verdict:>16}")

    print("\n  --- fold 別 ΔNLL ---")
    for fn in ("h3a", "h3b"):
        cells = [f"{f['year']}:{f['d_nll']:+.5f}" if isinstance(f.get("d_nll"), float)
                 else f"{f['year']}:n/a" for f in results[fn]["folds"]]
        print(f"  [{fn}] {'  '.join(cells)}")

    print("\n[境界] 2018-2026 は探索的OOS（P2結果を見て発案）。結果を見て hard化/λ/窓/前後半差/合成を")
    print("        同期間で再試行しない＝多重探索。採用候補は未見期間(2027 or freeze後 prospective)で確認。")
    print("        B frozen 残差ヘッドは無変更で別レーン。合成は別仮説。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
