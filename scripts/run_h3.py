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
    """KYI から race単位 pace_yosou（非空を採用）。非空の真の競合は fail-closed。返す dict race_id→H/M/S。"""
    import pandas as pd
    if "pace_yosou" not in kyi.columns or "race_id" not in kyi.columns:
        return {}
    v = kyi[["race_id", "pace_yosou"]].copy()
    v["race_id"] = v["race_id"].astype(str)
    v["pace_yosou"] = v["pace_yosou"].astype(str).str.strip().replace("", pd.NA)
    v = v.dropna(subset=["pace_yosou"])
    conflict = v.groupby("race_id")["pace_yosou"].nunique()
    bad = conflict[conflict >= 2]
    if len(bad):
        raise RuntimeError(
            f"H3a fail-closed: pace_yosou の非空競合 race={len(bad)}（例 {list(bad.index[:3])}）"
            "＝レース単位に一意化できない。取込/結合を確認。")
    v = v.drop_duplicates("race_id")
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
             "n_h3b_nonzero": 0, "n_feature_rows": 0, "n_nan_inf": 0,
             "target_key_duplicate_count": int(tgt.duplicated(["race_id", "umaban"]).sum())}
    # feature-only 監査の材料（year, race_id, prior_count, h3a, h3b）
    rows_year, rows_rid, rows_prior, rows_h3a, rows_h3b = [], [], [], [], []
    max_src = None
    for row in tgt.itertuples(index=False):
        ty = int(row.year)
        if ty not in cal_by_testyear:      # 学習確保年（テスト対象外）
            continue
        audit["n_target"] += 1
        hist = hist_by_ketto.get(row.ketto)
        h3a = h3b = 0.0
        ncount = 0
        if hist is not None and len(hist):
            prior = hist[(hist["_d"] < row.sdate) & (hist["_rid"] != row.race_id)]
            ncount = len(prior)
            if ncount:
                ms = assert_strictly_prior(prior, row.sdate, row.race_id,
                                           date_col="_d", race_id_col="_rid")
                audit["n_leak_assert"] += 1
                if ms is not None and (max_src is None or ms > max_src):
                    max_src = ms
                h3a = h3a_pace_aptitude(prior, pace_fc.get(row.race_id), cal_by_testyear[ty])
                h3b = h3b_lap_aptitude(prior)
        if not (np.isfinite(h3a) and np.isfinite(h3b)):    # 欠測→中立0 のはず。inf/nan は失格材料
            audit["n_nan_inf"] += 1
            h3a = 0.0 if not np.isfinite(h3a) else h3a
            h3b = 0.0 if not np.isfinite(h3b) else h3b
        audit["n_feature_rows"] += 1
        audit["n_h3a_nonzero"] += int(h3a != 0.0)
        audit["n_h3b_nonzero"] += int(h3b != 0.0)
        rows_year.append(ty); rows_rid.append(row.race_id); rows_prior.append(ncount)
        rows_h3a.append(h3a); rows_h3b.append(h3b)
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

    # 履歴量分布 / 年別 feature 実効性（race内分散>0 率）/ calibration 行和
    fa = pd.DataFrame({"year": rows_year, "rid": rows_rid, "prior": rows_prior,
                       "h3a": rows_h3a, "h3b": rows_h3b})
    pv = pd.to_numeric(fa["prior"])
    audit["history_volume"] = {
        "0": int((pv == 0).sum()), "1": int((pv == 1).sum()),
        "2-4": int(pv.between(2, 4).sum()), "5+": int((pv >= 5).sum())}
    eff = {}
    for f in ("h3a", "h3b"):
        per = {}
        for y, g in fa.groupby("year"):
            var_by_race = g.groupby("rid")[f].apply(lambda s: float(s.var(ddof=0)) if len(s) > 1 else 0.0)
            per[int(y)] = float((var_by_race > 1e-12).mean()) if len(var_by_race) else 0.0
        eff[f] = per
    audit["feature_effectiveness_var_frac"] = eff
    audit["calibration_rowsums"] = {
        int(ty): {f: round(sum(cal[f].values()), 6) for f in cal} for ty, cal in cal_by_testyear.items()}
    audit["calibration"] = {int(ty): {f: {a: round(p, 5) for a, p in cal[f].items()} for f in cal}
                            for ty, cal in cal_by_testyear.items()}
    out = [r for r in records.values() if r["winner"] is not None and len(r["odds"]) >= 3]
    return out, audit


FEATURE_SCALING = "fold_internal_global"   # 続22 freeze: 学習期間のみで mean/std（neutral-0 保持＋unit化）


def _std_records(recs, feat_name, mu, sd):
    """各 race dict の feats[h][feat_name] を (x−mu)/sd に置換した浅いコピーを返す（leak-safe適用）。"""
    out = []
    for r in recs:
        feats = {h: {feat_name: (float(v.get(feat_name, 0.0)) - mu) / sd}
                 for h, v in r["feats"].items()}
        out.append({**r, "feats": feats})
    return out


def _eval_feature(records, feat_name, *, l2, min_train_years, n_boot, seed):
    """market vs market+単一H3特徴（**fold内 global 標準化**）の rolling-origin OOS。返す (bb, pooled, folds)。

    各 fold で学習期間の全馬行から mean/std を推定し train/test を標準化（leak-safe）。L2=1.0 に対し
    unit スケールを与え検定力を確保。neutral-0 は mean≈0 のためほぼ保持。ΔECE/ΔNLL は手計算。
    """
    import numpy as np
    from src.policies._market_residual import market_probs
    from src.policies._residual_head import fit_residual_head, residual_win_probs
    from src.simulation._model_compare import block_bootstrap_ci, ece, race_nll
    from src.simulation._rolling_origin import rolling_origin_folds

    dnll, blocks = [], []
    fold_rows = []
    pb_flat, pc_flat, y_flat = [], [], []
    nll_b_all, nll_c_all = [], []
    for train, test, year in rolling_origin_folds(records, min_train_years=min_train_years):
        vals = np.array([float(f.get(feat_name, 0.0)) for r in train for f in r["feats"].values()],
                        dtype=float)
        mu = float(vals.mean()) if vals.size else 0.0
        sd = float(vals.std()) if vals.size and vals.std() > 0 else 1.0     # 学習期間のみ
        tr = _std_records(train, feat_name, mu, sd)
        te = _std_records(test, feat_name, mu, sd)
        theta = fit_residual_head(tr, [feat_name], l2=l2)
        use_resid = bool(theta) and not all(v == 0 for v in theta.values())
        fdn = []
        for r in te:
            w = r.get("winner")
            if w is None:
                continue
            pb = market_probs(r["odds"])
            pc = residual_win_probs(r["odds"], r["feats"], theta) if use_resid else pb
            if not (pb and pc):
                continue
            nb, nc = race_nll(pb, w), race_nll(pc, w)
            dnll.append(nc - nb); blocks.append(str(r["race_id"])[:10]); fdn.append(nc - nb)
            nll_b_all.append(nb); nll_c_all.append(nc)
            for h in pb:
                pb_flat.append(float(pb[h])); pc_flat.append(float(pc.get(h, 0.0)))
                y_flat.append(1 if h == w else 0)
        fold_rows.append({"year": year, "n": len(fdn),
                          "d_nll": float(np.mean(fdn)) if fdn else float("nan")})
    bb = block_bootstrap_ci(dnll, blocks, n_boot=max(2000, n_boot), seed=seed)
    pooled = {"d_ece": ece(pc_flat, y_flat) - ece(pb_flat, y_flat) if pb_flat else None,
              "d_nll": float(np.mean(nll_c_all) - np.mean(nll_b_all)) if nll_b_all else float("nan")}
    return bb, pooled, fold_rows


SOURCE_CONTRACT_VERSION = "H3-2026-08-02-c19"
FEATURE_DEFINITION_COMMIT = "cef95ac"     # 特徴定義を freeze したコミット（続20）


def _sha1(s: str) -> str:
    import hashlib
    return hashlib.sha1(str(s).encode("utf-8")).hexdigest()


def _feature_def_hash() -> str:
    """特徴定義（_strictly_prior.py 本体）のハッシュ＝定義が変われば評価を弾くための指紋。"""
    p = Path(__file__).resolve().parents[1] / "src" / "features" / "_strictly_prior.py"
    return _sha1(p.read_bytes().decode("utf-8", "replace"))


def _data_fingerprint(sed, kyi, featured) -> str:
    ymd = "ymd" if "ymd" in sed.columns else "date"
    return _sha1(f"sed={len(sed)}|kyi={len(kyi)}|feat={len(featured)}|"
                 f"races={featured.index.nunique()}|ymin={sed[ymd].min()}|ymax={sed[ymd].max()}")


def _feature_hash(records) -> str:
    """生成された特徴値の指紋（race_id,馬番,h3a,h3b を丸めて正準化）。評価時に一致検証。"""
    parts = []
    for r in sorted(records, key=lambda x: x["race_id"]):
        for ub in sorted(r["feats"]):
            f = r["feats"][ub]
            parts.append(f"{r['race_id']}:{ub}:{round(f['h3a'], 6)}:{round(f['h3b'], 6)}")
    return _sha1("|".join(parts))


def _feature_audit(records, audit, *, min_train_years):
    """feature-only 監査の合格条件を評価し (checks, passed, blockers) を返す（ΔNLL は見ない）。"""
    from src.constants._model_category import central_index_mask
    import pandas as pd
    rids = [r["race_id"] for r in records]
    nar = int((~pd.Series(central_index_mask(pd.Index(rids).astype(str)))).sum()) if rids else 0
    calib_ok = all(abs(v - 1.0) < 1e-6 for m in audit.get("calibration_rowsums", {}).values()
                   for v in m.values())
    eff = audit.get("feature_effectiveness_var_frac", {})
    h3a_has_var = any(v > 0 for v in eff.get("h3a", {}).values())
    h3b_has_var = any(v > 0 for v in eff.get("h3b", {}).values())
    checks = {
        "target_key_duplicate_count": audit["target_key_duplicate_count"],
        "nar_rows": nar,
        "temporal_violations": 0,                        # 生成完走＝全件 assert 通過（違反時は例外停止）
        "feature_nan_inf": audit["n_nan_inf"],
        "completeness_all_targets": audit["n_feature_rows"] == audit["n_target"],
        "calibration_rowsums_ok": calib_ok,
        "h3a_all_zero": audit["n_h3a_nonzero"] == 0,
        "h3b_all_zero": audit["n_h3b_nonzero"] == 0,
        "h3a_has_race_variance": h3a_has_var,
        "h3b_has_race_variance": h3b_has_var,
    }
    blockers = []
    if checks["target_key_duplicate_count"] != 0:
        blockers.append("target (race_id,馬番) 重複あり")
    if nar != 0:
        blockers.append(f"NAR 行 {nar}（JRA限定違反）")
    if checks["feature_nan_inf"] != 0:
        blockers.append(f"NaN/inf 特徴 {checks['feature_nan_inf']}")
    if not checks["completeness_all_targets"]:
        blockers.append("完全性違反（全 eligible target に特徴が無い）")
    if not calib_ok:
        blockers.append("calibration 行和≠1")
    if checks["h3a_all_zero"] or not h3a_has_var:
        blockers.append("H3a が全ゼロ/全年 race内分散0＝構造的に検定不能")
    if checks["h3b_all_zero"] or not h3b_has_var:
        blockers.append("H3b が全ゼロ/全年 race内分散0＝構造的に検定不能")
    return checks, (len(blockers) == 0), blockers


def _run_generation(args):
    """featured/SED/KYI を読み、H3 特徴を生成して (records, audit, fingerprints) を返す。"""
    from app._model_eval import load_featured_data
    feat = load_featured_data(args.featured) if args.featured else load_featured_data()
    if feat is None or feat.empty:
        raise RuntimeError("featured を読めません（ローカルで実行）")
    from src.jrdb._store import JrdbStore
    store = JrdbStore()
    sed, kyi = store.read("SED"), store.read("KYI")
    records, audit = build_h3_records(sed, kyi, feat, min_train_years=args.min_train_years)
    fp = {"feature_definition_hash": _feature_def_hash(),
          "data_fingerprint": _data_fingerprint(sed, kyi, feat),
          "generated_feature_hash": _feature_hash(records)}
    return records, audit, fp


def _do_audit(args) -> int:
    import json
    from src.features._strictly_prior import ALPHA_DIRICHLET, H3_SHRINK_K, THREE_F_MAX, THREE_F_MIN
    print("=" * 82)
    print("H3 feature-only 監査（--audit-only・ΔNLL/ECE/p は計算しない＝性能を見る前のゲート）")
    records, audit, fp = _run_generation(args)
    checks, passed, blockers = _feature_audit(records, audit, min_train_years=args.min_train_years)
    manifest = {
        "feature_definition_commit": FEATURE_DEFINITION_COMMIT,
        "source_contract_version": SOURCE_CONTRACT_VERSION,
        **fp,
        "n_target": audit["n_target"], "n_feature_rows": audit["n_feature_rows"],
        "n_records": len(records),
        "lambda": H3_SHRINK_K, "pace_calibration_alpha": ALPHA_DIRICHLET,
        "feature_scaling": FEATURE_SCALING,
        "ato3f_valid_range": [THREE_F_MIN, THREE_F_MAX],
        "strictly_prior_violation_count": 0,
        "max_source_ymd": audit["max_source_ymd"],
        "history_volume": audit["history_volume"],
        "feature_effectiveness_var_frac": audit["feature_effectiveness_var_frac"],
        "calibration": audit["calibration"],
        "checks": checks,
        "audit_result": "PASS" if passed else "FAIL",
    }
    out_path = Path(args.manifest_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[生成] target={audit['n_target']:,} feature行={audit['n_feature_rows']:,} "
          f"records={len(records):,} leak-assert={audit['n_leak_assert']:,} "
          f"max_source_ymd={audit['max_source_ymd']}")
    print(f"[履歴量] 0走={audit['history_volume']['0']:,} 1走={audit['history_volume']['1']:,} "
          f"2-4走={audit['history_volume']['2-4']:,} 5+走={audit['history_volume']['5+']:,}")
    print(f"[非ゼロ] H3a={audit['n_h3a_nonzero']:,} H3b={audit['n_h3b_nonzero']:,}  "
          f"NaN/inf={audit['n_nan_inf']}")
    print("[年別 race内分散>0 率]")
    for f in ("h3a", "h3b"):
        per = audit["feature_effectiveness_var_frac"][f]
        print(f"  {f}: " + "  ".join(f"{y}:{v:.3f}" for y, v in sorted(per.items())))
    print("[calibration 行和(=1確認)] " + str(audit["calibration_rowsums"]))
    print("\n[監査項目]")
    for k, v in checks.items():
        print(f"  {k:<28} = {v}")
    print(f"\n判定: audit_result = {manifest['audit_result']}")
    if not passed:
        print("  ブロッカー: " + " / ".join(blockers))
        print("  → 構造的に検定不能。評価に進めない（生成/ソースを修正して再監査）。")
        return 5
    print(f"  manifest 保存: {out_path}")
    print("  → PASS。評価は次で**一度だけ**:")
    print(f"     python scripts/run_h3.py --evaluate --audit-manifest {out_path} "
          f"--n-boot {args.n_boot} --seed {args.seed}")
    return 0


def _do_evaluate(args) -> int:
    import json
    from src.features._strictly_prior import H3_SHRINK_K
    from src.simulation._model_compare import holm_correction
    from src.training._provenance import assert_jra_only

    mpath = Path(args.audit_manifest)
    if not mpath.exists():
        print(f"manifest がありません: {mpath}（先に --audit-only）", file=sys.stderr)
        return 2
    manifest = json.loads(mpath.read_text(encoding="utf-8"))
    if manifest.get("audit_result") != "PASS":
        print(f"manifest の audit_result={manifest.get('audit_result')}≠PASS＝評価不可。", file=sys.stderr)
        return 5

    print("=" * 82)
    print("H3 探索的OOS評価（--evaluate・manifest 一致検証後に一度だけ ΔNLL を計算）")
    records, audit, fp = _run_generation(args)
    # manifest 一致検証（特徴定義・データ・生成結果が監査時と同一か）
    mism = [k for k in ("feature_definition_hash", "data_fingerprint", "generated_feature_hash")
            if manifest.get(k) != fp[k]]
    if mism:
        print(f"[STOP] manifest 不一致 {mism}＝監査後にコード/データ/特徴が変化。再監査せよ。", file=sys.stderr)
        for k in mism:
            print(f"   {k}: manifest={manifest.get(k)[:12]}… now={fp[k][:12]}…", file=sys.stderr)
        return 6
    print(f"[一致検証 OK] feature_def/data/feature の3ハッシュが manifest と一致。records={len(records):,}")
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
    print(f"\n=== H3 探索的OOS（Holm family=2・開催場×日 block・B={b_used:,}/seed={args.seed}）===")
    print(f"  freeze: H3a=calibrated P(z)×市場残差(λ={H3_SHRINK_K}) H3b=race内上りpct(λ={H3_SHRINK_K}) "
          f"L2={args.l2} MES={args.mes}  最小到達可能 p={1.0/(b_used+1):.2e}  estimand=race-weighted ΔNLL")
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
    # 内部値（±0.000000 表示の下の実値を科学表記で残す＝再現記録の完成・表示精度の修正のみ）
    print("\n  --- 内部値（科学表記）---")
    for fn in ("h3a", "h3b"):
        bb, pooled = results[fn]["bb"], results[fn]["pooled"]
        de = pooled.get("d_ece")
        de_s = f"{de:.3e}" if isinstance(de, float) else "n/a"
        print(f"  [{fn}] ΔNLL={bb['mean']:.3e}  CI[{bb['lo']:.3e},{bb['hi']:.3e}]  "
              f"ΔECE={de_s}  p={bb.get('p_improve', float('nan')):.4f}")
    print("\n  --- fold 別 ΔNLL ---")
    for fn in ("h3a", "h3b"):
        cells = [f"{f['year']}:{f['d_nll']:+.5f}" if isinstance(f.get("d_nll"), float)
                 else f"{f['year']}:n/a" for f in results[fn]["folds"]]
        print(f"  [{fn}] {'  '.join(cells)}")
    tyears = sorted({f["year"] for f in results["h3a"]["folds"]})
    ryears = sorted({r["year"] for r in records})
    warm = [y for y in ryears if y < (tyears[0] if tyears else 9999)]
    print(f"\n[評価期間] warm-up/初期学習={warm}  test folds={tyears}  "
          f"records pool={ryears[0] if ryears else '?'}-{ryears[-1] if ryears else '?'}"
          f"（{len(records):,}レース・全てが test 観測ではない）")
    print("[境界] 探索的 rolling-origin OOS（P2結果を見て発案）。結果を見て hard化/λ/窓/前後半差/合成を")
    print("        同期間で再試行しない＝多重探索。採用候補は未見期間(2027 or freeze後 prospective)で確認。")
    print("        B frozen 残差ヘッドは無変更で別レーン。合成は別仮説。")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="H3 馬別発走前適性（監査と評価を分離・freeze済）")
    ap.add_argument("--featured", default=None)
    ap.add_argument("--audit-only", action="store_true",
                    help="特徴生成＋feature-only監査＋manifest保存のみ（既定・ΔNLLを見ない）")
    ap.add_argument("--evaluate", action="store_true",
                    help="manifest 一致検証後に一度だけ ΔNLL 評価（--audit-manifest 必須）")
    ap.add_argument("--audit-manifest", default=None, help="--evaluate 時に照合する manifest パス")
    ap.add_argument("--manifest-out", default="artifacts/h3_feature_audit.json")
    ap.add_argument("--l2", type=float, default=1.0)
    ap.add_argument("--mes", type=float, default=1e-3)
    ap.add_argument("--min-train-years", type=int, default=3)
    ap.add_argument("--n-boot", type=int, default=20000)
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    try:
        if args.evaluate:
            if not args.audit_manifest:
                print("--evaluate には --audit-manifest が必須（先に --audit-only で PASS を得る）",
                      file=sys.stderr)
                return 2
            return _do_evaluate(args)
        # 既定は監査のみ（--evaluate が無い限り ΔNLL/p を計算しない＝性能を先に見ない設計）
        return _do_audit(args)
    except RuntimeError as e:
        print(f"[エラー] {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
