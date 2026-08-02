"""B_RESIDUAL_HEAD_2027_CONFIRM: B 残差ヘッドの 2027 未見期間 confirmation（事前登録・凍結）。

研究フェーズ(P1/A/B/P2/H3)で唯一の最強 positive だった B 残差ヘッドを、**2027 JRA レースのみ**で
out-of-period 確認する。特徴集合・変換・L2・欠測処理・interaction は**一切変えない**（凍結）。
2026 が完結してから、凍結仕様のまま 2026-12-31 までのデータで**一度だけ**学習し、2027 で評価する。

H3 と同じ **manifest-bound の audit→evaluate 分離**: 既定 --audit-only（2027 データ完全性ゲート＋
manifest 保存・性能を見ない）／--evaluate --audit-manifest（3+ハッシュ一致検証後に一度だけ ΔNLL）。
interim looks=0（2027 全開催終了後に一度だけ）。Primary は m=1（B 単独ゆえ Holm 不要）。

**単位**: ΔNLL は自然対数＝**nats/race**（race_nll=−ln P）。MES=0.001 nats/race。
featured はローカル成果物。純部は tests/scripts/test_run_residual_head_2027.py で検証。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# ---- 凍結仕様（事前登録・結果を見て変えない）--------------------------------------------------
FROZEN = {
    "hypothesis_id": "B_RESIDUAL_HEAD_2027_CONFIRM",
    "features": ["jrdb_idm", "jrdb_kishu_idx", "jrdb_joho_idx", "wet_rel_rank", "kinryo_per_weight"],
    "l2": 1.0,
    "mes_dnll": 0.001,              # nats/race（自然対数）
    "ece_tolerance": 0.005,
    "test_year": 2027,
    "bootstrap_blocks": "venue_x_date",   # race_id[:10]
    "bootstrap_repetitions": 20000,
    "bootstrap_seed": 0,
    "estimand": "race_weighted_mean_dnll",
    "jra_only": True,
    "nar_rows_required": 0,
    "interim_looks": 0,
    "data_contract_version": "H3B-2027-v1",
    "preprocessing": "within_race_zscore(build_residual_records)",  # B 原仕様・変更なし
}
MIN_WINNER_RATE = 0.995            # 2027 の 1着1頭率ゲート


def _sha1(s) -> str:
    import hashlib
    return hashlib.sha1(str(s).encode("utf-8")).hexdigest()


def _feature_def_hash() -> str:
    base = Path(__file__).resolve().parents[1] / "src" / "policies"
    return _sha1((base / "_residual_head.py").read_bytes().decode("utf-8", "replace")
                 + (base / "_market_residual.py").read_bytes().decode("utf-8", "replace"))


def _training_code_hash() -> str:
    return _sha1(Path(__file__).read_bytes().decode("utf-8", "replace"))


def _data_fingerprint(featured) -> str:
    rid = featured.index.astype(str)
    return _sha1(f"rows={len(featured)}|races={rid.nunique()}|"
                 f"ymin={rid.min()}|ymax={rid.max()}")


def _feature_hash(records) -> str:
    parts = []
    for r in sorted(records, key=lambda x: x["race_id"]):
        for ub in sorted(r["feats"]):
            fv = r["feats"][ub]
            parts.append(f"{r['race_id']}:{ub}:" + ":".join(
                f"{k}={round(float(fv.get(k, 0.0)), 6)}" for k in FROZEN["features"]))
    return _sha1("|".join(parts))


def verdict(dnll_mean, ci_hi, d_ece, *, mes=FROZEN["mes_dnll"], ece_tol=FROZEN["ece_tolerance"]):
    """事前判定規則（結果を見て変えない）。🟢 confirmed / 🟡 replicated sub-MES / ❌ not confirmed。"""
    ece_ok = (d_ece is None) or (d_ece <= ece_tol)
    if ci_hi < 0 and dnll_mean <= -mes and ece_ok:
        return "🟢 Confirmed（採用候補・ROIは別仮説）"
    if ci_hi < 0 and -mes < dnll_mean < 0 and ece_ok:
        return "🟡 Replicated but sub-MES"
    return "❌ Not confirmed"


def _build_records(featured):
    from scripts.run_residual_head import build_residual_records
    return build_residual_records(featured, FROZEN["features"], jra_only=FROZEN["jra_only"])


def _data_gate_2027(featured, records):
    """2027 データ完全性ゲート（性能を見る前・spec 準拠）。返す (checks, passed, blockers)。"""
    import pandas as pd
    from src.constants._model_category import central_index_mask
    from src.constants._results_cols import ResultsCols

    rid = pd.Series(featured.index.astype(str))
    y = pd.to_numeric(rid.str[:4], errors="coerce")
    place = rid.str[4:6]
    is2027 = (y == FROZEN["test_year"]).to_numpy()
    f27 = featured[is2027]
    checks = {"test_year": FROZEN["test_year"], "n_rows_2027": int(len(f27))}
    blockers = []
    if len(f27) == 0:
        blockers.append("2027 レースが featured に無い（2027 開催がまだ／取込前）")
        return checks, False, blockers
    r27 = pd.Series(f27.index.astype(str))
    # JRA 限定・NAR 0
    nar = int((~pd.Series(central_index_mask(pd.Index(f27.index).astype(str)))).sum())
    checks["nar_rows"] = nar
    # (race_id,馬番) 重複
    uma = pd.to_numeric(f27.get(ResultsCols.UMABAN), errors="coerce")
    dup = int(pd.DataFrame({"r": r27.to_numpy(), "u": uma.to_numpy()}).duplicated().sum())
    checks["race_umaban_duplicate"] = dup
    # 1着1頭率
    rank = pd.to_numeric(f27.get(ResultsCols.RANK), errors="coerce")
    win_per_race = pd.DataFrame({"r": r27.to_numpy(), "w": (rank == 1).to_numpy()}).groupby("r")["w"].sum()
    winner_rate = float((win_per_race == 1).mean()) if len(win_per_race) else 0.0
    checks["winner_1of1_rate"] = round(winner_rate, 5)
    # 特徴 coverage は **2027 vs pre-2027 の相対**で見る（絶対閾値でなく・自然に部分的な wet_rel_rank を
    # 罰しない）。凍結特徴は B で検証済＝欠測は残差ヘッドが処理。ここで見るのは 2027 の**取込断絶**のみ。
    fidx = pd.Series(featured.index.astype(str))
    fy = pd.to_numeric(fidx.str[:4], errors="coerce")
    pre_mask = ((fy < FROZEN["test_year"]) & fidx.str[4:6].isin(
        {f"{i:02d}" for i in range(1, 11)})).to_numpy()
    cov27, covpre, regress = {}, {}, []
    for c in FROZEN["features"]:
        if c not in featured.columns:
            cov27[c] = covpre[c] = None
            regress.append(c)
            continue
        col = pd.to_numeric(featured[c], errors="coerce")
        cov27[c] = round(float(col[is2027].notna().mean()), 4)
        covpre[c] = round(float(col[pre_mask].notna().mean()), 4) if pre_mask.any() else None
        if covpre[c] and cov27[c] < 0.5 * covpre[c]:
            regress.append(c)
    checks["feature_coverage_2027"] = cov27
    checks["feature_coverage_pre2027"] = covpre
    # training max year < test min year（year 粒度で保証）
    train_years = sorted({int(r["year"]) for r in records if r["year"] and r["year"] < FROZEN["test_year"]})
    checks["train_years"] = train_years
    checks["train_max_year_lt_test"] = bool(train_years and max(train_years) < FROZEN["test_year"])
    checks["n_records_2027"] = int(sum(1 for r in records if r["year"] == FROZEN["test_year"]))

    if nar != 0:
        blockers.append(f"NAR {nar} 行（JRA限定違反）")
    if dup != 0:
        blockers.append(f"(race_id,馬番) 重複 {dup}")
    if winner_rate < MIN_WINNER_RATE:
        blockers.append(f"1着1頭率 {winner_rate:.4f} < {MIN_WINNER_RATE}")
    if not checks["train_max_year_lt_test"]:
        blockers.append("training max year >= test year（時系列違反）")
    if regress:
        blockers.append(f"凍結特徴の 2027 coverage 断絶（pre-2027比0.5未満）: {regress}")
    if checks["n_records_2027"] < 100:
        blockers.append(f"2027 の評価 records {checks['n_records_2027']} が少なすぎ")
    return checks, (len(blockers) == 0), blockers


def fit_and_eval(train, test, feat_cols, *, l2, n_boot, seed):
    """freeze 期(train)で一度だけ学習し未見期(test)で固定評価（係数不変）。共有評価コア。

    返す {bb, d_ece, theta, xw, by_venue, b_used}。market vs market+残差の race-weighted ΔNLL・
    開催場×日 block・centered ASL。純度: fit/predict は既存の検証済関数。
    """
    import numpy as np
    from src.policies._market_residual import market_probs
    from src.policies._residual_head import fit_residual_head, residual_predict, residual_win_probs
    from src.simulation._model_compare import block_bootstrap_ci, ece, race_nll

    theta = fit_residual_head(train, feat_cols, l2=l2)
    use = bool(theta) and not all(v == 0 for v in theta.values())
    dnll, blocks, xw = [], [], []
    pb_flat, pc_flat, y_flat = [], [], []
    by_venue = {}
    for r in test:
        w = r.get("winner")
        if w is None:
            continue
        pb = market_probs(r["odds"])
        pc = residual_win_probs(r["odds"], r["feats"], theta) if use else pb
        if not (pb and pc):
            continue
        d = race_nll(pc, w) - race_nll(pb, w)
        dnll.append(d); blocks.append(str(r["race_id"])[:10])
        by_venue.setdefault(str(r["race_id"])[4:6], []).append(d)
        for _h, fv in r["feats"].items():
            xw.append(residual_predict(fv, theta) if use else 0.0)
        for h in pb:
            pb_flat.append(float(pb[h])); pc_flat.append(float(pc.get(h, 0.0)))
            y_flat.append(1 if h == w else 0)
    b_used = max(2000, n_boot)
    bb = block_bootstrap_ci(dnll, blocks, n_boot=b_used, seed=seed)
    return {"bb": bb, "d_ece": ece(pc_flat, y_flat) - ece(pb_flat, y_flat) if pb_flat else None,
            "theta": theta, "xw": np.asarray(xw, dtype=float), "by_venue": by_venue,
            "b_used": b_used, "n_dnll": len(dnll)}


def _load_featured(path):
    from app._model_eval import load_featured_data
    feat = load_featured_data(path) if path else load_featured_data()
    if feat is None or feat.empty:
        raise RuntimeError("featured を読めません（ローカルで実行）")
    return feat


def _do_audit(args) -> int:
    import json
    from src.training._provenance import _git_commit
    print("=" * 84)
    print(f"B-2027 confirmation 監査（--audit-only・{FROZEN['hypothesis_id']}・性能を見ない）")
    feat = _load_featured(args.featured)
    records, feat_cols = _build_records(feat)
    checks, passed, blockers = _data_gate_2027(feat, records)
    manifest = {
        **FROZEN,
        "freeze_commit": _git_commit(),
        "feature_definition_hash": _feature_def_hash(),
        "training_code_hash": _training_code_hash(),
        "data_fingerprint": _data_fingerprint(feat),
        "generated_feature_hash": _feature_hash(records),
        "resolved_features": feat_cols,
        "gate_checks": checks,
        "audit_result": "PASS" if passed else "FAIL",
    }
    out = Path(args.manifest_out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[凍結] features={FROZEN['features']}  L2={FROZEN['l2']}  MES={FROZEN['mes_dnll']} nats/race  "
          f"test_year={FROZEN['test_year']}  interim_looks=0")
    print(f"[解決特徴] {feat_cols}")
    print("[2027 ゲート]")
    for k, v in checks.items():
        print(f"  {k:<26} = {v}")
    print(f"\n判定: audit_result = {manifest['audit_result']}")
    if not passed:
        print("  ブロッカー: " + " / ".join(blockers))
        print("  → 2027 データ未整備 or 完全性違反。整備後に再監査（2027 全開催終了後に一度）。")
        return 5
    print(f"  manifest 保存: {out}")
    print("  → PASS。評価は 2027 全開催終了後に**一度だけ**:")
    print(f"     python scripts/run_residual_head_2027.py --evaluate --audit-manifest {out} "
          f"--n-boot {FROZEN['bootstrap_repetitions']} --seed {FROZEN['bootstrap_seed']}")
    return 0


def _do_evaluate(args) -> int:
    import json
    import numpy as np
    from src.policies._market_residual import market_probs
    from src.policies._residual_head import fit_residual_head, residual_predict, residual_win_probs
    from src.simulation._model_compare import block_bootstrap_ci, ece, race_nll
    from src.training._provenance import assert_jra_only

    mpath = Path(args.audit_manifest)
    if not mpath.exists():
        print(f"manifest がありません: {mpath}（先に --audit-only）", file=sys.stderr)
        return 2
    manifest = json.loads(mpath.read_text(encoding="utf-8"))
    if manifest.get("audit_result") != "PASS":
        print(f"manifest audit_result={manifest.get('audit_result')}≠PASS＝評価不可。", file=sys.stderr)
        return 5

    print("=" * 84)
    print(f"B-2027 confirmation 評価（--evaluate・{FROZEN['hypothesis_id']}・一度だけ）")
    feat = _load_featured(args.featured)
    records, feat_cols = _build_records(feat)
    fp = {"feature_definition_hash": _feature_def_hash(),
          "training_code_hash": _training_code_hash(),
          "data_fingerprint": _data_fingerprint(feat),
          "generated_feature_hash": _feature_hash(records)}
    mism = [k for k in fp if manifest.get(k) != fp[k]]
    if mism:
        print(f"[STOP] manifest 不一致 {mism}＝凍結後に定義/コード/データ/特徴が変化。再監査せよ。",
              file=sys.stderr)
        return 6
    # 凍結特徴が manifest と一致（追加/除外なし）
    if list(manifest.get("features", [])) != FROZEN["features"]:
        print("[STOP] 特徴集合が凍結仕様と不一致。", file=sys.stderr)
        return 6
    print(f"[一致検証 OK] 4ハッシュ＋特徴集合が manifest と一致。records={len(records):,}")

    train = [r for r in records if r["year"] and r["year"] < FROZEN["test_year"]]
    test = [r for r in records if r["year"] == FROZEN["test_year"]]
    if not test:
        print("2027 の test records が空（2027 未整備）。", file=sys.stderr)
        return 5
    try:
        nar = assert_jra_only([r["race_id"] for r in test])
        print(f"[provenance] test JRA限定 nar_rows={nar}（fail-closed）")
    except RuntimeError as e:
        print(f"[FAIL-CLOSED] {e}", file=sys.stderr)
        return 4

    # freeze 期まで一度だけ学習・未見期で固定評価（係数は test 中に変えない）
    res = fit_and_eval(train, test, feat_cols, l2=FROZEN["l2"], n_boot=args.n_boot, seed=args.seed)
    bb, d_ece, theta, xw, by_venue = res["bb"], res["d_ece"], res["theta"], res["xw"], res["by_venue"]
    b_used = res["b_used"]
    v = verdict(bb["mean"], bb["hi"], d_ece)
    print(f"\n=== Primary（2027 test・{len(test):,} races・venue×日 block・m=1・B={b_used:,}/seed={args.seed}）===")
    print(f"  最小到達可能 p=1/(B+1)={1.0/(b_used+1):.2e}  estimand=race-weighted 平均ΔNLL(nats/race)")
    de_s = f"{d_ece:+.6f}" if isinstance(d_ece, float) else "n/a"
    print(f"  ΔNLL={bb['mean']:+.6f} ({bb['mean']:.3e})  95%CI[{bb['lo']:+.6f},{bb['hi']:+.6f}]  "
          f"p={bb.get('p_improve', float('nan')):.4f}  ΔECE={de_s}")
    print(f"  block数={bb['n_blocks']:,}  test race数={len(dnll):,}")
    print(f"\n判定: {v}")
    print("\n--- Secondary（安定性の説明用・採用判定でない）---")
    print(f"  std(Xθ)={float(np.std(xw)):.5f}  mean|Xθ|={float(np.mean(np.abs(xw))):.5f}")
    print(f"  係数 θ: { {k: round(float(theta.get(k, 0.0)), 5) for k in feat_cols} }")
    print("  会場別 ΔNLL(月別は ymd 要・会場を安定性代理): "
          + "  ".join(f"{v_}:{np.mean(ds):+.5f}(n={len(ds)})" for v_, ds in sorted(by_venue.items())))
    print("\n[境界] これは NLL 上の out-of-period 確認。🟢 でも ROI・控除超過・サイジングは別の新仮説。")
    print("        2027 は interim looks=0（一度だけ）。特徴/L2/変換/欠測処理は凍結・再較正しない。")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="B 残差ヘッドの 2027 未見確認（事前登録・manifest-bound）")
    ap.add_argument("--featured", default=None)
    ap.add_argument("--audit-only", action="store_true", help="2027 完全性ゲート＋manifest（既定・性能見ない）")
    ap.add_argument("--evaluate", action="store_true", help="manifest 一致検証後に一度だけ ΔNLL")
    ap.add_argument("--audit-manifest", default=None)
    ap.add_argument("--manifest-out", default="artifacts/b_2027_audit.json")
    ap.add_argument("--n-boot", type=int, default=FROZEN["bootstrap_repetitions"])
    ap.add_argument("--seed", type=int, default=FROZEN["bootstrap_seed"])
    args = ap.parse_args()
    try:
        if args.evaluate:
            if not args.audit_manifest:
                print("--evaluate には --audit-manifest が必須（先に --audit-only で PASS）", file=sys.stderr)
                return 2
            return _do_evaluate(args)
        return _do_audit(args)
    except RuntimeError as e:
        print(f"[エラー] {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
