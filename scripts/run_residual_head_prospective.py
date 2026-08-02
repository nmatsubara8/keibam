"""B_RESIDUAL_HEAD_PROSPECTIVE_CONFIRM: B 残差ヘッドの **prospective shadow** 確認（今 freeze・層(2)）。

続16 の三層確認のうち層(2)＝prospective shadow。モデルを **freeze 日(2026-08-02)まで**で一度学習・以後固定し、
**freeze 日より後に走る未見レース**を test 窓とする（2027 完結を待たずに証拠が貯まり始める）。特徴集合・
変換(within-race z-score)・L2・欠測処理は B 原仕様のまま**一切変えない**。層(3)の 2027 カレンダー確認
(run_residual_head_2027.py)は別レーンで併存。

**検定力の注意（事前明記）**: B の効果は −3e-4 nats/race と小さい。prospective 窓が小さいと block CI が広く、
真に −3e-4 でも CI上限<0 に届かない（＝shadow は監視・早期信号であり、正式判定には十分な n が要る）。
そこで **min_test_races=5000（~1.5年相当）の trigger** を事前登録し、それ未満では --evaluate を gate で拒否
（interim looks=0＝trigger 到達時に一度だけ）。--audit-only はデータ完全性のみ（性能を見ない）で反復可。

日付は SED の ymd を race_id に結合して取得（featured は race_id 索引で暦日を持たないため）。H3/2027 と同型の
manifest-bound audit→evaluate。単位: ΔNLL=nats/race。featured/SED はローカル成果物。純部はテスト済。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.run_residual_head_2027 import (  # noqa: E402  共有コア・純部を再利用
    MIN_WINNER_RATE,
    _data_fingerprint,
    _feature_def_hash,
    _feature_hash,
    fit_and_eval,
    verdict,
)

FROZEN = {
    "hypothesis_id": "B_RESIDUAL_HEAD_PROSPECTIVE_CONFIRM",
    "features": ["jrdb_idm", "jrdb_kishu_idx", "jrdb_joho_idx", "wet_rel_rank", "kinryo_per_weight"],
    "l2": 1.0,
    "mes_dnll": 0.001,             # nats/race
    "ece_tolerance": 0.005,
    "freeze_date": "2026-08-02",   # train<=これ・test>これ（pre-registered cutoff）
    "min_test_races": 5000,        # trigger（~1.5年相当・power 確保）。未満は evaluate 拒否
    "bootstrap_blocks": "venue_x_date",
    "bootstrap_repetitions": 20000,
    "bootstrap_seed": 0,
    "estimand": "race_weighted_mean_dnll",
    "jra_only": True,
    "nar_rows_required": 0,
    "interim_looks": 0,            # trigger 到達時に一度だけ
    "data_contract_version": "H3B-2027-v1",
    "preprocessing": "within_race_zscore(build_residual_records)",
}


def _training_code_hash() -> str:
    import hashlib
    return hashlib.sha1(Path(__file__).read_bytes().decode("utf-8", "replace").encode("utf-8")).hexdigest()


def _ymd_map(sed):
    """SED から race_id→ymd(datetime) を作る（暦日 split 用）。"""
    import pandas as pd
    rid = "race_id" if "race_id" in sed.columns else "race_key"
    ymd = "ymd" if "ymd" in sed.columns else "date"
    d = sed[[rid, ymd]].copy()
    d[rid] = d[rid].astype(str)
    d["_d"] = pd.to_datetime(d[ymd].astype(str), errors="coerce", format="%Y%m%d")
    d = d.dropna(subset=["_d"]).drop_duplicates(rid)
    return dict(zip(d[rid], d["_d"]))


def _split_by_freeze(records, ymd_map):
    """records を freeze_date で train(<=)/test(>) に分割。ymd 不明は除外。返す (train, test, meta)。"""
    import pandas as pd
    fd = pd.to_datetime(FROZEN["freeze_date"])
    train, test = [], []
    max_train = min_test = None
    for r in records:
        d = ymd_map.get(str(r["race_id"]))
        if d is None:
            continue
        r2 = {**r, "_d": d}
        if d <= fd:
            train.append(r2)
            max_train = d if max_train is None or d > max_train else max_train
        else:
            test.append(r2)
            min_test = d if min_test is None or d < min_test else min_test
    meta = {"max_train_date": None if max_train is None else str(max_train.date()),
            "min_test_date": None if min_test is None else str(min_test.date()),
            "n_train": len(train), "n_test": len(test)}
    return train, test, meta


def _gate(train, test, meta, featured):
    """prospective ゲート（性能前）: trigger(min_test_races)＋完全性＋時系列。返す (checks, passed, blockers)。"""
    import pandas as pd
    from src.constants._model_category import central_index_mask
    from src.constants._results_cols import ResultsCols
    rids = [r["race_id"] for r in test]
    nar = int((~pd.Series(central_index_mask(pd.Index(rids).astype(str)))).sum()) if rids else 0
    winner_ok = all(r.get("winner") is not None for r in test)
    # coverage は **train窓 vs test窓の相対**で見る（絶対閾値でなく・自然に部分的な wet_rel_rank を罰しない）。
    # 凍結特徴は B で検証済＝欠測は残差ヘッドが z-score→0 で処理。ここで見るのは新期間の**取込断絶**のみ。
    fidx = pd.Series(featured.index.astype(str))
    tr_rids = {r["race_id"] for r in train}
    te_rids = {r["race_id"] for r in test}
    tr_mask = fidx.isin(tr_rids).to_numpy()
    te_mask = fidx.isin(te_rids).to_numpy()
    cov_tr, cov_te, regress = {}, {}, []
    for c in FROZEN["features"]:
        if c not in featured.columns:
            cov_tr[c] = cov_te[c] = None
            regress.append(c)
            continue
        col = pd.to_numeric(featured[c], errors="coerce")
        cov_tr[c] = round(float(col[tr_mask].notna().mean()), 4) if tr_mask.any() else None
        cov_te[c] = round(float(col[te_mask].notna().mean()), 4) if te_mask.any() else None
        if te_mask.any() and cov_tr[c] and cov_te[c] is not None and cov_te[c] < 0.5 * cov_tr[c]:
            regress.append(c)   # 新期間で coverage が学習期の半分未満＝取込断絶の疑い
    checks = {"freeze_date": FROZEN["freeze_date"], **meta, "nar_rows": nar,
              "min_test_races_trigger": FROZEN["min_test_races"],
              "trigger_reached": meta["n_test"] >= FROZEN["min_test_races"],
              "time_order_ok": bool(meta["max_train_date"] and meta["min_test_date"]
                                    and meta["max_train_date"] < meta["min_test_date"]),
              "all_test_have_winner": winner_ok,
              "feature_coverage_train": cov_tr, "feature_coverage_test": cov_te}
    blockers = []
    if not checks["trigger_reached"]:
        blockers.append(f"未見レース {meta['n_test']} < trigger {FROZEN['min_test_races']}"
                        "（貯まるまで evaluate 不可＝shadow 蓄積中）")
    if nar != 0:
        blockers.append(f"NAR {nar}")
    if meta["n_test"] > 0 and not checks["time_order_ok"]:
        blockers.append("train max date >= test min date（時系列違反）")
    if regress:
        blockers.append(f"凍結特徴の新期間 coverage 断絶（train比0.5未満）: {regress}")
    return checks, (len(blockers) == 0), blockers


def _load(args):
    from app._model_eval import load_featured_data
    from src.jrdb._store import JrdbStore
    from scripts.run_residual_head import build_residual_records
    feat = load_featured_data(args.featured) if args.featured else load_featured_data()
    if feat is None or feat.empty:
        raise RuntimeError("featured を読めません（ローカルで実行）")
    sed = JrdbStore().read("SED")
    records, feat_cols = build_residual_records(feat, FROZEN["features"], jra_only=FROZEN["jra_only"])
    train, test, meta = _split_by_freeze(records, _ymd_map(sed))
    return feat, records, feat_cols, train, test, meta


def _do_audit(args) -> int:
    import json
    from src.training._provenance import _git_commit
    print("=" * 84)
    print(f"B prospective 監査（--audit-only・{FROZEN['hypothesis_id']}・性能を見ない）")
    feat, records, feat_cols, train, test, meta = _load(args)
    checks, passed, blockers = _gate(train, test, meta, feat)
    manifest = {**FROZEN, "freeze_commit": _git_commit(),
                "feature_definition_hash": _feature_def_hash(),
                "training_code_hash": _training_code_hash(),
                "data_fingerprint": _data_fingerprint(feat),
                "generated_feature_hash": _feature_hash(records),
                "resolved_features": feat_cols, "gate_checks": checks,
                "audit_result": "PASS" if passed else "FAIL"}
    out = Path(args.manifest_out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[凍結] features={FROZEN['features']} L2={FROZEN['l2']} MES={FROZEN['mes_dnll']} nats/race "
          f"freeze_date={FROZEN['freeze_date']} trigger={FROZEN['min_test_races']}")
    print(f"[分割] train={meta['n_train']:,}(<= {meta['max_train_date']}) "
          f"test={meta['n_test']:,}(> freeze, min {meta['min_test_date']})")
    print("[ゲート]")
    for k, v in checks.items():
        print(f"  {k:<24} = {v}")
    print(f"\n判定: audit_result = {manifest['audit_result']}")
    if not passed:
        print("  ブロッカー: " + " / ".join(blockers))
        print("  → 未見レースが trigger に達するまで蓄積（--audit-only はデータのみで反復可・性能は見ない）。")
        return 5
    print(f"  manifest 保存: {out}\n  → PASS(trigger到達)。評価は一度だけ:")
    print(f"     python scripts/run_residual_head_prospective.py --evaluate --audit-manifest {out} "
          f"--n-boot {FROZEN['bootstrap_repetitions']} --seed {FROZEN['bootstrap_seed']}")
    return 0


def _do_evaluate(args) -> int:
    import json
    import numpy as np
    from src.training._provenance import assert_jra_only
    mpath = Path(args.audit_manifest)
    if not mpath.exists():
        print(f"manifest がありません: {mpath}（先に --audit-only）", file=sys.stderr)
        return 2
    manifest = json.loads(mpath.read_text(encoding="utf-8"))
    if manifest.get("audit_result") != "PASS":
        print(f"manifest audit_result={manifest.get('audit_result')}≠PASS＝評価不可"
              f"（trigger 未到達 or 完全性違反）。", file=sys.stderr)
        return 5
    print("=" * 84)
    print(f"B prospective 評価（--evaluate・{FROZEN['hypothesis_id']}・一度だけ）")
    feat, records, feat_cols, train, test, meta = _load(args)
    fp = {"feature_definition_hash": _feature_def_hash(), "training_code_hash": _training_code_hash(),
          "data_fingerprint": _data_fingerprint(feat), "generated_feature_hash": _feature_hash(records)}
    mism = [k for k in fp if manifest.get(k) != fp[k]]
    if mism or list(manifest.get("features", [])) != FROZEN["features"]:
        print(f"[STOP] manifest 不一致 {mism or 'features'}＝凍結後に変化。再監査せよ。", file=sys.stderr)
        return 6
    if not test:
        print("test 窓が空。", file=sys.stderr)
        return 5
    try:
        nar = assert_jra_only([r["race_id"] for r in test])
        print(f"[一致検証 OK] 4ハッシュ＋特徴一致。test JRA nar_rows={nar}・test={len(test):,}")
    except RuntimeError as e:
        print(f"[FAIL-CLOSED] {e}", file=sys.stderr)
        return 4
    res = fit_and_eval(train, test, feat_cols, l2=FROZEN["l2"], n_boot=args.n_boot, seed=args.seed)
    bb, d_ece, theta, xw = res["bb"], res["d_ece"], res["theta"], res["xw"]
    v = verdict(bb["mean"], bb["hi"], d_ece)
    de_s = f"{d_ece:+.6f}" if isinstance(d_ece, float) else "n/a"
    print(f"\n=== Primary（prospective test {res['n_dnll']:,} races・venue×日 block・m=1・"
          f"B={res['b_used']:,}/seed={args.seed}）===")
    print(f"  freeze_date={FROZEN['freeze_date']}  train max={meta['max_train_date']}  "
          f"test {meta['min_test_date']}〜  最小到達可能 p={1.0/(res['b_used']+1):.2e}")
    print(f"  ΔNLL={bb['mean']:+.6f} ({bb['mean']:.3e}) nats/race  95%CI[{bb['lo']:+.6f},{bb['hi']:+.6f}]  "
          f"p={bb.get('p_improve', float('nan')):.4f}  ΔECE={de_s}  block数={bb['n_blocks']:,}")
    print(f"\n判定: {v}")
    print("--- Secondary（説明用）---")
    print(f"  std(Xθ)={float(np.std(xw)):.5f}  mean|Xθ|={float(np.mean(np.abs(xw))):.5f}  "
          f"θ={ {k: round(float(theta.get(k, 0.0)), 5) for k in feat_cols} }")
    print("  会場別 ΔNLL: " + "  ".join(
        f"{v_}:{np.mean(ds):+.5f}(n={len(ds)})" for v_, ds in sorted(res["by_venue"].items())))
    print("\n[境界] prospective shadow＝NLL 上の早期 out-of-period 確認。🟢 でも ROI/控除超過/サイジングは別新仮説。")
    print("        層(3)の 2027 カレンダー確認は別レーンで併存。特徴/L2/変換/欠測は凍結・再較正しない。")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="B 残差ヘッドの prospective shadow 確認（事前登録・manifest-bound）")
    ap.add_argument("--featured", default=None)
    ap.add_argument("--audit-only", action="store_true")
    ap.add_argument("--evaluate", action="store_true")
    ap.add_argument("--audit-manifest", default=None)
    ap.add_argument("--manifest-out", default="artifacts/b_prospective_audit.json")
    ap.add_argument("--n-boot", type=int, default=FROZEN["bootstrap_repetitions"])
    ap.add_argument("--seed", type=int, default=FROZEN["bootstrap_seed"])
    args = ap.parse_args()
    try:
        if args.evaluate:
            if not args.audit_manifest:
                print("--evaluate には --audit-manifest が必須", file=sys.stderr)
                return 2
            return _do_evaluate(args)
        return _do_audit(args)
    except RuntimeError as e:
        print(f"[エラー] {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
