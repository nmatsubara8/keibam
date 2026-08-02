"""JRDB42_RESIDUAL_2027_CONFIRM: 実体化した JRDB 特徴の予測価値を market 直交で確認（事前登録・凍結）。

続36 で 42-feature augment contract を実体化認定した。その **新規特徴が市場(オッズ)に直交する予測情報を
持つか** を、B と同じ market-anchored residual head（P=softmax(log q + θ·x)）の ΔNLL で確認する。
B と同じ検証済みコア（`fit_and_eval`/`verdict`/block bootstrap）を、**特徴集合だけ差し替えて**使う。

**プロトコル順守（重要）**:
- 特徴集合は within-race softmax で相殺する race-context（jrdb_pace_hms）を**除外**した
  CURRENT_ACTIVE(33)＋HISTORY(8)=**41列**（下記 FROZEN["features"]）。これは実体化済み augment 列のみ。
- **B と 2027 を共有するため joint 事前登録**（`docs/jrdb42_preregistration.md`）。primary の多重性は
  Holm（family={B, JRDB42}・m=2）。interim looks=0（2027 全開催終了後に一度だけ）。
- 学習側は明示 allowlist（`assert_no_unguarded_augment` を満たす）＝新規列の silent 混入なし。
- history 列の strictly-prior 性は **build 時**（`scripts/jrdb_build_features.py` の leak manifest）で
  fail-closed 認定済み前提。ここでは coverage 断絶のみ見る。

**未確定（freeze 前にユーザ確定が要る2点）**:
- features: 下記 41 列でよいか（部分集合にするか）。
- l2: 既定 1.0（B と同 regime）。development_known(2015-2024) 内部 CV で選ぶなら freeze 前に確定・記録。

**単位**: ΔNLL は自然対数＝nats/race。MES=0.001 nats/race。featured はローカル成果物。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.training._feature_materialization import (  # noqa: E402
    CURRENT_ACTIVE_JRDB,
    HISTORY_JRDB,
)

# race-context(jrdb_pace_hms)は within-race softmax で相殺＝残差ヘッドに情報を足さないので除外。
_FEATURES = sorted(set(CURRENT_ACTIVE_JRDB) | set(HISTORY_JRDB))

# ---- 凍結仕様（事前登録・結果を見て変えない。freeze は最初の --audit-only 実行で commit hash を刻む）----
FROZEN = {
    "hypothesis_id": "JRDB42_RESIDUAL_2027_CONFIRM",
    "features": _FEATURES,               # 41列（CURRENT_ACTIVE 33＋HISTORY 8・CONTEXT 除外）
    "l2": 1.0,                           # 既定 B と同 regime（dev-CV 選定するなら freeze 前に置換・記録）
    "mes_dnll": 0.001,                   # nats/race（自然対数）
    "ece_tolerance": 0.005,
    "test_year": 2027,
    "reserved_test_start": 2027,
    "consumed_test_years": [],
    "test_tranche_status": "reserved",
    "family": ["B_RESIDUAL_HEAD_2027_CONFIRM", "JRDB42_RESIDUAL_2027_CONFIRM"],  # Holm m=2
    "multiplicity": "holm",
    "bootstrap_blocks": "venue_x_date",
    "bootstrap_repetitions": 20000,
    "bootstrap_seed": 0,
    "estimand": "race_weighted_mean_dnll",
    "jra_only": True,
    "nar_rows_required": 0,
    "interim_looks": 0,
    "preprocessing": "within_race_zscore(build_residual_records)",
    "leak_manifest_enforced_at": "scripts/jrdb_build_features.py (strictly_prior_join_report)",
}
MIN_WINNER_RATE = 0.995


def holm_reject(pvalues: dict, alpha: float = 0.05) -> dict:
    """Holm-Bonferroni（family の p 値 dict → 各仮説 reject 可否）。純関数。

    p を昇順に、i 番目の閾値 alpha/(m-i)。最初に超えた以降は不採択（step-down）。
    """
    items = sorted(pvalues.items(), key=lambda kv: kv[1])
    m = len(items)
    out, stop = {}, False
    for i, (h, p) in enumerate(items):
        thr = alpha / (m - i)
        rej = (p <= thr) and not stop
        if not rej:
            stop = True
        out[h] = {"p": p, "threshold": thr, "reject": rej}
    return out


def _sha1(s) -> str:
    import hashlib
    return hashlib.sha1(str(s).encode("utf-8")).hexdigest()


def _training_code_hash() -> str:
    return _sha1(Path(__file__).read_bytes().decode("utf-8", "replace"))


def _build_records(featured):
    from scripts.run_residual_head import build_residual_records
    return build_residual_records(featured, FROZEN["features"], jra_only=FROZEN["jra_only"])


def verdict(dnll_mean, ci_hi, d_ece):
    from scripts.run_residual_head_2027 import verdict as _v
    return _v(dnll_mean, ci_hi, d_ece, mes=FROZEN["mes_dnll"], ece_tol=FROZEN["ece_tolerance"])


def _data_gate(featured, records):
    """2027 データ完全性ゲート（性能を見る前）。B-2027 の gate を JRDB41 特徴へ適用。"""
    import pandas as pd
    from src.constants._model_category import central_index_mask
    from src.constants._results_cols import ResultsCols

    rid = pd.Series(featured.index.astype(str))
    y = pd.to_numeric(rid.str[:4], errors="coerce")
    is_test = (y == FROZEN["test_year"]).to_numpy()
    ft = featured[is_test]
    checks = {"test_year": FROZEN["test_year"], "n_rows_test": int(len(ft))}
    blockers = []
    if len(ft) == 0:
        blockers.append("test 年レースが featured に無い（未開催／取込前）")
        return checks, False, blockers
    # augment 列（history 含む）が featured に materialize されているか（build 未実行を検知）
    missing = [c for c in FROZEN["features"] if c not in featured.columns]
    checks["missing_features"] = missing
    if missing:
        blockers.append(f"凍結特徴が featured に未実体化 {len(missing)} 列"
                        "（完全 augment build を先に実行）: " + str(missing[:8]))
    nar = int((~pd.Series(central_index_mask(pd.Index(ft.index).astype(str)))).sum())
    checks["nar_rows"] = nar
    if nar != 0:
        blockers.append(f"NAR {nar} 行（JRA限定違反）")
    rank = pd.to_numeric(ft.get(ResultsCols.RANK), errors="coerce")
    rtest = pd.Series(ft.index.astype(str))
    wpr = pd.DataFrame({"r": rtest.to_numpy(), "w": (rank == 1).to_numpy()}).groupby("r")["w"].sum()
    winner_rate = float((wpr == 1).mean()) if len(wpr) else 0.0
    checks["winner_1of1_rate"] = round(winner_rate, 5)
    if winner_rate < MIN_WINNER_RATE:
        blockers.append(f"1着1頭率 {winner_rate:.4f} < {MIN_WINNER_RATE}")
    # 特徴 coverage の 2027 vs pre-2027 相対断絶（絶対閾値でなく取込断絶のみ）
    fy = pd.to_numeric(rid.str[:4], errors="coerce")
    pre = ((fy < FROZEN["test_year"]) & rid.str[4:6].isin(
        {f"{i:02d}" for i in range(1, 11)})).to_numpy()
    regress = []
    for c in FROZEN["features"]:
        if c not in featured.columns:
            continue
        col = pd.to_numeric(featured[c], errors="coerce")
        cov_t = float(col[is_test].notna().mean())
        cov_p = float(col[pre].notna().mean()) if pre.any() else 0.0
        if cov_p and cov_t < 0.5 * cov_p:
            regress.append(c)
    checks["coverage_regressed"] = regress
    if regress:
        blockers.append(f"凍結特徴の test coverage 断絶（pre比0.5未満）: {regress}")
    n_test_records = int(sum(1 for r in records if r["year"] == FROZEN["test_year"]))
    checks["n_records_test"] = n_test_records
    if n_test_records < 100:
        blockers.append(f"test 評価 records {n_test_records} が少なすぎ")
    return checks, (len(blockers) == 0), blockers


def _do_audit(args) -> int:
    import json
    from src.training._provenance import _git_commit
    print("=" * 84)
    print(f"JRDB42 confirmation 監査（--audit-only・{FROZEN['hypothesis_id']}・性能を見ない）")
    from scripts.run_residual_head_2027 import _data_fingerprint, _load_featured
    feat = _load_featured(args.featured)
    records, feat_cols = _build_records(feat)
    checks, passed, blockers = _data_gate(feat, records)
    manifest = {
        **FROZEN,
        "freeze_commit": _git_commit(),
        "training_code_hash": _training_code_hash(),
        "data_fingerprint": _data_fingerprint(feat),
        "n_features": len(FROZEN["features"]),
        "audit_checks": checks,
        "audit_result": "PASS" if passed else "FAIL",
        "audit_blockers": blockers,
    }
    Path(args.manifest_out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.manifest_out).write_text(json.dumps(manifest, ensure_ascii=False, indent=2),
                                       encoding="utf-8")
    print(f"[監査] {'PASS' if passed else 'FAIL'}  features={len(FROZEN['features'])}  "
          f"records={len(records):,}  → {args.manifest_out}")
    for b in blockers:
        print(f"  BLOCKER: {b}")
    if not passed:
        print("\n2027 が未整備／未 build＝評価不可（queued）。整備後に再監査→--evaluate。")
    return 0 if passed else 3


def _do_evaluate(args) -> int:
    import json
    import numpy as np
    from scripts.run_residual_head_2027 import _data_fingerprint, _load_featured, fit_and_eval
    from src.training._provenance import assert_jra_only
    from src.training._temporal_split import assert_clean_final_test

    mpath = Path(args.audit_manifest)
    if not mpath.exists():
        print(f"manifest がありません: {mpath}（先に --audit-only）", file=sys.stderr)
        return 2
    manifest = json.loads(mpath.read_text(encoding="utf-8"))
    if manifest.get("audit_result") != "PASS":
        print(f"manifest audit_result={manifest.get('audit_result')}≠PASS＝評価不可。", file=sys.stderr)
        return 5
    print("=" * 84)
    print(f"JRDB42 confirmation 評価（--evaluate・{FROZEN['hypothesis_id']}・一度だけ）")
    feat = _load_featured(args.featured)
    records, feat_cols = _build_records(feat)
    # 凍結一致検証（コード/データ/特徴集合）
    if manifest.get("training_code_hash") != _training_code_hash():
        print("[STOP] training_code_hash 不一致＝凍結後にコード変化。再監査せよ。", file=sys.stderr)
        return 6
    if list(manifest.get("features", [])) != FROZEN["features"]:
        print("[STOP] 特徴集合が凍結仕様と不一致。", file=sys.stderr)
        return 6
    if manifest.get("data_fingerprint") != _data_fingerprint(feat):
        print("[STOP] data_fingerprint 不一致＝データ変化。再監査せよ。", file=sys.stderr)
        return 6
    train = [r for r in records if r["year"] and r["year"] < FROZEN["test_year"]]
    test = [r for r in records if r["year"] == FROZEN["test_year"]]
    if not test:
        print("test records が空（2027 未整備）。", file=sys.stderr)
        return 5
    try:
        assert_clean_final_test({r["year"] for r in train}, {FROZEN["test_year"]},
                                reserved_test_start=FROZEN["reserved_test_start"],
                                consumed_test_years=FROZEN["consumed_test_years"])
        nar = assert_jra_only([r["race_id"] for r in test])
        print(f"[provenance] test JRA限定 nar_rows={nar}（fail-closed）")
    except (ValueError, RuntimeError) as e:
        print(f"[STOP] temporal-split/provenance 違反: {e}", file=sys.stderr)
        return 6
    res = fit_and_eval(train, test, feat_cols, l2=FROZEN["l2"], n_boot=args.n_boot, seed=args.seed)
    bb, d_ece = res["bb"], res["d_ece"]
    v = verdict(bb["mean"], bb["hi"], d_ece)
    de_s = f"{d_ece:+.6f}" if isinstance(d_ece, float) else "n/a"
    print(f"\n=== Primary（2027 test・{len(test):,} races・venue×日 block・family m=2/Holm）===")
    print(f"  ΔNLL={bb['mean']:+.6f}  95%CI[{bb['lo']:+.6f},{bb['hi']:+.6f}]  "
          f"p={bb.get('p_improve', float('nan')):.4f}  ΔECE={de_s}")
    print(f"\n判定(単独): {v}")
    print("  ※ 最終判定は B の p 値と合わせ Holm(m=2) 適用後（docs/jrdb42_preregistration.md）。")
    print("  ※ std(Xθ)=%.5f（θ 41次元・安定性の説明用）" % float(np.std(res["xw"])))
    print("\n[境界] NLL 上の out-of-period 確認。🟢 でも ROI・控除超過・サイジングは別の新仮説。"
          " 2027 は interim looks=0。特徴/L2/前処理は凍結・再較正しない。")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="JRDB42 残差ヘッドの 2027 未見確認（事前登録・manifest-bound）")
    ap.add_argument("--featured", default=None)
    ap.add_argument("--audit-only", action="store_true", help="完全性ゲート＋manifest（既定・性能見ない）")
    ap.add_argument("--evaluate", action="store_true", help="manifest 一致検証後に一度だけ ΔNLL")
    ap.add_argument("--audit-manifest", default=None)
    ap.add_argument("--manifest-out", default="artifacts/jrdb42_2027_audit.json")
    ap.add_argument("--n-boot", type=int, default=FROZEN["bootstrap_repetitions"])
    ap.add_argument("--seed", type=int, default=FROZEN["bootstrap_seed"])
    args = ap.parse_args()
    try:
        if args.evaluate:
            return _do_evaluate(args)
        return _do_audit(args)
    except RuntimeError as e:
        print(f"[エラー] {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
