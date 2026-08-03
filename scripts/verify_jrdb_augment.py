"""既存 standalone augment（scripts/jrdb_build_features.py）の出力を feature-only 認定する検証ツール。

続31 監査で「本線 featured は 5/43 実体化」。修復前に、**完全 augment が 38 特徴を正しく materialize
するか**を性能を見ずに認定する（ユーザ選択: 既存 standalone をまず実データ検証）。

3 経路:
  --from-store     : **JRDB store（既取込 SQLite）から augment を構築**して検証（推奨・raw txt 不要）
  --augmented PATH : jrdb_build_features.py が出力した featured+JRDB pickle を読む
  --jrdb-dir  DIR  : その場で raw txt を parse して augment を構築

各 EXPECTED_JRDB_FULL 特徴について: 実在 / 非欠測率(JRA) / 年別 / sentinel率(-99.9/負) / race内分散>0率 /
NaN・inf。加えて join 一致率（attach が featured 行にどれだけ値を付けたか）と asof 特徴の leak spot-check。
性能(ΔNLL)は一切見ない。base featured(現5列) との差分(5→43)も表示。要ローカル。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _agreement_vs_base(kyi, base):
    """store 由来 kyi と base(adapter経路・既知良好5列)を (race_id,馬番) で突合し scale/parse 一致を検証。

    corr≈1 かつ median比≈1 なら store 経路の値は adapter と整合（本線統合で jrdb_idm 等が変質しない）。
    median比が ~10/~0.1 等なら ZZ9.9 スケール差＝統合前に要修正（妥当性チェック）。
    """
    import numpy as np
    import pandas as pd
    from src.constants._results_cols import ResultsCols
    overlap = [c for c in ("jrdb_idm", "jrdb_kijun_odds", "jrdb_kyakushitsu", "jrdb_joho_idx",
                           "jrdb_kishu_idx") if c in base.columns and c in kyi.columns]
    if not overlap:
        print("  [値一致検証] base に既知列が無くスキップ。")
        return
    b = pd.DataFrame({"race_id": base.index.astype(str).to_numpy(),
                      "umaban": pd.to_numeric(base.get(ResultsCols.UMABAN),
                                              errors="coerce").astype("Int64").to_numpy()})
    for c in overlap:
        b[c + "_base"] = pd.to_numeric(base[c], errors="coerce").to_numpy()
    m = b.merge(kyi[["race_id", "umaban", *overlap]], on=["race_id", "umaban"], how="inner")
    print("  [値一致検証 store vs adapter既知5列]（corr≈1・median比≈1 が整合）")
    for c in overlap:
        x = pd.to_numeric(m[c + "_base"], errors="coerce")
        y = pd.to_numeric(m[c], errors="coerce")
        ok = x.notna() & y.notna()
        if ok.sum() < 10:
            print(f"    {c:<20} n={int(ok.sum())} 不足")
            continue
        corr = float(np.corrcoef(x[ok], y[ok])[0, 1])
        ratio = float((y[ok] / x[ok].replace(0, np.nan)).median())
        print(f"    {c:<20} n={int(ok.sum()):>7,} corr={corr:+.4f} median比(store/base)={ratio:.3f}")


def _canary_npast(base, attached, soten):
    """1頭を canary に、jrdb_ms_npast が過去走数として単調増加するかを target 側で確認する。

    base(ketto/date 付) と attached は行順が一致（attach は入力順・index を保持）＝**位置**で整列できる
    （attached は ketto を落とすため）。3走以上かつ soten 在の馬を1頭選び、target 各走の npast を表示。
    初出走=NaN、以降 1,2,... と増えれば as-of の行復元は健全。全 NaN なら index/行復元の不備を示唆。
    """
    import numpy as np
    import pandas as pd
    if base is None or "ketto" not in base.columns or soten is None or getattr(soten, "empty", True):
        print("  [canary] ketto/soten 不足でスキップ")
        return
    if "jrdb_ms_npast" not in attached.columns or "date" not in base.columns:
        print("  [canary] npast/date 列が無くスキップ")
        return
    bpos = base.reset_index(drop=True)
    apos = attached.reset_index(drop=True)
    npast = pd.to_numeric(apos["jrdb_ms_npast"], errors="coerce").to_numpy()
    kser = bpos["ketto"].astype("string")
    dser = pd.to_datetime(bpos["date"], errors="coerce")
    sk = set(soten["ketto"].astype(str))
    cnt = kser.value_counts()
    cand = [k for k, n in cnt.items() if n >= 3 and str(k) in sk and str(k) != "<NA>"]
    if not cand:
        print("  [canary] 3走以上かつ soten 在の馬が無くスキップ")
        return
    k = cand[0]
    mask = (kser == k).fillna(False).to_numpy(dtype=bool)   # nullable string の NA を False に
    idx = np.where(mask)[0]
    order = np.argsort(dser.to_numpy()[idx])
    idx = idx[order]
    src_dates = pd.to_datetime(soten[soten["ketto"].astype(str) == str(k)]["hist_date"]).sort_values()
    print(f"  [canary jrdb_ms_npast] ketto={k}  base走数={int(mask.sum())}  soten履歴={len(src_dates)}")
    print(f"    source dates: {[d.strftime('%Y-%m-%d') for d in src_dates][:6]}")
    for j, i in enumerate(idx[:5]):
        d = dser.iloc[i]
        ds = d.strftime("%Y-%m-%d") if pd.notna(d) else "NaT"
        print(f"    target[{j}] {ds} → npast={npast[i]}（期待: 初走 NaN, 以降 1,2,...）")


def _load_augmented(args):
    import pandas as pd
    if args.from_store:
        from app._model_eval import load_featured_data
        from src.jrdb._augment import (attach, build_history_from_dfs, build_kyi_from_df,
                                        build_soten_from_df, ensure_ketto)
        from src.jrdb._store import JrdbStore
        base = load_featured_data()
        if base is None or base.empty:
            raise RuntimeError("base featured を読めません")
        store = JrdbStore()
        kyi_raw = store.read("KYI")
        kyi = build_kyi_from_df(kyi_raw)
        sed = store.read("SED")        # SED は一度だけ読み history/soten で共有（全表 SELECT の重複回避）
        hist = build_history_from_dfs(sed, store.read("SKB"))
        soten = build_soten_from_df(sed)
        # history/soten は ketto で結合するが netkeiba featured は ketto を持たないことがある。
        # KYI(race_id,馬番→ketto) で base に ketto を補って attach 可能にする（本線統合でも要る配線）。
        _agreement_vs_base(kyi, base)      # 既知良好(adapter経路)の5列と scale/parse 一致を検証
        base = ensure_ketto(base, kyi_raw)
        # asof 結合の前提診断（history/soten が DEAD の原因切り分け）。date は robust パーサで判定
        # （既定 pd.to_datetime は netkeiba の 'YYYY年MM月DD日' を全 NaT にする＝続31 DEAD 原因）。
        import pandas as _pd
        from src.jrdb._augment import _to_race_datetime
        if "date" in base.columns:
            naive = float(_pd.to_datetime(base["date"], errors="coerce").notna().mean())
            robust = float(_to_race_datetime(base["date"]).notna().mean())
        else:
            naive = robust = 0.0
        # ketto 有効率は all-rows と JRA2015+ eligible を分けて表示（地方/2014以前は分母外＝誤解防止）。
        if "ketto" in base.columns:
            from src.constants._model_category import central_index_mask
            kall = float(_pd.Series(base["ketto"]).notna().mean())
            _yr = _pd.to_numeric(_pd.Series(base.index.astype(str)).str[:4], errors="coerce")
            _elig = central_index_mask(base.index) & (_yr >= 2015).to_numpy()
            kelig = float(_pd.Series(base["ketto"]).notna().to_numpy()[_elig].mean()) if _elig.any() else 0.0
        else:
            kall = kelig = 0.0
        hk = set(hist["ketto"].astype(str)) if len(hist) else set()
        bk = set(_pd.Series(base.get("ketto")).dropna().astype(str)) if "ketto" in base.columns else set()
        print(f"  [asof前提] base date有効率 既定={naive:.3f}→robust={robust:.3f}  "
              f"base ketto有効率 all={kall:.3f}/JRA2015+={kelig:.3f}  ketto∩(base,hist)={len(bk & hk):,}")
        print(f"  [from-store] KYI jrdb列={len([c for c in kyi.columns if str(c).startswith('jrdb_')])} "
              f"history rows={len(hist):,} soten rows={len(soten):,} "
              f"base ketto={'あり' if 'ketto' in base.columns else 'なし'}")
        out = attach(base, kyi, hist, soten=soten)
        _canary_npast(base, out, soten)   # jrdb_ms_npast の行復元 canary（as-of の健全性）
        # strictly-prior 全件 manifest（canary だけでなく全 target 行で未来/同日参照=0 を認定）。
        from src.jrdb._leak_audit import assert_strictly_prior, strictly_prior_join_report
        tgt = base[["ketto", "date"]] if {"ketto", "date"} <= set(base.columns) else base
        for label, src in (("history", hist), ("soten", soten)):
            rep = strictly_prior_join_report(tgt, src)
            print(f"  [leak manifest {label}] target_rows={rep['target_rows']:,} "
                  f"valid={rep['target_valid_rows']:,} feature_rows={rep['feature_rows']:,} "
                  f"future={rep['future_reference_count']} same_day={rep['same_day_reference_count']} "
                  f"exact={rep['exact_target_reference_count']} "
                  f"dup_keys={rep['target_key_duplicates']:,} max_src={rep['max_source_date']} "
                  f"leak_safe={rep['leak_safe']}")
            assert_strictly_prior(rep, label=label)   # 未来/同日参照があれば fail-closed
        return out
    if args.augmented:
        p = Path(args.augmented)
        if not p.exists():
            raise RuntimeError(f"--augmented {p} が無い（先に jrdb_build_features.py で生成）")
        return pd.read_pickle(p)
    if args.jrdb_dir:
        import glob
        from app._model_eval import load_featured_data
        from src.jrdb._augment import (attach, build_history, build_kyi, build_soten_history)
        base = load_featured_data()
        if base is None or base.empty:
            raise RuntimeError("base featured を読めません")
        d = args.jrdb_dir
        files = {t: sorted(glob.glob(f"{d}/{t}*.txt")) for t in ("KYI", "SED", "SKB")}
        kyi = build_kyi(files["KYI"])
        hist = build_history(files["SED"], files["SKB"])
        soten = build_soten_history(files["SED"])
        return attach(base, kyi, hist, soten=soten)
    raise RuntimeError("--from-store / --augmented / --jrdb-dir のいずれかを指定")


def main() -> int:
    import numpy as np
    import pandas as pd
    from app._model_eval import load_featured_data
    from src.constants._model_category import central_index_mask
    from src.training._feature_materialization import (CONTEXT_JRDB, CURRENT_ACTIVE_JRDB,
                                                       EXPECTED_JRDB_FULL, HISTORY_JRDB,
                                                       REQUIRED_JRDB_MIN,
                                                       assert_features_materialized,
                                                       classify_jrdb_feature,
                                                       materialization_verdict,
                                                       within_race_var_frac)

    ap = argparse.ArgumentParser(description="JRDB 完全 augment 出力の feature-only 認定（性能を見ない）")
    ap.add_argument("--from-store", action="store_true", help="JRDB store から augment 構築（raw txt 不要・推奨）")
    ap.add_argument("--augmented", default=None, help="jrdb_build_features.py 出力 pickle")
    ap.add_argument("--jrdb-dir", default=None, help="JRDB txt ディレクトリ（その場 augment）")
    args = ap.parse_args()

    print("=" * 88)
    print("JRDB augment 検証（feature-only・実体化を3契約で認定・性能は見ない）")
    try:
        feat = _load_augmented(args)
    except RuntimeError as e:
        print(f"[エラー] {e}", file=sys.stderr)
        return 2
    base = load_featured_data()
    base_cols = set(base.columns) if base is not None else set()

    rid = pd.Series(feat.index.astype(str))
    year = pd.to_numeric(rid.str[:4], errors="coerce")
    sel = (central_index_mask(feat.index) & (year >= 2015).to_numpy())
    fj = feat[sel]

    newcols = [c for c in EXPECTED_JRDB_FULL if c not in base_cols]
    print(f"[入力] augmented rows={len(feat):,}  JRA2015+={len(fj):,}  base featured 列数={len(base_cols)}")
    print(f"  既存 EXPECTED（base に在る5列）: {sorted(set(EXPECTED_JRDB_FULL) & base_cols)}")
    print(f"  新規生成 EXPECTED={len(newcols)}（内訳: ACTIVE {len(CURRENT_ACTIVE_JRDB)-5}＋"
          f"CONTEXT {len(CONTEXT_JRDB)}＋HISTORY {len(HISTORY_JRDB)}・既存5は ACTIVE に含む）")

    # 列を3群に割り当てて群別の判定基準で認定する（「列が在るだけで全欠測でも PASS」を防ぐ）。
    stats = {}                     # c -> (nm, sent, vf, verdict, klass)
    absent = []
    print(f"\n  {'特徴':<24}{'群':>8}{'実在':>4}{'非欠測':>8}{'sentinel':>9}{'分散有率':>9}{'新規':>5}{'判定':>8}")
    for c in EXPECTED_JRDB_FULL:
        kl = classify_jrdb_feature(c)
        newly = "新" if c not in base_cols else "既"
        if c not in feat.columns:
            absent.append(c)
            print(f"  {c:<24}{kl:>8}{'無':>4}{'—':>8}{'—':>9}{'—':>9}{newly:>5}{'ABSENT':>8}")
            continue
        col = pd.to_numeric(fj[c], errors="coerce")
        nm = float(col.notna().mean()) if len(col) else 0.0
        sent = float((col <= -99).mean()) if len(col) else 0.0
        vf = within_race_var_frac(fj[c], fj.index)
        inf = int(np.isinf(col.to_numpy(dtype=float, na_value=np.nan)).sum())
        v = materialization_verdict(kl, nm, sent, vf)
        stats[c] = (nm, sent, vf, v, kl)
        flag = "!inf" if inf else ""
        print(f"  {c:<24}{kl:>8}{'有':>4}{nm:>8.3f}{sent:>9.3f}{vf:>9.3f}{newly:>5}{v:>8}{flag}")

    # 群別の契約判定（分離）。CURRENT_ACTIVE=全 OK / CONTEXT=CTX_OK / HISTORY=全 HIST_OK（history有効時）。
    def _fail(group, good):
        return [c for c in group if c in absent or stats.get(c, (0, 0, 0, "DEAD"))[3] not in good]

    active_fail = _fail(CURRENT_ACTIVE_JRDB, {"OK"})
    context_fail = _fail(CONTEXT_JRDB, {"CTX_OK"})
    history_present = any(stats.get(c, (0,))[0] > 0.02 for c in HISTORY_JRDB)  # history 有効化された build か
    history_fail = _fail(HISTORY_JRDB, {"HIST_OK"}) if history_present else []

    print(f"\n[3契約認定]  ABSENT={len(absent)}"
          f"{('  '+str(absent)) if absent else ''}")
    print(f"  CURRENT_ACTIVE_REQUIRED（{len(CURRENT_ACTIVE_JRDB)}列・presence+coverage+race分散）: "
          f"{'✅ PASS' if not active_fail else '❌ FAIL '+str(active_fail)}")
    print(f"  CONTEXT_REQUIRED（{len(CONTEXT_JRDB)}列・presence+coverage・race分散は不問）: "
          f"{'✅ PASS' if not context_fail else '❌ FAIL '+str(context_fail)}")
    if history_present:
        print(f"  HISTORY_REQUIRED（{len(HISTORY_JRDB)}列・semantic coverage・全欠測は fail-closed）: "
              f"{'✅ PASS' if not history_fail else '❌ FAIL '+str(history_fail)}")
    else:
        print(f"  HISTORY_REQUIRED（{len(HISTORY_JRDB)}列）: ⚠ history 無効 build（全欠測）＝current-only 認定"
              "（history を有効にした build では全欠測を fail-closed にする）")

    # asof 特徴の leak spot-check（jrdb_ms_npast は「今走前の過去走数」＝初出走は NaN/0 のはず）
    if "jrdb_ms_npast" in fj.columns:
        npast = pd.to_numeric(fj["jrdb_ms_npast"], errors="coerce")
        print(f"\n[leak spot-check] jrdb_ms_npast: 非欠測={float(npast.notna().mean()):.3f} "
              f"min={npast.min()} median={npast.median()}（初出走は欠落・>=1 は過去走存在）")

    # 現行本線の fail-closed 退行チェック（REQUIRED_JRDB_MIN は base に在るべき）
    try:
        miss_opt = assert_features_materialized(feat.columns, REQUIRED_JRDB_MIN,
                                                optional=EXPECTED_JRDB_FULL)
        print(f"\n[fail-closed] REQUIRED_JRDB_MIN 充足。EXPECTED の欠落(warn)={len(miss_opt)}")
    except RuntimeError as e:
        print(f"\n[fail-closed] {e}", file=sys.stderr)

    current_ok = not active_fail and not context_fail and not absent
    full_ok = current_ok and not history_fail and history_present
    print("\n[本線統合の判定]")
    print(f"  current-only 統合（ACTIVE+CONTEXT）: {'✅ 可（明示 allowlist 付き）' if current_ok else '❌ 不可'}")
    print(f"  完全 augment 認定（+HISTORY）: {'✅ 可' if full_ok else '❌ 不可（HISTORY 修復まで）'}")
    print("  既存モデルへの新規列 silent 混入は assert_training_allowlist で阻止。B frozen は従来5特徴のまま。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
