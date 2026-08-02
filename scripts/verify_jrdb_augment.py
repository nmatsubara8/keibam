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


def _within_race_var_frac(feat, col):
    """race(index)内で col が >1 値を持つレース割合（＝馬間分散あり率）。"""
    import pandas as pd
    s = pd.to_numeric(feat[col], errors="coerce")
    nun = s.groupby(feat.index).nunique(dropna=True)
    return float((nun > 1).mean()) if len(nun) else 0.0


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


def _ensure_ketto(base, kyi_raw):
    """base featured に ketto が無ければ KYI(race_id,馬番→ketto) で補う（history/soten 結合用）。"""
    import pandas as pd
    from src.constants._results_cols import ResultsCols
    if "ketto" in base.columns:
        return base
    km = kyi_raw[["race_id", "umaban", "ketto"]].copy()
    km["race_id"] = km["race_id"].astype(str)
    km["umaban"] = pd.to_numeric(km["umaban"], errors="coerce").astype("Int64")
    km = km.dropna(subset=["umaban"]).drop_duplicates(["race_id", "umaban"])
    b = base.copy()
    b["_rid"] = b.index.astype(str)
    b["_uma"] = pd.to_numeric(b.get(ResultsCols.UMABAN), errors="coerce").astype("Int64")
    merged = b.merge(km, left_on=["_rid", "_uma"], right_on=["race_id", "umaban"], how="left")
    merged.index = base.index
    base = base.copy()
    base["ketto"] = merged["ketto"].to_numpy()
    return base


def _load_augmented(args):
    import pandas as pd
    if args.from_store:
        from app._model_eval import load_featured_data
        from src.jrdb._augment import (attach, build_history_from_dfs, build_kyi_from_df,
                                        build_soten_from_df)
        from src.jrdb._store import JrdbStore
        base = load_featured_data()
        if base is None or base.empty:
            raise RuntimeError("base featured を読めません")
        store = JrdbStore()
        kyi_raw = store.read("KYI")
        kyi = build_kyi_from_df(kyi_raw)
        hist = build_history_from_dfs(store.read("SED"), store.read("SKB"))
        soten = build_soten_from_df(store.read("SED"))
        # history/soten は ketto で結合するが netkeiba featured は ketto を持たないことがある。
        # KYI(race_id,馬番→ketto) で base に ketto を補って attach 可能にする（本線統合でも要る配線）。
        _agreement_vs_base(kyi, base)      # 既知良好(adapter経路)の5列と scale/parse 一致を検証
        base = _ensure_ketto(base, kyi_raw)
        # asof 結合の前提診断（history/soten が DEAD の原因切り分け）。date は robust パーサで判定
        # （既定 pd.to_datetime は netkeiba の 'YYYY年MM月DD日' を全 NaT にする＝続31 DEAD 原因）。
        import pandas as _pd
        from src.jrdb._augment import _to_race_datetime
        if "date" in base.columns:
            naive = float(_pd.to_datetime(base["date"], errors="coerce").notna().mean())
            robust = float(_to_race_datetime(base["date"]).notna().mean())
        else:
            naive = robust = 0.0
        kcov = float(_pd.Series(base.get("ketto")).notna().mean()) if "ketto" in base.columns else 0.0
        hk = set(hist["ketto"].astype(str)) if len(hist) else set()
        bk = set(_pd.Series(base.get("ketto")).dropna().astype(str)) if "ketto" in base.columns else set()
        print(f"  [asof前提] base date有効率 既定={naive:.3f}→robust={robust:.3f}  "
              f"base ketto有効率={kcov:.3f}  ketto∩(base,hist)={len(bk & hk):,}")
        print(f"  [from-store] KYI jrdb列={len([c for c in kyi.columns if str(c).startswith('jrdb_')])} "
              f"history rows={len(hist):,} soten rows={len(soten):,} "
              f"base ketto={'あり' if 'ketto' in base.columns else 'なし'}")
        return attach(base, kyi, hist, soten=soten)
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
    from src.training._feature_materialization import (EXPECTED_JRDB_FULL, REQUIRED_JRDB_MIN,
                                                       assert_features_materialized)

    ap = argparse.ArgumentParser(description="JRDB 完全 augment 出力の feature-only 認定（性能を見ない）")
    ap.add_argument("--from-store", action="store_true", help="JRDB store から augment 構築（raw txt 不要・推奨）")
    ap.add_argument("--augmented", default=None, help="jrdb_build_features.py 出力 pickle")
    ap.add_argument("--jrdb-dir", default=None, help="JRDB txt ディレクトリ（その場 augment）")
    args = ap.parse_args()

    print("=" * 88)
    print("JRDB 完全 augment 検証（feature-only・38特徴の実体化認定・性能は見ない）")
    try:
        feat = _load_augmented(args)
    except RuntimeError as e:
        print(f"[エラー] {e}", file=sys.stderr)
        return 2
    base = load_featured_data()
    base_cols = set(base.columns) if base is not None else set()

    rid = pd.Series(feat.index.astype(str))
    jra = rid.str[4:6].isin({f"{i:02d}" for i in range(1, 11)})
    year = pd.to_numeric(rid.str[:4], errors="coerce")
    sel = (jra & (year >= 2015)).to_numpy()
    fj = feat[sel]

    print(f"[入力] augmented rows={len(feat):,}  JRA2015+={len(fj):,}  base featured 列数={len(base_cols)}")
    print(f"  base(現5列)に在る EXPECTED: {sorted(set(EXPECTED_JRDB_FULL) & base_cols)}")

    ok, thin, absent = [], [], []
    print(f"\n  {'特徴':<24}{'実在':>4}{'非欠測':>8}{'sentinel':>9}{'分散有率':>9}{'新規':>5}{'判定':>7}")
    for c in EXPECTED_JRDB_FULL:
        newly = "新" if c not in base_cols else "既"
        if c not in feat.columns:
            absent.append(c)
            print(f"  {c:<24}{'無':>4}{'—':>8}{'—':>9}{'—':>9}{newly:>5}{'ABSENT':>7}")
            continue
        col = pd.to_numeric(fj[c], errors="coerce")
        nm = float(col.notna().mean()) if len(col) else 0.0
        sent = float((col <= -99).mean()) if len(col) else 0.0
        vf = _within_race_var_frac(fj, c)
        inf = int(np.isinf(col.to_numpy(dtype=float, na_value=np.nan)).sum())
        v = "OK" if (nm >= 0.3 and sent < 0.2 and vf > 0.1) else ("DEAD" if nm < 0.02 else "薄い")
        (ok if v == "OK" else thin).append(c)
        flag = "!inf" if inf else ""
        print(f"  {c:<24}{'有':>4}{nm:>8.3f}{sent:>9.3f}{vf:>9.3f}{newly:>5}{v:>7}{flag}")

    print(f"\n[認定] OK={len(ok)}  薄い/DEAD={len(thin)}  ABSENT={len(absent)}  / EXPECTED={len(EXPECTED_JRDB_FULL)}")
    if absent:
        print(f"  ABSENT（augment 後も欠落＝実装/結合の不備）: {absent}")
    if thin:
        print(f"  薄い/DEAD（要 sentinel/coverage 精査）: {thin}")

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

    print("\n[判定基準] OK=非欠測>=0.3 & sentinel<0.2 & race内分散有率>0.1。ABSENT が 0 で大半 OK なら"
          "\n  standalone augment は 38 を正しく materialize＝本線統合へ進める。ABSENT/薄いが多いなら"
          "\n  結合キー/sentinel/年 coverage を先に修正。性能評価は standing protocol(2027)で別途。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
