"""1 特徴列を年別＋fresh/artifact parity で診断する（分類を推測でなく証拠で確定する・続37）。

例: jrdb_kokyu_flag は audit(2020+)で race内分散有率=0 だが verify(2015+)で 0.129＝古い年度のみ変動の疑い。
本ツールで **年別の value_counts / global unique / 非ゼロ率 / race内分散有率** と、**saved artifact vs
fresh augment(store) の (race_id,馬番) 一致率・dtype** を出し、TEMPORALLY_DEAD / CONTEXT / DEAD / ACTIVE /
SCHEMA_DRIFT を確定する。性能は見ない。要ローカル（featured＋JrdbStore）。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _within_race_var_frac(sub, col):
    import pandas as pd
    s = pd.to_numeric(sub[col], errors="coerce")
    nun = s.groupby(sub.index).nunique(dropna=True)
    return float((nun > 1).mean()) if len(nun) else 0.0


def main() -> int:
    import numpy as np
    import pandas as pd
    from app._model_eval import load_featured_data
    from src.constants._results_cols import ResultsCols

    ap = argparse.ArgumentParser(description="1特徴の年別＋fresh/artifact parity 診断（分類確定用）")
    ap.add_argument("--col", required=True, help="診断する列（例 jrdb_kokyu_flag）")
    ap.add_argument("--featured", default="data/featured_jrdb.pkl", help="saved artifact pickle")
    ap.add_argument("--min-year", type=int, default=2015)
    args = ap.parse_args()
    col = args.col

    print("=" * 84)
    print(f"特徴診断: {col}（年別変動＋fresh/artifact parity・性能は見ない）")
    feat = load_featured_data(args.featured)
    if feat is None or feat.empty:
        print(f"[エラー] featured を読めません: {args.featured}", file=sys.stderr)
        return 2
    if col not in feat.columns:
        print(f"[エラー] {col} が {args.featured} に無い", file=sys.stderr)
        return 2

    rid = pd.Series(feat.index.astype(str))
    year = pd.to_numeric(rid.str[:4], errors="coerce")
    jra = rid.str[4:6].isin({f"{i:02d}" for i in range(1, 11)})
    sel = (jra & (year >= args.min_year)).to_numpy()
    fj = feat[sel]
    yj = pd.to_numeric(pd.Series(fj.index.astype(str)).str[:4], errors="coerce").to_numpy()

    print(f"[対象] artifact={args.featured}  rows(JRA{args.min_year}+)={len(fj):,}  "
          f"dtype={feat[col].dtype}")
    print(f"\n  {'年':>6}{'rows':>9}{'global_unique':>14}{'非ゼロ率':>9}{'race内分散有率':>13}  top_values")
    for y in sorted(set(int(v) for v in yj if not np.isnan(v))):
        sub = fj[yj == y]
        c = pd.to_numeric(sub[col], errors="coerce")
        nun = int(c.nunique(dropna=True))
        nz = float((c.fillna(0) != 0).mean()) if len(c) else 0.0
        vf = _within_race_var_frac(sub, col)
        vc = c.value_counts(dropna=True).head(3).to_dict()
        vc = {round(float(k), 3): int(v) for k, v in vc.items()}
        print(f"  {y:>6}{len(sub):>9,}{nun:>14}{nz:>9.3f}{vf:>13.3f}  {vc}")

    # fresh augment (store) と (race_id,馬番) parity
    print(f"\n[parity] saved artifact vs fresh augment(store) を (race_id,馬番) で照合")
    try:
        from src.jrdb._augment import build_kyi_from_df
        from src.jrdb._store import JrdbStore
        kyi = build_kyi_from_df(JrdbStore().read("KYI"))
    except Exception as e:  # noqa: BLE001
        print(f"  store 読込不可でスキップ: {e}")
        kyi = None
    if kyi is not None and col in kyi.columns:
        b = pd.DataFrame({"race_id": fj.index.astype(str).to_numpy(),
                          "umaban": pd.to_numeric(fj.get(ResultsCols.UMABAN),
                                                  errors="coerce").astype("Int64").to_numpy(),
                          "saved": pd.to_numeric(fj[col], errors="coerce").to_numpy()})
        k = kyi[["race_id", "umaban", col]].rename(columns={col: "fresh"}).copy()
        k["race_id"] = k["race_id"].astype(str)
        k["umaban"] = pd.to_numeric(k["umaban"], errors="coerce").astype("Int64")
        m = b.merge(k, on=["race_id", "umaban"], how="inner")
        both = m["saved"].notna() & m["fresh"].notna()
        eq = float((np.isclose(m.loc[both, "saved"], m.loc[both, "fresh"])).mean()) if both.any() else 0.0
        print(f"  照合 n={int(both.sum()):,}  値一致率={eq:.4f}  fresh dtype={kyi[col].dtype}")
        fresh_vf_recent = None
        if "race_id" in kyi.columns:
            ky = kyi.copy()
            ky["_y"] = pd.to_numeric(ky["race_id"].astype(str).str[:4], errors="coerce")
            fr = ky[(ky["_y"] >= 2020)]
            if len(fr):
                s = pd.to_numeric(fr[col], errors="coerce")
                nun = s.groupby(fr["race_id"].astype(str)).nunique(dropna=True)
                fresh_vf_recent = float((nun > 1).mean()) if len(nun) else 0.0
        print(f"  fresh の 2020+ race内分散有率={fresh_vf_recent}")
    elif kyi is not None:
        print(f"  {col} は KYI 由来でない（fresh kyi に無し）＝parity スキップ")

    print("\n[分類指針] 年別 race内分散有率を見て確定:")
    print("  ・全年で >0.1                      → ACTIVE（そのまま）")
    print("  ・古い年のみ >0・近年 0            → TEMPORALLY_DEAD（近年 JRDB 停止/定数化）")
    print("  ・race内 0 だが race間で変動        → CONTEXT_ONLY（race定数・pace_hms と同扱い）")
    print("  ・saved は 0 だが fresh は >0       → SCHEMA_DRIFT（保存 artifact 不整合＝再build）")
    print("  ・全年・全行で単一値                → DEAD")
    return 0


if __name__ == "__main__":
    sys.exit(main())
