"""archive era の脚質が全馬 1.0 に潰れる箇所を、再構築を実走して特定する。

results.pkl（通過 ~100% 有り）から build_horse_results_from_results を実走し、
first_corner / 頭数 / _pace_num（=first_corner/頭数）の分布を archive/recent 別に出す。
どの中間量が縮退しているかで原因（通過パース失敗 / 頭数異常 / horse_id 連結欠落 / date 欠落）を切る。

実行例: python diag_recon.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))


def main():
    import numpy as np
    import pandas as pd

    from src.constants._horse_results_cols import HorseResultsCols as HRCols
    from src.constants._local_paths import LocalPaths
    from src.preprocessing._horse_features import build_horse_results_from_results

    res = pd.read_pickle(LocalPaths.RAW_RESULTS_PATH)
    print(f"results.pkl: {len(res):,}行  index={res.index.name}  列抜粋="
          f"{[c for c in ('horse_id','date','通過','n_horses','course_len','race_type','着順') if c in res.columns]}")

    # race_id 先頭4桁で era 分け（archive=合成id, recent=2023+）。date から year も見る。
    if "date" in res.columns:
        yr = pd.to_datetime(res["date"], errors="coerce").dt.year
        print("date 年分布(抜粋):", {int(k): int(v) for k, v in
              yr.value_counts().sort_index().head(3).items()},
              "…", {int(k): int(v) for k, v in yr.value_counts().sort_index().tail(3).items()})
    else:
        yr = pd.Series(np.nan, index=res.index)

    # horse_id 連結: 各 horse_id が何レースに出るか（1なら連結欠落＝career無し）
    if "horse_id" in res.columns:
        vc = res["horse_id"].astype(str).value_counts()
        arch_mask = yr <= 2021
        rec_mask = yr >= 2023
        for lab, msk in (("archive≤2021", arch_mask), ("recent≥2023", rec_mask)):
            if msk.any():
                ids = res.loc[msk, "horse_id"].astype(str)
                per = ids.map(vc)
                print(f"[{lab}] horse_id 出走数/馬: median={per.median():.0f} "
                      f"1走のみ={100*(per==1).mean():.0f}%  ユニーク馬={ids.nunique():,}")

    # 再構築を era 別に実走して中間量の分布を見る
    from src.preprocessing._horse_results_processor import parse_corner
    for lab, msk in (("archive≤2021", yr <= 2021), ("recent≥2023", yr >= 2023)):
        if not msk.any():
            print(f"\n[{lab}] 該当なし"); continue
        sub = res.loc[msk]
        # 直接 通過→first_corner を検算（再構築を通さず raw で）
        if "通過" in sub.columns:
            fc_raw = pd.to_numeric(sub["通過"].map(lambda x: parse_corner(x, 1)), errors="coerce")
        else:
            fc_raw = pd.Series(np.nan, index=sub.index)
        hr = build_horse_results_from_results(sub.reset_index() if sub.index.name else sub)
        print(f"\n[{lab}] 再構築 horse_results: {len(hr):,}行 列={list(hr.columns)}")
        def _desc(s, name):
            s = pd.to_numeric(s, errors="coerce")
            sf = s.dropna()
            if not len(sf):
                print(f"    {name}: 全欠損"); return
            print(f"    {name}: finite={100*len(sf)/len(s):.0f}% "
                  f"min={sf.min():.2f} median={sf.median():.2f} max={sf.max():.2f} uniq={sf.round(2).nunique()}")
        _desc(fc_raw, "first_corner(raw検算)")
        if "first_corner" in hr.columns:
            _desc(hr["first_corner"], "first_corner(再構築)")
        if HRCols.N_HORSES in hr.columns:
            _desc(hr[HRCols.N_HORSES], "頭数")
            if "first_corner" in hr.columns:
                pn = (pd.to_numeric(hr["first_corner"], errors="coerce")
                      / pd.to_numeric(hr[HRCols.N_HORSES], errors="coerce")).clip(0, 1)
                _desc(pn, "_pace_num=fc/頭数(clip)")
                print(f"    → _pace_num>=0.5 の割合={100*(pn>=0.5).mean():.0f}%  "
                      f"(高いほど全馬『追込(1.0)』に潰れる)")
        if "date" in hr.columns:
            _desc(pd.to_datetime(hr["date"], errors="coerce").astype("int64", errors="ignore"), "date(有効性)")


if __name__ == "__main__":
    main()
