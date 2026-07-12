"""archive era の脚質が全馬 1.0 に潰れる箇所を、再構築を実走して特定する。

results.pkl（date 無し）に race_info.pkl（date/course_len/race_type）をマージしてから
build_horse_results_from_results を era 別に実走し、first_corner / 頭数 / _pace_num の
分布を出す。どの中間量が縮退しているかで原因を切る。スキーマ不明でも動くよう自己記述する。

実行例: python diag_recon.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))


def _find_race_id(df):
    """race_id を列 or index から取り出して Series で返す。"""
    import pandas as pd
    if "race_id" in df.columns:
        return df["race_id"].astype(str)
    if df.index.name == "race_id":
        return pd.Series(df.index.astype(str), index=df.index)
    if df.index.nlevels > 1 and "race_id" in (df.index.names or []):
        return pd.Series(df.index.get_level_values("race_id").astype(str), index=df.index)
    # 先頭 index を race_id とみなす
    return pd.Series(df.index.get_level_values(0).astype(str), index=df.index)


def main():
    import numpy as np
    import pandas as pd

    from src.constants._horse_results_cols import HorseResultsCols as HRCols
    from src.constants._local_paths import LocalPaths
    from src.preprocessing._horse_features import build_horse_results_from_results
    from src.preprocessing._horse_results_processor import parse_corner

    res = pd.read_pickle(LocalPaths.RAW_RESULTS_PATH)
    ri = pd.read_pickle(LocalPaths.RAW_RACE_INFO_PATH)
    print(f"results : shape={res.shape} index={res.index.name} 列={list(res.columns)}")
    print(f"race_info: shape={ri.shape} index={ri.index.name} 列={list(ri.columns)}")

    res = res.copy()
    res["race_id"] = _find_race_id(res).to_numpy()
    ri = ri.copy()
    ri["race_id"] = _find_race_id(ri).to_numpy()

    # race_info から date/course_len/race_type/ground を results へ付与
    join_cols = [c for c in ("date", "course_len", "race_type", "ground_state1", "ground_state2")
                 if c in ri.columns]
    ri_small = ri[["race_id", *join_cols]].drop_duplicates("race_id")
    merged = res.merge(ri_small, on="race_id", how="left")
    print(f"merged: {len(merged):,}行  付与列={join_cols}")

    if "date" not in merged.columns:
        print("race_info に date が無い。年代分けできない。")
        return
    yr = pd.to_datetime(merged["date"], errors="coerce").dt.year
    print("date 年分布:", {int(k): int(v) for k, v in yr.value_counts().sort_index().items()
          if k in (1986, 2000, 2021, 2023, 2026)})

    if "horse_id" in merged.columns:
        vc = merged["horse_id"].astype(str).value_counts()
        for lab, msk in (("archive≤2021", yr <= 2021), ("recent≥2023", yr >= 2023)):
            if msk.any():
                ids = merged.loc[msk, "horse_id"].astype(str)
                per = ids.map(vc)
                print(f"[{lab}] horse_id 出走数/行: median={per.median():.0f} "
                      f"1走のみ={100*(per == 1).mean():.0f}%  ユニーク馬={ids.nunique():,}")

    for lab, msk in (("archive≤2021", yr <= 2021), ("recent≥2023", yr >= 2023)):
        if not msk.any():
            print(f"\n[{lab}] 該当なし"); continue
        sub = merged.loc[msk]
        fc_raw = (pd.to_numeric(sub["通過"].map(lambda x: parse_corner(x, 1)), errors="coerce")
                  if "通過" in sub.columns else pd.Series(np.nan, index=sub.index))
        hr = build_horse_results_from_results(sub)
        print(f"\n[{lab}] {len(sub):,}行 → 再構築 horse_results: {len(hr):,}行 列={list(hr.columns)}")

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
                print(f"    → _pace_num>=0.5 割合={100*(pn >= 0.5).mean():.0f}% "
                      f"(≈100%なら全馬『追込1.0』に潰れる直接原因)")
        print(f"    date finite={100*pd.to_datetime(hr['date'], errors='coerce').notna().mean():.0f}%"
              if "date" in hr.columns else "    date列なし")


if __name__ == "__main__":
    main()
