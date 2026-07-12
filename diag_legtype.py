"""脚質特徴(leg_type_binary / pace_median)が年度別にどれだけ変動するかを診断する。

pace_target で ≤2021 の脚質が全馬同値(unique=1)と判明。原因が「全 era で縮退」か
「archive era(≤2021) だけ縮退で recent は正常」かを切り分ける。併せて 通過列の有無と
被覆率、first_corner の由来を確認し、rebuild-featured の要否を判断する。

実行例: python diag_legtype.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))


def main():
    import numpy as np
    import pandas as pd

    from app._model_eval import load_featured_data
    from src.constants._local_paths import LocalPaths

    feat = load_featured_data()
    if feat is None or feat.empty:
        print("featured_data がありません")
        return

    yr = pd.to_datetime(feat["date"]).dt.year
    print(f"featured: {len(feat):,}行 / {feat.index.nunique():,}レース")
    print("列存在:", {c: (c in feat.columns) for c in
                    ["leg_type_binary", "pace_median", "pace_at_distance", "通過", "course_len"]})
    print("-" * 70)
    print(f"{'年':>6}{'行数':>9}{'lt_uniq':>8}{'lt_mean':>9}{'lt_%fin':>9}"
          f"{'pm_uniq':>8}{'pm_std':>8}")
    for y in sorted(yr.dropna().unique()):
        sub = feat[yr == y]
        row = [f"{int(y):>6}", f"{len(sub):>9,}"]
        for col, kind in (("leg_type_binary", "lt"), ("pace_median", "pm")):
            if col in sub.columns:
                v = pd.to_numeric(sub[col], errors="coerce")
                vf = v.dropna()
                nuq = vf.round(4).nunique()
                if kind == "lt":
                    row += [f"{nuq:>8}", f"{(vf.mean() if len(vf) else float('nan')):>9.3f}",
                            f"{100*len(vf)/max(len(sub),1):>8.0f}%"]
                else:
                    row += [f"{nuq:>8}", f"{(vf.std() if len(vf) else float('nan')):>8.3f}"]
            else:
                row += ["   -", "   -", "   -"] if kind == "lt" else ["   -", "   -"]
        print("".join(row))
    print("-" * 70)

    # results.pkl の 通過 被覆（脚質の元データ）を年度別に見る
    rpath = Path(LocalPaths.RAW_RESULTS_PATH)
    if rpath.exists():
        res = pd.read_pickle(rpath)
        print(f"\nresults.pkl: {len(res):,}行  列に'通過'={'通過' in res.columns}")
        if "通過" in res.columns:
            rid = res.index.get_level_values(0) if res.index.nlevels > 1 else res.index
            ry = pd.Series(rid.astype(str).str[:4], index=res.index)
            tp = res["通過"].astype(str)
            has = (tp.str.len() > 0) & (tp != "nan") & (tp != "None")
            cov = has.groupby(ry).mean()
            print("通過 被覆率（年代抜粋）:")
            for y in sorted(cov.index)[::max(1, len(cov)//12)]:
                print(f"  {y}: {100*cov[y]:.0f}%  (例: {tp[ry == y].iloc[0][:20] if (ry==y).any() else ''})")

    # horse_results の first_corner 被覆（脚質算出の直接入力）
    hpath = Path(LocalPaths.HTML_HORSE_RESULTS_PATH)
    hr = None
    for p in (hpath, Path(LocalPaths.RAW_HORSE_RESULTS_PATH)):
        if p.exists():
            hr = pd.read_pickle(p); print(f"\nhorse_results: {p} / {len(hr):,}行"); break
    if hr is not None:
        print("horse_results 列:", [c for c in ("first_corner", "通過", "頭数", "date") if c in hr.columns])
        if "first_corner" in hr.columns:
            fc = pd.to_numeric(hr["first_corner"], errors="coerce")
            print(f"first_corner: finite={fc.notna().mean()*100:.0f}% unique={fc.dropna().round(0).nunique()} "
                  f"min={fc.min()} max={fc.max()}")


if __name__ == "__main__":
    main()
