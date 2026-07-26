"""archive脚質が1.0定数のまま残る原因＝頭数(n_horses)縮退 仮説を featured 自身で検証する。

date パース修正後も archive の leg_type が全馬1.0のまま。build_horse_results_from_results は
self._results['n_horses'] を頭数に使うため、archive の n_horses が壊れて小さいと
_pace_num=first_corner/頭数 が clip で 1.0 に張り付き全馬同値になる。featured は
n_horses/通過/horse_id/date を持つので、featured 自身から再構築→add_pace_stats を
パイプライン相当で再現し、n_horses の健全性と leg_type 変動を era 別に確定する。

実行例: python diag_nhorses.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))


def main():
    import numpy as np
    import pandas as pd

    from app._model_eval import load_featured_data
    from src.preprocessing._horse_features import add_pace_stats, build_horse_results_from_results

    feat = load_featured_data()
    if feat is None or feat.empty:
        print("featured_data がありません"); return
    yr = pd.to_datetime(feat["date"], errors="coerce").dt.year

    print("[featured n_horses 健全性]（reconstruction が頭数に使う値）")
    for lab, msk in (("archive≤2021", yr <= 2021), ("recent≥2023", yr >= 2023)):
        if "n_horses" in feat.columns and msk.any():
            nh = pd.to_numeric(feat.loc[msk, "n_horses"], errors="coerce").dropna()
            print(f"  {lab}: n_horses median={nh.median():.0f} min={nh.min():.0f} "
                  f"max={nh.max():.0f} uniq={nh.round(0).nunique()} "
                  f"（正常なら median≈14-16 / 異常なら 1 等の定数）")
        elif "n_horses" not in feat.columns:
            print("  featured に n_horses 列なし"); break

    # featured 自身から reconstruction→add_pace_stats を era別に再現（パイプライン相当）
    need = [c for c in ("horse_id", "date", "着順", "通過", "course_len", "n_horses") if c in feat.columns]
    print(f"\n再現に使う featured 列: {need}")
    fr = feat.reset_index()
    if "race_id" not in fr.columns and feat.index.name:
        fr = fr.rename(columns={feat.index.name: "race_id"})
    for lab, msk in (("archive≤2021", (yr <= 2021).to_numpy()), ("recent≥2023", (yr >= 2023).to_numpy())):
        if not msk.any():
            continue
        sub = fr.loc[msk, [c for c in ("race_id", *need) if c in fr.columns]].copy()
        # (A) パイプライン相当: featured の n_horses をそのまま使う
        hrA = build_horse_results_from_results(sub)
        outA = add_pace_stats(sub.copy(), hrA) if not hrA.empty else sub
        # (B) n_horses を落として groupby 実頭数で再構築（正しい頭数）
        subB = sub.drop(columns=[c for c in ["n_horses"] if c in sub.columns])
        hrB = build_horse_results_from_results(subB)
        outB = add_pace_stats(subB.copy(), hrB) if not hrB.empty else subB

        def _lt(o):
            if "leg_type_binary" not in o.columns:
                return "列なし"
            v = pd.to_numeric(o["leg_type_binary"], errors="coerce").dropna()
            return f"uniq={v.round(2).nunique()} mean={v.mean():.3f} (n={len(v):,})"
        # 頭数分布も併記
        def _nh(hr):
            from src.constants._horse_results_cols import HorseResultsCols as HRC
            if HRC.N_HORSES not in hr.columns:
                return "頭数列なし"
            v = pd.to_numeric(hr[HRC.N_HORSES], errors="coerce").dropna()
            return f"median={v.median():.0f} min={v.min():.0f} uniq={v.round(0).nunique()}"
        print(f"\n[{lab}] {len(sub):,}行")
        print(f"  (A) featured n_horses 使用 : 頭数[{_nh(hrA)}]  leg_type[{_lt(outA)}]")
        print(f"  (B) groupby 実頭数 で再構築: 頭数[{_nh(hrB)}]  leg_type[{_lt(outB)}]")
    print("\n判定: (A)が定数(uniq=1)で(B)が変動(uniq=2)なら、原因は featured の n_horses 縮退で確定。")


if __name__ == "__main__":
    main()
