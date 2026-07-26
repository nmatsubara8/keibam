"""旧年代の jockey/trainer/owner 統計リーク源を特定する（ID 健全性の年代別実測）。

walk_forward --quality で旧年代のみモデル logloss が市場を 0.2nat 下回る＝リーク。
debug_leak の年代比較で、旧年代のみ急上昇する列は人物統計（jockey/trainer/owner_avg_rank_z
＝0.879・3列完全同一・近代<0.68）に局在。_attach_jockey_trainer_stats は
groupby(id).shift(1).rolling(N).mean() で人物の直近成績を作るが、旧年代で id が
潰れる（None/同一値）と groupby が壊れ、同一レース馬の結果を混ぜてリークし得る。

本診断は raw_results と featured から機序を確定する:
  (1) raw の jockey_id/trainer_id/owner_id の年代別 ユニーク数・null率・相互一致率・horse_id一致率
  (2) featured の人物統計列が3者で同一値か（相関・一致率）、null率
  → 旧年代で「3 ID が同一 or null に潰れ、人物統計が縮退」なら機序確定。

実行例: python diag_person_ids.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

ERA_BUCKETS = [
    ("1986-1992", 1986, 1992),
    ("1993-2000", 1993, 2000),
    ("2001-2008", 2001, 2008),
    ("2009-2016", 2009, 2016),
    ("2017-2026", 2017, 2026),
]


def _year_from_race_id(rid):
    import pandas as pd
    txt = pd.Series(rid).astype(str).str.replace(r"\.0$", "", regex=True)
    return pd.to_numeric(txt.str[:4], errors="coerce")


def main():
    import os

    import numpy as np
    import pandas as pd

    from src.constants._local_paths import LocalPaths

    # (1) raw_results の ID 健全性
    rpath = LocalPaths.RAW_RESULTS_PATH
    if not os.path.exists(rpath):
        print(f"raw_results.pkl が無い（{rpath}）"); return
    raw = pd.read_pickle(rpath)
    if "jockey_id" not in raw.columns:
        print(f"raw_results に jockey_id 列が無い（列: {list(raw.columns)[:20]}）"); return
    rid = raw["race_id"] if "race_id" in raw.columns else pd.Series(raw.index, index=raw.index)
    year = _year_from_race_id(rid).to_numpy()

    def _s(col):
        return raw[col].astype(str).str.replace(r"\.0$", "", regex=True).to_numpy() if col in raw.columns else None

    jk, tr, ow, ho = _s("jockey_id"), _s("trainer_id"), _s("owner_id"), _s("horse_id")

    print("[raw_results ID 健全性]（人物統計 groupby の入力）")
    print(f"  {'年代':<12}{'行数':>9}{'jk uniq':>9}{'tr uniq':>9}{'ow uniq':>9}"
          f"{'jk null%':>9}{'jk=tr=ow%':>11}{'jk=horse%':>10}")
    NULLS = {"", "nan", "None", "NaN", "<NA>"}
    for lab, y0, y1 in ERA_BUCKETS:
        m = (year >= y0) & (year <= y1)
        n = int(m.sum())
        if n == 0:
            continue
        jkm = jk[m]
        jk_null = float(np.isin(jkm, list(NULLS)).mean()) * 100
        all_eq = (float(((jkm == tr[m]) & (jkm == ow[m])).mean()) * 100
                  if tr is not None and ow is not None else float("nan"))
        jk_horse = float((jkm == ho[m]).mean()) * 100 if ho is not None else float("nan")
        print(f"  {lab:<12}{n:>9,}{pd.unique(jkm).size:>9,}{pd.unique(tr[m]).size:>9,}"
              f"{pd.unique(ow[m]).size:>9,}{jk_null:>8.1f}%{all_eq:>10.1f}%{jk_horse:>9.1f}%")

    # (2) featured の人物統計列
    from app._model_eval import load_featured_data
    feat = load_featured_data()
    if feat is None or feat.empty:
        print("\nfeatured_data がありません"); return
    pcols = [c for c in ("jockey_avg_rank", "trainer_avg_rank", "owner_avg_rank",
                         "jockey_win_rate", "trainer_win_rate", "owner_win_rate") if c in feat.columns]
    if not pcols:
        print("\nfeatured に人物統計列が無い"); return
    fyear = pd.to_datetime(feat["date"], errors="coerce").dt.year.to_numpy()
    print("\n[featured 人物統計]（jockey vs trainer vs owner が同一値なら縮退＝リーク機序）")
    print(f"  {'年代':<12}{'jk_rank null%':>14}{'jk==tr==ow(rank)%':>18}{'jk_rank std':>12}")
    ja, ta, oa = (pd.to_numeric(feat.get("jockey_avg_rank"), errors="coerce").to_numpy(),
                  pd.to_numeric(feat.get("trainer_avg_rank"), errors="coerce").to_numpy(),
                  pd.to_numeric(feat.get("owner_avg_rank"), errors="coerce").to_numpy())
    for lab, y0, y1 in ERA_BUCKETS:
        m = (fyear >= y0) & (fyear <= y1)
        if not m.any():
            continue
        jam = ja[m]
        null_pct = float(np.isnan(jam).mean()) * 100
        # 3者が（NaN 同士含め）ほぼ一致する行の割合
        both = ~np.isnan(jam) & ~np.isnan(ta[m]) & ~np.isnan(oa[m])
        if both.any():
            eq = float((np.isclose(jam[both], ta[m][both]) & np.isclose(jam[both], oa[m][both])).mean()) * 100
        else:
            eq = float("nan")
        std = float(np.nanstd(jam))
        print(f"  {lab:<12}{null_pct:>13.1f}%{eq:>17.1f}%{std:>12.4f}")

    print("\n判定:")
    print("  ・旧年代で jk uniq が極小 or jk null% 高 or jk=tr=ow% 高 → ID 潰れ確定。")
    print("  ・featured で旧年代の jk==tr==ow(rank)% が高い → 人物統計が3者同一に縮退＝リーク機序。")
    print("  対処案: ID が潰れている年代は人物統計を NaN 化（縮退値を使わせない）か、")
    print("          groupby を (id, race_id) 同一レース除外に変え same-race リークを断つ。")


if __name__ == "__main__":
    main()
