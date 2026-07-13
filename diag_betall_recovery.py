"""walk_forward --by-odds が回収率1.324（非現実的>1）を出す原因を featured 単体で切り分ける。

_tansho_oos_walk_forward の払戻は「勝ち馬(着順==1)の単勝オッズ × stake」。この払戻機構が
健全なら、EV 選択を挟まず**全馬を単勝で買った回収率**は市場の控除率どおり ≈0.80 になるはず
（Σ勝ち馬オッズ / Σ頭数 ≈ 1-takeout）。

分岐:
  (A) 全馬買い回収率 ≈ 0.80 → オッズ・勝ち馬判定は正しい＝払戻復元は健全。
      → walk_forward の >1 は EV 選択(モデル)側の異常（リーク疑い）。次はモデル特徴を精査。
  (B) 全馬買い回収率 ≈ 1.3 等 >1 → featured の 単勝/着順 自体が壊れている＝復元バグ。
      候補: ①単勝オッズが最終確定値でなく歪んでいる ②着順==1 が1レース複数(重複行/着順コード)
      → overround Σ(1/単勝) と 1レースあたり勝ち馬数で特定する。

診断項目（年代別＋全体）:
  - 全馬買い回収率 = Σ(勝ち馬単勝) / Σ(頭数)      … 健全なら ≈0.80
  - 1レース勝ち馬数 mean                           … 健全なら ≈1.00（>1なら着順重複）
  - overround mean = Σ(1/単勝) per race            … 健全なら ≈1.25（20%控除）。<1なら単勝が過大
  - 単勝オッズ分位（min/1着馬中央値 等）

実行例: python diag_betall_recovery.py
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
    ("全体", 1900, 2100),
]


def main():
    import numpy as np
    import pandas as pd

    from app._model_eval import load_featured_data
    from src.constants._results_cols import ResultsCols

    odds_col = ResultsCols.TANSHO_ODDS  # '単勝'
    rank_col = ResultsCols.RANK          # '着順'

    feat = load_featured_data()
    if feat is None or feat.empty:
        print("featured_data がありません"); return
    for c in (odds_col, rank_col):
        if c not in feat.columns:
            print(f"featured に列 '{c}' が無い"); return

    df = pd.DataFrame({
        "race_id": (feat["race_id"] if "race_id" in feat.columns
                    else pd.Series(feat.index, index=feat.index)).astype(str).to_numpy(),
        "odds": pd.to_numeric(feat[odds_col], errors="coerce").to_numpy(),
        "rank": pd.to_numeric(feat[rank_col], errors="coerce").to_numpy(),
        "year": pd.to_datetime(feat["date"], errors="coerce").dt.year.to_numpy(),
    })

    # レース単位の集計材料
    g = df.groupby("race_id")
    per_race = pd.DataFrame({
        "n_horses": g.size(),
        "year": g["year"].first(),
        "overround": g["odds"].apply(lambda o: np.nansum(1.0 / o[o > 0])),
        "n_winners": g["rank"].apply(lambda r: int((r == 1).sum())),
        # 勝ち馬(着順==1)の単勝オッズ合計（複数いれば全部足す＝機構どおり）
        "win_odds_sum": g.apply(
            lambda x: float(np.nansum(x.loc[x["rank"] == 1, "odds"].to_numpy())),
            include_groups=False,
        ),
    })

    print(f"featured: {len(df):,}行 / {len(per_race):,}レース")
    print(f"\n  {'年代':<12}{'レース':>9}{'全馬買い回収率':>14}{'勝ち馬数/R':>12}"
          f"{'overround':>11}{'1着馬odds中央':>14}")
    for lab, y0, y1 in ERA_BUCKETS:
        m = (per_race["year"] >= y0) & (per_race["year"] <= y1)
        pr = per_race[m]
        if pr.empty:
            continue
        # 全馬買い回収率 = Σ勝ち馬オッズ / Σ頭数（フラット100円なので stake は約分）
        recov = pr["win_odds_sum"].sum() / pr["n_horses"].sum()
        nwin = pr["n_winners"].mean()
        over = pr["overround"].replace([np.inf, -np.inf], np.nan).mean()
        dm = df[(df["year"] >= y0) & (df["year"] <= y1)]
        win_odds_med = np.nanmedian(dm.loc[dm["rank"] == 1, "odds"].to_numpy())
        print(f"  {lab:<12}{len(pr):>9,}{recov:>14.3f}{nwin:>12.3f}"
              f"{over:>11.3f}{win_odds_med:>14.2f}")

    print("\n判定:")
    print("  ・全馬買い回収率 ≈0.80 かつ 勝ち馬数/R≈1.0 かつ overround≈1.25 →")
    print("      払戻復元は健全。walk_forward の>1 は EV選択(モデル)側の異常＝特徴リークを疑う。")
    print("  ・全馬買い回収率>1 → featured の単勝/着順が壊れている:")
    print("      勝ち馬数/R>1 なら着順重複、overround<1 なら単勝オッズが過大（最終確定値でない等）。")


if __name__ == "__main__":
    main()
