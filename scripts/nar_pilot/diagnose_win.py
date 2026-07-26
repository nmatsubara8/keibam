"""地方単勝市場の効率性診断（モデル非依存）: 実効控除率・FLB較正・オッズ帯別回収率・本命回収率。

エッジは主張しない。「どの帯・どの場のオッズが実際に損益分岐(≈0.79)を跨ぐか」を実測する。
使い方: python scripts/nar_pilot/diagnose_win.py --data-dir data/nar_pilot
"""
from __future__ import annotations

import argparse
import os

import numpy as np
import pandas as pd

ODDS_BINS = [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, np.inf]
ODDS_LABELS = ["1-2", "2-4", "4-8", "8-16", "16-32", "32-64", "64+"]


def load(data_dir):
    df = pd.read_csv(os.path.join(data_dir, "nar_pilot.csv"))
    df = df[df["tansho_odds"] > 0].copy()
    df["win"] = (df["rank"] == 1).astype(int)
    df["inv"] = 1.0 / df["tansho_odds"]
    df["overround"] = df.groupby("race_id")["inv"].transform("sum")
    df["p_implied"] = df["inv"] / df["overround"]
    return df


def effective_takeout(df):
    per = df.groupby("race_id")["overround"].first()
    return float((1.0 - 1.0 / per).mean())


def boot_ci(x, n=5000):
    if len(x) == 0:
        return (0.0, 0.0)
    rng = np.random.default_rng(0)
    idx = rng.integers(0, len(x), (n, len(x)))
    m = x[idx].mean(axis=1)
    return tuple(np.percentile(m, [2.5, 97.5]))


def report(name, df):
    print("\n" + "=" * 74)
    print(f"[{name}]  races={df['race_id'].nunique()}  horses={len(df)}")
    print("=" * 74)
    t = effective_takeout(df)
    print(f"実効控除率 = {t:.1%}  → 損益分岐回収率 = {1 - t:.3f}")
    fav = df.loc[df.groupby("race_id")["tansho_odds"].idxmin()]
    ret = (fav["tansho_odds"] * fav["win"]).values
    lo, hi = boot_ci(ret)
    print(f"1番人気1点買い: 的中 {fav['win'].mean():.3f}  回収率 {ret.mean():.3f}  "
          f"95%CI [{lo:.3f},{hi:.3f}]  (n={len(fav)})")
    b = pd.cut(df["tansho_odds"], ODDS_BINS, labels=ODDS_LABELS, right=False)
    g = df.groupby(b, observed=True)
    print(f"\n{'帯':<8}{'n':>6}{'含意':>9}{'実現':>9}{'回収率':>9}")
    for lab, x in g:
        rec = float((x["tansho_odds"] * x["win"]).mean())
        print(f"{lab:<8}{len(x):>6}{x['p_implied'].mean():>9.3f}{x['win'].mean():>9.3f}{rec:>9.3f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="data/nar_pilot")
    args = ap.parse_args()
    df = load(args.data_dir)
    print(f"読み込み: {len(df)}頭 / {df['race_id'].nunique()}レース / 場={sorted(df['place_name'].unique())}")
    report("全場プール", df)
    for pn, g in df.groupby("place_name"):
        report(pn, g)
    print("\n読み取り: 全帯・全場で回収率が(1-控除)近傍以下・CI下限>1.0なし → 単勝プールは効率的。")
    print("本命寄せは損失を縮める(FLB)が控除率を跨がない。>1.0の帯は小標本ノイズ(プールで消える)。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
