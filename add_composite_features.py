"""ドメイン知識ベースの非線形合成特徴を seed_featured に足す（leak-safe・self-contained）。

提案(①斤量比/パワー ②距離変化ショック ③バイアス逆行 ほか)を、**発走前確定値＋過去走集計**
（いずれも leak-free）から合成する。当該走のタイム/上がり等（発走後にしか分からない値）は使わない。

重要な前提（測定して確認する用）:
  - 市場効率は確定済み(単勝35年 echo0.989)。これらの効果は市場が織り込み済みの公算大。
  - LightGBM は木分割で比/交互作用を既に近似するため、明示的合成の上乗せは限定的。
  - 正しい判定は AUC でなく **ΔR²/echo/logloss vs 市場**（--edge-diagnostic）。

本番パイプライン無改変。seed 専用に featured を拡張した別ファイルを出力し retrain --featured-path で比較。
各合成は必要列が無ければ自動 skip し、作れた/skip を報告する（列名差に堅牢）。

使い方:
    python add_composite_features.py            # data/raw/seed_featured_data.pkl を拡張
    python add_composite_features.py --featured <p> --out <p>
"""

from __future__ import annotations

import argparse
import os
import sys

import numpy as np
import pandas as pd


def _col(df: pd.DataFrame, *names):
    """候補名のうち最初に存在する列の Series を返す（無ければ None）。"""
    for n in names:
        if n in df.columns:
            return pd.to_numeric(df[n], errors="coerce")
    return None


def _first_present(df, names):
    for n in names:
        if n in df.columns:
            return n
    return None


def build_composites(df: pd.DataFrame) -> dict:
    """存在する列から leak-safe な非線形合成を作る。{name: Series} を返す。"""
    out: dict = {}
    kinryo = _col(df, "斤量")
    weight = _col(df, "体重", "馬体重")
    agari5 = _col(df, "上り_mean_5R", "上り_median_5R")
    course = _col(df, "course_len")
    interval = _col(df, "interval")
    elo = _col(df, "elo_rating")
    win5 = _col(df, "win_rate_5R")
    win20 = _col(df, "win_rate_20R")
    rel_agari = _col(df, "rel_agari_hist", "rel_agari_hist5")  # add_pace_features 由来（あれば）
    pace_med = _col(df, "pace_median")
    prize5 = _col(df, "賞金_mean_5R", "賞金_median_5R")

    def add(name, series, need):
        if all(s is not None for s in need) and series is not None:
            out[name] = series.replace([np.inf, -np.inf], np.nan)

    # ① 斤量比（発走前確定・比）: 背負う負荷の客観値。木でも比は近似だが明示化して検証。
    if kinryo is not None and weight is not None:
        add("comp_kinryo_ratio", kinryo / weight.replace(0, np.nan), [kinryo, weight])
        # パワー×脚: 馬格に対し斤量が軽く、過去に速い脚 → パワー局面のブレイク
        add("comp_power_speed", (weight / kinryo.replace(0, np.nan)) * (40.0 - agari5)
            if agari5 is not None else None, [weight, kinryo, agari5])
        # 斤量負荷×距離: 長距離ほど斤量が効く
        add("comp_kinryo_dist", (kinryo / weight.replace(0, np.nan)) * course
            if course is not None else None, [kinryo, weight, course])

    # ② 距離×休み明け（発走前確定の交互作用）: 距離延長×間隔
    if course is not None and interval is not None:
        add("comp_dist_layoff", course * np.log1p(interval.clip(lower=0)), [course, interval])

    # ③ バイアス逆行×脚質（過去走集計・leak-free）: 相対脚 × 脚質（後方から強い末脚＝負けて強し）
    if rel_agari is not None and pace_med is not None:
        add("comp_closing_bias", (-rel_agari) * pace_med, [rel_agari, pace_med])
        # 相対脚 × 距離: 距離が延びるほど末脚の価値
        if course is not None:
            add("comp_closing_dist", (-rel_agari) * course, [rel_agari, course])

    # フォームの勢い（近走 − 中期）: 上昇馬の非線形強調
    if win5 is not None and win20 is not None:
        add("comp_form_momentum", win5 - win20, [win5, win20])

    # 実力×クラス経験: elo × 過去賞金（賞金＝走ってきたクラスの代理）
    if elo is not None and prize5 is not None:
        add("comp_ability_class", elo * np.log1p(prize5.clip(lower=0)), [elo, prize5])

    return out


def run(args) -> int:
    if not os.path.isfile(args.featured):
        print(f"[NG] featured が無い: {args.featured}")
        return 2
    print("=" * 78)
    print("ドメイン非線形合成特徴を付与（leak-safe）")
    print("=" * 78)
    df = pd.read_pickle(args.featured)
    comps = build_composites(df)
    if not comps:
        print("[NG] 合成に必要な列が見つからず 0 件。featured の列名を確認。")
        return 2

    before = df.shape[1]
    for name, s in comps.items():
        df[name] = s.to_numpy()
    for name in comps:
        nn = df[name].notna().mean() * 100
        print(f"  {name:<22} 非null {nn:5.1f}%  mean={df[name].mean():.4g}")
    # 参考: 作られなかった候補（列不足）を示す
    tried = {"comp_kinryo_ratio", "comp_power_speed", "comp_kinryo_dist", "comp_dist_layoff",
             "comp_closing_bias", "comp_closing_dist", "comp_form_momentum", "comp_ability_class"}
    skipped = sorted(tried - set(comps))
    if skipped:
        print(f"  skip（列不足）: {skipped}")
    print(f"  列数 {before} → {df.shape[1]}（+{len(comps)}）")

    df.to_pickle(args.out)
    print(f"\n書き込み: {args.out}")
    print("次: retrain --featured-path で比較（判定は AUC でなく ΔR²/echo/logloss vs 市場）:")
    print(f"  python -m src.pipeline.run_pipeline retrain --featured-path {args.out} \\")
    print("    --version-name seed35y_comp --holdout-years 2020 2021")
    print(f"  python -m src.pipeline.run_pipeline backtest --version seed35y_comp \\")
    print(f"    --featured-path {args.out} --edge-diagnostic --years 2020 2021")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="ドメイン非線形合成特徴を seed_featured に付与")
    ap.add_argument("--featured", default="data/raw/seed_featured_data.pkl")
    ap.add_argument("--out", default="data/raw/seed_featured_comp.pkl")
    return run(ap.parse_args())


if __name__ == "__main__":
    sys.exit(main())
