"""③ laptime → pace 特徴を seed_featured に足す（leak-free・self-contained）。

【リーク注意】当該レースの pace（前半3F 等）は発走後にしか分からないので直接特徴にすると
未来のぞき見。ここは **各馬の過去走の pace を as-of 集計**（当該走は shift で除外）した form 特徴
だけを作る。既存 add_pace_stats は脚質を通過順から算出しており laptime を使っていないため、
laptime のレース pace は本スクリプトで初めて活用される。

作る特徴（いずれも過去走のみ・leak-free）:
  rel_agari_hist   : 「自分の上り − レース上がり3F」の過去平均（＝相対的な脚/closing の強さ）
  rel_agari_hist5  : 同・直近5走
  zenhan_hist      : 過去走が走ったレースの前半3F の平均（＝前傾/高速ペース経験）

本番パイプラインは無改変。seed 専用に seed_featured を拡張した別ファイルを出力し、
retrain --featured-path で比較する（市場効率確定済みなので AUC/較正が動くかの測定が目的）。

使い方:
    python add_pace_features.py \
        "/mnt/c/Users/Ayaka/Downloads/archive/19860105-20210731_race_result.csv" \
        "/mnt/c/Users/Ayaka/Downloads/archive/19860105-20210731_laptime.csv"
"""

from __future__ import annotations

import argparse
import os
import sys

import pandas as pd

C_RACE_ID = "レースID"
C_DATE = "レース日付"
C_UMABAN = "馬番"
C_AGARI = "上り"          # 馬ごとの上がり3F（results CSV）
LAP_ZENHAN = "前半3ハロン"
LAP_AGARI = "上がり3ハロン"  # レース全体の上がり3F（laptime CSV）


def _read_csv(path, cols=None):
    for enc in ("utf-8-sig", "utf-8", "cp932", "shift_jis"):
        try:
            return pd.read_csv(path, usecols=cols, encoding=enc, low_memory=False)
        except ValueError:
            return pd.read_csv(path, encoding=enc, low_memory=False)  # usecols 不一致
        except Exception:  # noqa: BLE001
            continue
    raise RuntimeError(f"読込失敗: {path}")


def run(args) -> int:
    for p in (args.results_csv, args.laptime_csv):
        if not os.path.isfile(p):
            print(f"[NG] 見つからない: {p}")
            return 2
    if not os.path.isfile(args.featured):
        print(f"[NG] seed featured が無い: {args.featured}（build_seed_featured.py）")
        return 2

    from seed_from_csv import build_synthetic_horse_id

    print("=" * 78)
    print("③ pace 特徴（過去走の相対脚・前半ペース）を leak-free で付与")
    print("=" * 78)

    import numpy as np

    res = _read_csv(args.results_csv)
    hid = build_synthetic_horse_id(res)  # seed_results と同一の合成 horse_id
    lap = _read_csv(args.laptime_csv)
    lap[C_RACE_ID] = lap[C_RACE_ID].astype("Int64").astype(str)
    # CSV の要約列(前半3ハロン/上がり3ハロン)は一部で1ハロン目の値になっており不正確。
    # 生ラップ(ラップタイム1..N)から正しく再計算する: 前半3F=先頭3本和、上がり3F=末尾3本(非null)和。
    lapcols = [c for c in lap.columns if str(c).startswith("ラップタイム")]
    L = lap[lapcols].apply(pd.to_numeric, errors="coerce")
    zenhan3 = L.iloc[:, :3].sum(axis=1, min_count=3)   # 先頭3本が揃うときのみ
    arr = L.to_numpy()

    def _last3(row):
        v = row[~np.isnan(row)]
        return float(v[-3:].sum()) if v.size >= 3 else np.nan

    agari3 = pd.Series([_last3(r) for r in arr], index=lap.index)
    print(f"生ラップから再計算: 前半3F mean={zenhan3.mean():.1f}s / 上がり3F mean={agari3.mean():.1f}s "
          f"（要約列バグ回避。~34-38s が正常）")
    lap_z = dict(zip(lap[C_RACE_ID], zenhan3))
    lap_a = dict(zip(lap[C_RACE_ID], agari3))

    d = pd.DataFrame({
        "race_id": res[C_RACE_ID].astype("Int64").astype(str),
        "馬番": pd.to_numeric(res[C_UMABAN], errors="coerce"),
        "hid": hid.astype("Int64"),
        "date": pd.to_datetime(res[C_DATE], errors="coerce"),
        "horse_agari": pd.to_numeric(res[C_AGARI], errors="coerce"),
    })
    d["race_zenhan"] = d["race_id"].map(lap_z)
    d["race_agari"] = d["race_id"].map(lap_a)
    # 相対脚: 自分の上り − レース上がり3F（負=レース平均より速く上がった＝強い脚）
    d["rel_agari"] = d["horse_agari"] - d["race_agari"]

    # as-of 集計: 馬ごと日付順に、当該走を shift で除外して過去走のみ集計（leak-free）
    d = d.sort_values(["hid", "date"], kind="stable")
    g = d.groupby("hid", sort=False)
    d["rel_agari_hist"] = g["rel_agari"].transform(lambda s: s.shift().expanding().mean())
    d["rel_agari_hist5"] = g["rel_agari"].transform(lambda s: s.shift().rolling(5, min_periods=1).mean())
    d["zenhan_hist"] = g["race_zenhan"].transform(lambda s: s.shift().expanding().mean())

    pace_cols = ["rel_agari_hist", "rel_agari_hist5", "zenhan_hist"]
    key = d[["race_id", "馬番"] + pace_cols].copy()
    key["馬番"] = key["馬番"].astype("Int64")

    featured = pd.read_pickle(args.featured)
    fidx_name = featured.index.name
    fr = featured.reset_index()
    fr["race_id"] = fr[fidx_name if fidx_name else "index"].astype(str) if fidx_name else fr["race_id"].astype(str)
    if "馬番" not in fr.columns:
        print("[NG] featured に 馬番 列が無く結合不可。")
        return 2
    fr["馬番"] = pd.to_numeric(fr["馬番"], errors="coerce").astype("Int64")
    before = fr.shape[1]
    fr = fr.merge(key, on=["race_id", "馬番"], how="left")
    merged = fr.set_index("race_id")
    if fidx_name and fidx_name != "race_id":
        merged.index.name = fidx_name

    for c in pace_cols:
        nn = merged[c].notna().mean() * 100
        print(f"  {c:<18} 非null {nn:5.1f}%  mean={merged[c].mean():.3f}")
    print(f"  列数 {before-1} → {merged.shape[1]}（+{len(pace_cols)}）")

    merged.to_pickle(args.out)
    print(f"\n書き込み: {args.out}")
    print("次: retrain --featured-path で seed35y_ho と AUC/較正を比較。")
    print("  python -m src.pipeline.run_pipeline retrain --featured-path %s \\" % args.out)
    print("    --version-name seed35y_pace --holdout-years 2020 2021")
    print("  python -m src.pipeline.run_pipeline backtest --version seed35y_pace \\")
    print("    --featured-path %s --edge-diagnostic --years 2020 2021" % args.out)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="③ laptime→pace 特徴を seed_featured に付与（leak-free）")
    ap.add_argument("results_csv", help="race_result CSV（馬ごとの上り用）")
    ap.add_argument("laptime_csv", help="laptime CSV（レース pace 用）")
    ap.add_argument("--featured", default="data/raw/seed_featured_data.pkl")
    ap.add_argument("--out", default="data/raw/seed_featured_pace.pkl")
    return run(ap.parse_args())


if __name__ == "__main__":
    sys.exit(main())
