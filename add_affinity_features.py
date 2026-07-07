"""エンティティ×コンテキストの「相性」を網羅的に掃討する（leak-free target-encoding sweep）。

既存 featured は一部の相性（馬×距離/コース/馬場、騎手/調教師×種別・開催 等）を既にカバー済み。
本スクリプトは **未カバーの相性**（騎手×競馬場/馬場/距離/天候、調教師×競馬場/馬場/距離、
馬主×種別/場、馬性×距離/種別、馬×天候、騎手×馬コンビ）を、既存の検証済み基盤
`build_person_form_features`（厳密過去・スムージング済み＝leak-free）で一括生成する。

「相性」の正しい系統ツールは gplearn(数値)でなく、この entity×context target-encoding の
spec を増やすこと。DEFAULT_PERSON_SPECS の拡張版。本番無改変、seed 専用に featured 拡張。

前提（測定して確認）: 市場効率確定(echo0.989)。相性も市場が織り込む公算大。判定は AUC でなく
ΔR²/echo/logloss vs 市場（--edge-diagnostic）。±0.003 の logloss 揺れはノイズ（pace で実証済）。

使い方:
    python add_affinity_features.py       # seed_results/seed_race_info + seed_featured_pace を拡張
"""

from __future__ import annotations

import argparse
import os
import sys

import numpy as np
import pandas as pd

# 未カバーの相性 grid（entity × context）。既存(by_type/開催/at_distance 等)と重複しないもの。
AFFINITY_SPECS: list[dict] = [
    {"name": "jockey_win_te_by_place",   "keys": ["jockey_id", "開催"],          "target": "_win"},
    {"name": "jockey_win_te_by_ground",  "keys": ["jockey_id", "ground_state1"], "target": "_win"},
    {"name": "jockey_place_te_by_dist",  "keys": ["jockey_id", "dist_band"],     "target": "_place"},
    {"name": "jockey_win_te_by_weather", "keys": ["jockey_id", "weather"],       "target": "_win"},
    {"name": "trainer_win_te_by_place",  "keys": ["trainer_id", "開催"],         "target": "_win"},
    {"name": "trainer_place_te_by_ground", "keys": ["trainer_id", "ground_state1"], "target": "_place"},
    {"name": "trainer_win_te_by_dist",   "keys": ["trainer_id", "dist_band"],    "target": "_win"},
    {"name": "owner_win_te_by_type",     "keys": ["owner_id", "race_type"],      "target": "_win"},
    {"name": "owner_win_te_by_place",    "keys": ["owner_id", "開催"],           "target": "_win"},
    {"name": "horse_place_te_by_weather", "keys": ["horse_id", "weather"],       "target": "_place"},
    {"name": "sex_win_te_by_dist",       "keys": ["性", "dist_band"],            "target": "_win"},
    {"name": "sex_place_te_by_type",     "keys": ["性", "race_type"],            "target": "_place"},
    {"name": "combo_jh_place",           "keys": ["jockey_id", "horse_id"],      "target": "_place"},
    {"name": "combo_jh_win",             "keys": ["jockey_id", "horse_id"],      "target": "_win"},
]


def _build_context_frame(results_pkl: str, race_info_pkl: str) -> pd.DataFrame:
    """seed_results に race_info の context を結合し、相性 sweep 用の (race,horse) 粒度フレームを作る。"""
    res = pd.read_pickle(results_pkl)
    if res.index.name == "race_id":
        res = res.reset_index()
    res["race_id"] = res["race_id"].astype(str)
    ri = pd.read_pickle(race_info_pkl)
    if ri.index.name == "race_id":
        ri = ri.reset_index()
    ri["race_id"] = ri["race_id"].astype(str)

    ctx_cols = ["race_id"]
    for c in ("race_type", "place_id", "開催", "ground_state1", "weather", "course_len", "date"):
        if c in ri.columns and c not in ctx_cols:
            ctx_cols.append(c)
    ri = ri[ctx_cols].copy()
    if "開催" not in ri.columns and "place_id" in ri.columns:
        ri = ri.rename(columns={"place_id": "開催"})

    df = res.merge(ri, on="race_id", how="left")
    # date を datetime へ（seed_race_info は "1986年06月07日" 文字列）
    df["date"] = pd.to_datetime(df.get("date"), format="%Y年%m月%d日", errors="coerce")
    if df["date"].isna().all():
        df["date"] = pd.to_datetime(df.get("date"), errors="coerce")
    # 距離帯（400m バケット）と 性（性齢の先頭文字）
    cl = pd.to_numeric(df.get("course_len"), errors="coerce")
    df["dist_band"] = (cl // 400).astype("Int64")
    if "性齢" in df.columns:
        df["性"] = df["性齢"].astype(str).str[0]
    return df


def run(args) -> int:
    for p in (args.results, args.race_info, args.featured):
        if not os.path.isfile(p):
            print(f"[NG] 見つからない: {p}")
            return 2

    from src.preprocessing._target_encoding import build_person_form_features

    print("=" * 78)
    print("相性(entity×context) target-encoding sweep（leak-free）")
    print("=" * 78)
    df = _build_context_frame(args.results, args.race_info)
    have = [s for s in AFFINITY_SPECS
            if all(k in df.columns for k in s["keys"])]
    skip = [s["name"] for s in AFFINITY_SPECS if s not in have]
    print(f"生成対象 {len(have)} 相性 / skip(列不足) {skip}")

    feats = build_person_form_features(
        df, specs=have, date_col="date", rank_col="着順", alpha=args.alpha
    )
    feats["race_id"] = df["race_id"].to_numpy()
    feats["馬番"] = pd.to_numeric(df["馬番"], errors="coerce").astype("Int64").to_numpy()

    featured = pd.read_pickle(args.featured)
    fidx = featured.index.name or "race_id"
    fr = featured.reset_index()
    fr["race_id"] = fr[fidx].astype(str)
    fr["馬番"] = pd.to_numeric(fr["馬番"], errors="coerce").astype("Int64")
    names = [s["name"] for s in have]
    before = featured.shape[1]
    fr = fr.merge(feats[["race_id", "馬番"] + names], on=["race_id", "馬番"], how="left")
    merged = fr.set_index("race_id")
    merged.index.name = fidx

    print("\n生成した相性特徴（非null率・mean）:")
    for n in names:
        print(f"  {n:<26} 非null {merged[n].notna().mean()*100:5.1f}%  mean={merged[n].mean():.4f}")
    print(f"  列数 {before} → {merged.shape[1]}（+{len(names)}）")

    merged.to_pickle(args.out)
    print(f"\n書き込み: {args.out}")
    print("次（判定は ΔR²/echo/logloss vs 市場。±0.003 はノイズ）:")
    print(f"  python -m src.pipeline.run_pipeline retrain --featured-path {args.out} \\")
    print("    --version-name seed35y_aff --holdout-years 2020 2021")
    print(f"  python -m src.pipeline.run_pipeline backtest --version seed35y_aff \\")
    print(f"    --featured-path {args.out} --edge-diagnostic --years 2020 2021")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="相性 entity×context target-encoding sweep")
    ap.add_argument("--results", default="data/raw/seed_results.pkl")
    ap.add_argument("--race-info", default="data/raw/seed_race_info.pkl")
    ap.add_argument("--featured", default="data/raw/seed_featured_pace.pkl",
                    help="拡張元 featured（既定は pace 版。合成 comp 版でも可）")
    ap.add_argument("--out", default="data/raw/seed_featured_aff.pkl")
    ap.add_argument("--alpha", type=float, default=20.0, help="スムージング強度（低カード相性の過信抑制）")
    return run(ap.parse_args())


if __name__ == "__main__":
    sys.exit(main())
