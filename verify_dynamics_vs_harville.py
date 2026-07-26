"""展開/当日バイアス特徴が「Harville が外す joint 構造」を予測できるかの部分検証（連系ΔR²の前哨）。

連系の事前オッズは過去に無いので p_mkt は測れないが、p_harv(単勝由来Harville)と実際の joint 結果は
seed にある。ここでは **無料データ由来の展開/バイアス特徴が、Harville の複勝(top3)確率の残差
(actual_top3 − p_harv_place)を説明できるか** を測る。説明できれば「単勝が織り込まない joint 構造
(相関)を捉えている」証拠＝連系 ΔR² の芽。説明できなければ早期 NO-GO(展開も市場/Harville に含まれる)。

すべて leak-safe:
  - horse 脚質 = その馬の**過去走**の1コーナー相対位置の平均(当該走は shift 除外)
  - field ペース圧 = 出走各馬の過去脚質から作る前傾馬シェア(発走前確定の隊列)
  - pace_fit = 差し馬(高style) × 高ペース圧(共倒れ→差し有利)
  - 当日バイアス = 同開催・同芝ダの**先行レース**(発走時刻が前)の勝ち馬脚質(前有利の日か)
  - p_harv_place = 当該レースの単勝オッズ由来 Harville 複勝確率(発走前確定=単勝と同前提)

生 race_result CSV を直接読む（per-horse 1コーナー・着順・単勝・日付・場が必要。seed_results は
コーナーを持たないため）。合成 horse_id は seed_from_csv と同一ロジックで再計算。

使い方:
    python verify_dynamics_vs_harville.py "/mnt/c/Users/Ayaka/Downloads/archive/19860105-20210731_race_result.csv"
"""

from __future__ import annotations

import argparse
import os
import sys

import numpy as np
import pandas as pd


def _ols_r2(X: np.ndarray, y: np.ndarray):
    """定数項つき OLS の R² と係数を返す（sklearn 非依存）。"""
    A = np.column_stack([np.ones(len(X)), X])
    beta, *_ = np.linalg.lstsq(A, y, rcond=None)
    pred = A @ beta
    ss_res = float(((y - pred) ** 2).sum())
    ss_tot = float(((y - y.mean()) ** 2).sum())
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
    return r2, beta[1:]


def _read_csv(path):
    for enc in ("utf-8-sig", "utf-8", "cp932", "shift_jis"):
        try:
            return pd.read_csv(path, encoding=enc, low_memory=False)
        except Exception:  # noqa: BLE001
            continue
    raise RuntimeError("CSV 読込失敗。")


def run(args) -> int:
    if not os.path.isfile(args.csv):
        print(f"[NG] 見つからない: {args.csv}")
        return 2
    from seed_from_csv import build_synthetic_horse_id
    from src.preprocessing._place_prob import implied_from_odds, prob_place

    print("=" * 78)
    print("展開/当日バイアス vs Harville 複勝残差（連系ΔR²の前哨・leak-safe）")
    print("=" * 78)

    csv = _read_csv(args.csv)
    d = pd.DataFrame()
    d["race_id"] = csv["レースID"].astype("Int64").astype(str)
    d["date"] = pd.to_datetime(csv["レース日付"], errors="coerce")
    d["開催"] = pd.to_numeric(csv["競馬場コード"], errors="coerce")
    d["race_type"] = csv.get("芝・ダート区分")
    d["time"] = csv.get("発走時刻")
    d["uma"] = pd.to_numeric(csv["馬番"], errors="coerce")
    d["rank"] = pd.to_numeric(csv["着順"], errors="coerce")
    d["tan"] = pd.to_numeric(csv["単勝"], errors="coerce")
    d["c1"] = pd.to_numeric(csv.get("1コーナー"), errors="coerce")
    d["nh"] = d.groupby("race_id")["uma"].transform("count")
    d["hid"] = build_synthetic_horse_id(csv).astype("Int64").astype(str)

    # --- 脚質(style): 過去走の 1コーナー相対位置 の平均（0=前, 1=後）。leak-safe(shift) ---
    d["style_raw"] = (d["c1"] / d["nh"]).clip(0, 1)
    d = d.sort_values(["hid", "date"], kind="stable")
    g = d.groupby("hid", sort=False)["style_raw"]
    d["style_hist"] = g.transform(lambda s: s.shift().expanding().mean())

    # --- field ペース圧: 出走各馬の過去脚質から前傾(style<0.35)シェア（発走前確定の隊列）---
    d["_front"] = (d["style_hist"] < 0.35).astype(float)
    d["field_front_share"] = d.groupby("race_id")["_front"].transform("mean")
    # pace_fit: 差し馬(高style) × 高ペース圧 → 共倒れで差し有利
    d["pace_fit"] = d["style_hist"].fillna(0.5) * d["field_front_share"]

    # --- 当日トラックバイアス: 同開催・同芝ダの先行レース(発走時刻前)の勝ち馬 style 平均 ---
    d["_win_style"] = np.where(d["rank"] == 1, d["style_hist"], np.nan)
    bias = {}
    if "開催" in d.columns and "time" in d.columns:
        d["_t"] = d["time"].astype(str)
        for (day, kai, rtype), grp in d.groupby([d["date"].dt.date, "開催", d.get("race_type", "")]):
            races = grp.groupby("race_id").agg(t=("_t", "first"),
                                               wstyle=("_win_style", "max")).sort_values("t")
            cum = races["wstyle"].shift().expanding().mean()  # 先行レースの勝ち馬 style 平均
            for rid, v in cum.items():
                bias[rid] = v
    d["track_bias"] = d["race_id"].map(bias)  # 高=後方有利の日 / 低=前有利の日

    # --- p_harv_place: 単勝由来 Harville 複勝確率 ---
    php = np.full(len(d), np.nan)
    idx = {rid: sub for rid, sub in d.groupby("race_id")}
    pos = 0
    ph_map = {}
    for rid, sub in idx.items():
        wo = {int(u): float(o) for u, o in zip(sub["uma"], sub["tan"]) if o and o > 0}
        if len(wo) < 3:
            continue
        wp = implied_from_odds(wo, normalized=True)
        for u in wo:
            ph_map[(rid, u)] = prob_place(wp, u, 3)
    d["p_harv_place"] = [ph_map.get((r, int(u)) if pd.notna(u) else None, np.nan)
                         for r, u in zip(d["race_id"], d["uma"])]
    d["top3"] = (d["rank"] <= 3).astype(float)
    d["resid"] = d["top3"] - d["p_harv_place"]

    feats = ["style_hist", "field_front_share", "pace_fit", "track_bias"]
    print("\n特徴カバレッジ（非null率）:")
    for f in feats + ["resid"]:
        print(f"  {f:<20} {d[f].notna().mean()*100:5.1f}%")
    # core（脚質・ペース圧・残差）が揃う行で評価。track_bias は疎（各日各場の初戦は無い）ので平均補完。
    core = ["resid", "style_hist", "field_front_share"]
    use = d.dropna(subset=core).copy()
    use["track_bias"] = use["track_bias"].fillna(use["track_bias"].mean())
    use["pace_fit"] = use["pace_fit"].fillna(use["pace_fit"].mean())
    print(f"\n有効サンプル={len(use):,} 行（{use['race_id'].nunique():,} レース）")
    if len(use) < 1000:
        print("[NG] サンプル不足（実データでは十分になる。フィクスチャは小さい）。")
        return 2

    # 各特徴の残差との相関
    print("\n各特徴 vs Harville複勝残差 の相関（|r| が大きいほど joint 構造を捉える）:")
    for f in feats:
        r = np.corrcoef(use[f], use["resid"])[0, 1]
        print(f"  {f:<20} corr={r:+.4f}")

    # 多変量 OLS の R²（残差を説明できるか）
    X = use[feats].to_numpy()
    r2, coefs = _ols_r2(X, use["resid"].to_numpy())
    print(f"\n多変量 OLS: 残差 R² = {r2:.5f}")
    print("  係数:", {f: round(float(c), 4) for f, c in zip(feats, coefs)})

    print("\n" + "=" * 78)
    print("判定（連系ΔR²の GO/NO-GO 前哨）:")
    print("  ・R² が有意に正（>0.001 級）＝展開/バイアスが Harville の外す joint を捉える＝連系で芽あり。")
    print("    → 連系事前オッズ蓄積(KEIBA_ODDS_CAPTURE_EXOTIC)後に p_mkt 版 ΔR² へ進む価値。")
    print("  ・R² ≈ 0＝展開も市場/Harville に含まれ独立情報なし＝早期 NO-GO。")
    print("  注: これは in-sample の説明力。正なら OOS/年別でも確認する。")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="展開/バイアス vs Harville 複勝残差（連系ΔR²前哨）")
    ap.add_argument("csv", help="生 race_result CSV（per-horse 1コーナー・着順・単勝が必要）")
    return run(ap.parse_args())


if __name__ == "__main__":
    sys.exit(main())
