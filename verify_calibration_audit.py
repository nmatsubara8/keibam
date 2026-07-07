"""較正監査: モデルの ECE を全体＋レジーム別に測り、再較正で下がるか(cross-fit)を検証する。

市場効率下では利益は出ない(全賭け -EV)が、**確率の較正が良いほどケリー配分が正しくなる**。
本監査は Win ヘッドの勝率 r̂ の較正を、市場 p_mkt と比較しつつ:
  1. 全体 ECE(モデル vs 市場) + 勝ち馬 logloss
  2. レジーム別 ECE(頭数帯 / レース種別 / 距離帯 / 人気帯) — 局所的な mis-calibration の発見
  3. 再較正の効果(cross-fit isotonic): 学習した isotonic を out-of-fold に当てて ECE が下がるか
既存の run_edge_diagnostic / calibration_error / IsotonicCalibrator を再利用。leak 回避のため
再較正は race 単位 2-fold cross-fit(片方で fit→他方で評価)。

使い方(本番モデルでも seed モデルでも):
    python verify_calibration_audit.py --version seed35y_ho --featured data/raw/seed_featured_pace.pkl --years 2020 2021
    python verify_calibration_audit.py --version <本番版>   # 既定 featured=FEATURED_DATA_PATH
"""

from __future__ import annotations

import argparse
import glob
import os
import sys

import numpy as np
import pandas as pd


def _load_model(version, model_path):
    from app._data_loader import load_model_from_path, load_win_head_for

    if model_path:
        return load_model_from_path(model_path), load_win_head_for(model_path), model_path
    heads = [p for p in glob.glob(os.path.join("models", "*", "*.pickle"))
             if "__" not in os.path.basename(p)
             and not os.path.basename(p).startswith(("basemodel", "selected"))]
    exact = [p for p in heads if os.path.basename(p) in (f"{version}.pickle", f"{version}_keibam.pickle")]
    match = exact or [p for p in heads if version and version in os.path.basename(p)]
    if match:
        p = sorted(match)[-1]
        return load_model_from_path(p), load_win_head_for(p), p
    print("[NG] モデル未検出。利用可能:")
    for p in sorted(heads):
        print(f"    --version {os.path.basename(p).replace('.pickle','')}")
    return None, None, None


def _ece(prob, won, n_bins=10):
    from src.policies._calibration import calibration_error
    return calibration_error(np.asarray(prob), np.asarray(won), n_bins=n_bins)


def _crossfit_recal_ece(edge_df, n_bins=10):
    """race 2-fold cross-fit で isotonic 再較正 → out-of-fold の ECE を返す(生 vs 再較正)。"""
    from src.policies._calibration import fit_isotonic_calibrator

    rids = edge_df.index.astype(str)
    uniq = pd.Index(sorted(rids.unique()))
    fold = pd.Series(np.arange(len(uniq)) % 2, index=uniq)
    f = rids.map(fold).to_numpy()
    rh = edge_df["r_hat"].to_numpy()
    wn = edge_df["won"].to_numpy()
    recal = np.full(len(edge_df), np.nan)
    for k in (0, 1):
        tr, te = f != k, f == k
        if tr.sum() == 0 or te.sum() == 0:
            continue
        cal = fit_isotonic_calibrator(rh[tr], wn[tr])
        # cal.predict は要素ごと単調写像で順序を保つ（calibrate_within_race は groupby で並びを
        # 変え代入がズレて ECE が壊れる。ECE 測定に within-race 正規化は不要）。
        recal[te] = cal.predict(rh[te])
    ok = ~np.isnan(recal)
    ece_raw = _ece(edge_df["r_hat"].to_numpy()[ok], edge_df["won"].to_numpy()[ok], n_bins)
    ece_recal = _ece(recal[ok], edge_df["won"].to_numpy()[ok], n_bins)
    return ece_raw, ece_recal


def run(args) -> int:
    from src.constants._local_paths import LocalPaths

    featured_path = args.featured or LocalPaths.FEATURED_DATA_PATH
    if not os.path.isfile(featured_path):
        print(f"[NG] featured が無い: {featured_path}")
        return 2
    place_ai, win_ai, mpath = _load_model(args.version, args.model_path)
    if place_ai is None:
        return 2
    ai = win_ai or place_ai  # KeibaAI ラッパ。calc_score が feature_names_ で列を自動整合する
    print("=" * 80)
    print(f"較正監査  model={os.path.basename(mpath)}  ヘッド={'Win' if win_ai else 'Place'}")
    print("=" * 80)

    featured = pd.read_pickle(featured_path)
    rid = featured.index.astype(str)
    if args.years:
        featured = featured[rid.str[:4].isin({str(y) for y in args.years})]
    print(f"評価: {len(featured):,} 行 / {featured.index.nunique():,} レース")

    # KeibaAI.calc_score が学習列(feature_names_)へ自動整合するため、featured の列数差があっても動く
    # （effective_model を直接使う run_edge_diagnostic は整合しないので calc_score 経由にする）。
    from src.policies._score_policy import ExpectedValueScorePolicy
    from src.simulation._edge_diagnostic import _actual_win, build_edge_frame

    table = ai.calc_score(featured, ExpectedValueScorePolicy)
    won = _actual_win(featured)
    edge = build_edge_frame(table, won.to_numpy()).dropna(subset=["r_hat", "p_mkt", "won"])

    # 1. 全体 ECE(モデル vs 市場)
    ece_m = _ece(edge["r_hat"], edge["won"])
    ece_mkt = _ece(edge["p_mkt"], edge["won"])
    print("\n【全体】ECE（低いほど較正が良い。ケリー配分の正しさに直結）")
    print(f"  モデル r̂  ECE = {ece_m:.4f}")
    print(f"  市場 p_mkt ECE = {ece_mkt:.4f}")
    print(f"  → {'モデルが市場より良い' if ece_m < ece_mkt else '市場と同等/劣る'}（差 {ece_m-ece_mkt:+.4f}）")

    # 2. レジーム別 ECE（局所 mis-calibration の発見）。頭数は edge から直接カウント（merge 不要で堅牢）。
    edge = edge.copy()
    edge["n_horses"] = edge.groupby(level=0)["r_hat"].transform("size")

    def _regime_report(label, series):
        print(f"\n【{label}別 ECE】(件数≥2000 のみ、モデル/市場)")
        g = pd.DataFrame({"r_hat": edge["r_hat"].to_numpy(), "p_mkt": edge["p_mkt"].to_numpy(),
                          "won": edge["won"].to_numpy(), "grp": series.to_numpy()})
        for name, sub in g.dropna(subset=["grp"]).groupby("grp"):
            if len(sub) < 2000:
                continue
            em = _ece(sub["r_hat"], sub["won"], 5)
            ek = _ece(sub["p_mkt"], sub["won"], 5)
            flag = "  ← モデル劣化" if em > ek + 0.003 else ""
            print(f"  {str(name):<14} n={len(sub):>7,}  モデルECE={em:.4f}  市場ECE={ek:.4f}{flag}")

    if "n_horses" in edge.columns:
        band = pd.cut(pd.to_numeric(edge["n_horses"], errors="coerce"),
                      [0, 8, 12, 14, 16, 99], labels=["~8", "9-12", "13-14", "15-16", "17+"])
        _regime_report("頭数", band)
    _regime_report("人気帯", pd.cut(edge["p_mkt"], [0, .02, .05, .1, .2, 1],
                                   labels=["穴", "中穴", "中", "上位", "本命"]))

    # 3. 再較正の効果（cross-fit）
    ece_raw, ece_recal = _crossfit_recal_ece(edge)
    print("\n【再較正（cross-fit isotonic）】out-of-fold ECE")
    print(f"  生 r̂       ECE = {ece_raw:.4f}")
    print(f"  再較正後   ECE = {ece_recal:.4f}  （改善 {ece_raw-ece_recal:+.4f}）")
    print("  → 改善が +0.002 以上なら再較正でケリー配分の信頼性が上がる。ほぼ0なら既に十分較正済み。")

    print("\n" + "=" * 80)
    print("解釈: 市場効率下で利益化は不可(全賭け -EV)。本監査は『確率の信頼性=ケリー配分の正しさ』を測る。")
    print("局所 mis-calibration や cross-fit の改善が明確なら、そのレジームに isotonic を適用する価値あり。")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="較正監査（ECE 全体/レジーム別 + 再較正効果）")
    ap.add_argument("--version", default=None, help="モデル版（部分一致）")
    ap.add_argument("--model-path", default=None, help="モデル pickle を直接指定")
    ap.add_argument("--featured", default=None, help="featured（既定 FEATURED_DATA_PATH）")
    ap.add_argument("--years", type=int, nargs="+", default=None, help="評価年（OOS。学習除外年）")
    return run(ap.parse_args())


if __name__ == "__main__":
    sys.exit(main())
