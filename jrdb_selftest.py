"""JRDB 自己完結 VOI テスト（featured 不要）— KYI/SED だけで「JRDBは市場を超えるか」を測る。

データ: KYI（前日・IDM/基準オッズ/各種指数）× SED（確定単勝オッズ＝市場・着順＝結果）を
(race_id, 馬番) で結合。**市場アンカーの帰無を最も厳しい形**で置く: 市場 q = 確定単勝オッズ
（最終・最も効率的な市場推定）。この q を JRDB の前日情報（IDM 等の非オッズ指数）が
上回れるかを、市場アンカー残差ヘッド＋事前定義判定（ΔNLL/ΔKL/2軸placebo）で検証する。

リーク遮断: レース日（SED ymd）で時系列 OOS 分割（前半で残差ヘッド fit・後半で評価）。
特徴量は前日 KYI のみ（確定オッズは市場 q としてのみ使用・特徴量に入れない）。

実行:
    python jrdb_selftest.py --jrdb-dir /tmp/jrdb_2025
    python jrdb_selftest.py --jrdb-dir /tmp/jrdb_2025 --features idm     # IDMのみ
    python jrdb_selftest.py --jrdb-dir /tmp/jrdb_2025 --features all     # 全KYI指数
"""
from __future__ import annotations

import argparse
import glob

import numpy as np
import pandas as pd

from src.jrdb._augment import KYI_FEATURE_MAP, _HMS
from src.jrdb._parser import parse
from src.policies._market_residual import market_probs, true_probs
from src.simulation._model_compare import calibration_by_odds_band, compare_models, race_nll
from src.training._residual_head import fit_residual_head, predict_residual

# 非オッズの JRDB 指数（市場と直交し得る。kijun_odds/kijun_fukuodds は市場近似なので既定除外）
IDX_FEATURES = ["jrdb_idm", "jrdb_kishu_idx", "jrdb_joho_idx", "jrdb_sougou_idx",
                "jrdb_ninki_idx", "jrdb_chokyo_idx", "jrdb_kyusha_idx", "jrdb_gekiso_idx",
                "jrdb_ten_idx", "jrdb_pace_idx", "jrdb_agari_idx", "jrdb_ichi_idx",
                "jrdb_pace_hms", "jrdb_kishu_tansho", "jrdb_kishu_3nai", "jrdb_start_idx",
                "jrdb_deokure_rate", "jrdb_manken_idx"]


def load_join(jrdb_dir: str, with_skb: bool = True) -> pd.DataFrame:
    """KYI×SED を結合し、per-horse フレーム（index=race_id・着順/単勝/馬番＋jrdb_指数）を返す。

    with_skb=True なら SKB から前走トラブル（jrdb_prev_trouble/jrdb_prev_deokure）を
    ketto×日付の merge_asof(backward・exact不可) で貼る（卍核＝直交情報の本命）。
    """
    kyi = pd.concat([parse(f, "KYI") for f in sorted(glob.glob(f"{jrdb_dir}/KYI*.txt"))],
                    ignore_index=True)
    sed = pd.concat([parse(f, "SED") for f in sorted(glob.glob(f"{jrdb_dir}/SED*.txt"))],
                    ignore_index=True)
    kcols = ["race_id", "umaban", "ketto", "pace_yosou", *KYI_FEATURE_MAP.keys()]
    k = kyi[[c for c in kcols if c in kyi.columns]].rename(columns=KYI_FEATURE_MAP)
    k["jrdb_pace_hms"] = kyi["pace_yosou"].str.strip().map(_HMS)
    s = sed[["race_id", "umaban", "ymd", "kakutei_tansho", "chakujun"]]
    m = k.merge(s, on=["race_id", "umaban"], how="inner")
    m = m.dropna(subset=["kakutei_tansho", "chakujun"])
    m = m[m["kakutei_tansho"] > 1.0].copy()
    m["着順"] = m["chakujun"]
    m["単勝"] = m["kakutei_tansho"]
    m["馬番"] = m["umaban"]

    if with_skb and "ketto" in m.columns:
        from src.jrdb._augment import build_history
        hist = build_history([], sorted(glob.glob(f"{jrdb_dir}/SKB*.txt")))
        if not hist.empty:
            today = pd.to_datetime(m["ymd"], format="%Y%m%d", errors="coerce")
            left = pd.DataFrame({"race_id": m["race_id"].to_numpy(),
                                 "ketto": m["ketto"].to_numpy(),
                                 "_today": today.to_numpy()}).dropna(subset=["ketto", "_today"])
            left = left.sort_values("_today")
            asof = pd.merge_asof(left, hist.sort_values("hist_date"), by="ketto",
                                 left_on="_today", right_on="hist_date",
                                 direction="backward", allow_exact_matches=False)
            asof = asof.rename(columns={"prev_trouble": "jrdb_prev_trouble",
                                        "prev_deokure": "jrdb_prev_deokure"})
            m = m.merge(
                asof[["race_id", "ketto", "jrdb_prev_trouble", "jrdb_prev_deokure"]]
                .drop_duplicates(["race_id", "ketto"]),
                on=["race_id", "ketto"], how="left")
    return m.set_index("race_id")


def build_races(df: pd.DataFrame) -> list[dict]:
    """per-horse フレーム → レース辞書（odds/winner/year=ymd）。控除ガード付き。"""
    races = []
    for rid, g in df.groupby(df.index.astype(str)):
        gg = g.dropna(subset=["馬番", "単勝"])
        gg = gg[gg["単勝"] > 1.0]
        if len(gg) < 5 or float((1.0 / gg["単勝"]).sum()) < 1.02:  # 疑似裁定ガード
            continue
        omap = {int(u): float(o) for u, o in zip(gg["馬番"], gg["単勝"], strict=False)}
        winner = next((int(u) for u, c in zip(gg["馬番"], gg["着順"], strict=False)
                       if pd.notna(c) and int(c) == 1), None)
        if winner is None:
            continue
        ymd = str(g["ymd"].iloc[0])
        races.append({"race_id": str(rid), "ymd": ymd, "odds": omap, "winner": winner})
    return races


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--jrdb-dir", default="/tmp/jrdb_2025")
    ap.add_argument("--features", choices=("idm", "all", "trouble"), default="all",
                    help="idm=IDMのみ / all=KYI全指数 / trouble=前走トラブル(SKB)のみ")
    ap.add_argument("--train-frac", type=float, default=0.6)
    args = ap.parse_args()

    df = load_join(args.jrdb_dir, with_skb=True)
    races = build_races(df)
    races.sort(key=lambda r: r["ymd"])
    print(f"結合: {len(df):,}行 / 有効レース {len(races):,}"
          f"（{races[0]['ymd']}–{races[-1]['ymd']}）")

    trouble_cols = [c for c in ("jrdb_prev_trouble", "jrdb_prev_deokure")
                    if c in df.columns and df[c].notna().any()]
    if args.features == "idm":
        fcols = ["jrdb_idm"]
    elif args.features == "trouble":
        fcols = trouble_cols
    else:
        fcols = [c for c in IDX_FEATURES if c in df.columns and df[c].notna().any()] + trouble_cols
    if trouble_cols:
        cov = {c: f"{df[c].notna().mean():.0%}非欠損/{df[c].fillna(0).mean():.1%}陽性"
               for c in trouble_cols}
        print(f"前走トラブル(SKB): {cov}")
    print(f"特徴量（前日KYI＋前走トラブル・確定オッズは市場q）: {len(fcols)}列")
    if not fcols:
        raise SystemExit("特徴量が空です")

    # 時系列 OOS 分割（レース日順・前半 fit / 後半 評価）
    n_tr = int(len(races) * args.train_frac)
    train_rids = {r["race_id"] for r in races[:n_tr]}
    test_races = races[n_tr:]
    cut = races[n_tr]["ymd"]
    print(f"分割: 学習 {n_tr:,}レース(〜{cut}) / 評価 {len(test_races):,}レース")

    df_tr = df[df.index.astype(str).isin(train_rids)]
    booster, scale, diag = fit_residual_head(df_tr, fcols, num_boost_round=400)
    print(f"残差ヘッド: scale={scale:.2f}"
          f"  validNLL 市場{diag['nll_market']:.4f}→{diag['nll_used']:.4f}")

    # 評価レースに残差を焼き込む
    df_te = df[~df.index.astype(str).isin(train_rids)]
    r_hat = predict_residual(booster, df_te, fcols, scale)
    uma_te = pd.to_numeric(df_te["馬番"], errors="coerce").to_numpy()
    cache: dict[str, dict[int, float]] = {}
    for rid, u, v in zip(df_te.index.astype(str), uma_te, r_hat.to_numpy(), strict=True):
        if pd.notna(u):
            cache.setdefault(str(rid), {})[int(u)] = float(v)
    for r in test_races:
        r["residual"] = cache.get(r["race_id"], {})

    res = compare_models(test_races, lambda r: market_probs(r["odds"]),
                         lambda r: true_probs(r["odds"], r.get("residual", {})),
                         k_extra_params=len(fcols))
    print("\n== JRDB VOI 判定（市場=確定単勝オッズ・帰無）==")
    print(f"n={res['n_races']:,}  ΔNLL={res['d_nll']:+.5f}  CI95=({res['d_nll_ci95'][0]:+.5f},"
          f" {res['d_nll_ci95'][1]:+.5f})  LRT p={res['lrt_p']:.3g}")
    print(f"ΔECE={res['d_ece']:+.5f}  ΔKL(VOI)={res['d_kl_market']:+.5f} nats/レース")
    print(f"success = {res['success']}"
          + ("  ← JRDBは確定市場を超えない（前日情報は最終オッズに織り込み済み）"
             if not res["success"] else
             "  ← ⚠ JRDBが確定市場を上回る（要 placebo/別年 再検証）"))

    # 2軸 placebo
    from src.simulation._pnl_objective import evaluate_pnl
    print("\nPnL（E[logW]・確定オッズ市場）:")
    for name, kw in (("本番", {}), ("placebo残差", {"placebo": True}),
                     ("placeboオッズ", {"placebo_odds": True})):
        o = evaluate_pnl(test_races, elogw=True, **kw)
        print(f"  {name:<12} 点={o['n_bets']:>5,} logW={o['log_growth']:+8.2f}"
              f" flatROI={o['flat_roi']:.3f} MDD={o['max_drawdown']:.2f}")

    print("\n人気帯別較正:")
    cb = calibration_by_odds_band(test_races, lambda r: market_probs(r["odds"]))
    cc = calibration_by_odds_band(test_races,
                                  lambda r: true_probs(r["odds"], r.get("residual", {})))
    for lab in cb:
        if cb[lab].get("n"):
            print(f"  {lab:<8} 市場 {cb[lab]['bias']:+.4f} → JRDB込み {cc[lab]['bias']:+.4f}")

    imp = sorted(zip(fcols, booster.feature_importance(importance_type="gain"), strict=False),
                 key=lambda t: -t[1])[:10]
    print("\n特徴量重要度（gain 上位）:")
    for name, v in imp:
        print(f"  {name:<22} {float(v):>10,.1f}")


if __name__ == "__main__":
    main()
