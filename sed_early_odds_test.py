"""早期オッズ路線 VOI テスト（JRDB SED版・scheduler不要）— 10時単勝→確定の変動は最終市場を超えるか。

odds_snapshots(scheduler収集)は蓄積が少ない(数百レース未満)ため、JRDB SED の
**10時単勝オッズ**（@297・SEC仕様の項目順×実データmagnitudeの三点一致で確定）を歴史的な
「早期市場」に使う。2025-2026年 JRA 全レースで即座に大規模検証できる。

パリミュチュエル構造の再確認: 配当は最終オッズで確定するため「早く賭ける」自体に edge は
無い。唯一の筋は「市場の動き(10時→確定)が最終価格にすら織り込まれない情報を持つか」。
市場アンカー = 確定単勝、特徴量 = drift=log(10時/確定)、残差ヘッド＋事前定義判定。

結果(2025-2026・5,321レース): scale=0.00 / VOI=0.00000 / success=False（完全帰無）。
10時→確定の変動は最終価格に完全織り込み済み。早期オッズ路線も最終市場に対しては edge 無し。

実行: python sed_early_odds_test.py --jrdb-dir /tmp/jrdb_all
"""
from __future__ import annotations

import argparse
import glob

import numpy as np
import pandas as pd

from src.jrdb._parser import parse
from src.policies._market_residual import market_probs, true_probs
from src.simulation._model_compare import calibration_by_odds_band, compare_models
from src.simulation._pnl_objective import evaluate_pnl
from src.training._residual_head import fit_residual_head, predict_residual

_CENTRAL = {f"{i:02d}" for i in range(1, 11)}


def load_sed(jrdb_dir: str, central_only: bool = True) -> pd.DataFrame:
    """SED 群 → (race_id,馬番) の 10時単勝/確定単勝/着順 と drift。"""
    rows = []
    for f in sorted(glob.glob(f"{jrdb_dir}/SED*.txt")):
        d = parse(f, "SED")
        rows.append(d[["race_id", "umaban", "kakutei_tansho", "odds_10_tansho", "chakujun"]])
    df = pd.concat(rows, ignore_index=True).dropna(
        subset=["umaban", "kakutei_tansho", "odds_10_tansho", "chakujun"])
    df = df[(df["kakutei_tansho"] > 1.0) & (df["odds_10_tansho"] > 1.0)].copy()
    df["drift"] = np.log(df["odds_10_tansho"] / df["kakutei_tansho"])
    df["単勝"] = df["kakutei_tansho"]
    if central_only:
        df = df[df["race_id"].astype(str).str[4:6].isin(_CENTRAL)]
    return df


def build_races(df: pd.DataFrame) -> list[dict]:
    races = []
    for rid, g in df.groupby(df["race_id"].astype(str)):
        if len(g) < 5 or float((1.0 / g["kakutei_tansho"]).sum()) < 1.02:
            continue
        w = g[g["chakujun"] == 1]
        if len(w) == 0:
            continue
        races.append({"race_id": str(rid), "winner": int(w["umaban"].iloc[0]),
                      "odds": {int(u): float(o) for u, o in
                               zip(g["umaban"], g["kakutei_tansho"], strict=False)},
                      "drift": {int(u): float(d) for u, d in
                                zip(g["umaban"], g["drift"], strict=False)}})
    return races


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--jrdb-dir", default="/tmp/jrdb_all")
    ap.add_argument("--train-frac", type=float, default=0.6)
    ap.add_argument("--both-organizers", action="store_true")
    args = ap.parse_args()

    df = load_sed(args.jrdb_dir, central_only=not args.both_organizers)
    print(f"10時→確定 変動: {len(df):,}頭 / drift中央{df['drift'].median():+.3f} "
          f"詰まり{(df['drift'] < 0).mean() * 100:.0f}%/延び{(df['drift'] > 0).mean() * 100:.0f}%")
    races = build_races(df)
    races.sort(key=lambda r: r["race_id"])
    print(f"有効レース: {len(races):,}")
    if not races:
        raise SystemExit("SED に 10時単勝オッズが無い（速報版のみ？正式版を使用）")

    n_tr = int(len(races) * args.train_frac)
    trids = {r["race_id"] for r in races[:n_tr]}
    test = races[n_tr:]
    rows = [{"race_id": r["race_id"], "馬番": u, "単勝": o,
             "着順": 1 if u == r["winner"] else 2, "drift": r["drift"].get(u, 0.0)}
            for r in races for u, o in r["odds"].items()]
    D = pd.DataFrame(rows).set_index("race_id")
    booster, scale, diag = fit_residual_head(
        D[D.index.astype(str).isin(trids)], ["drift"], num_boost_round=400)
    print(f"残差ヘッド(市場=確定/特徴=10時→確定変動): scale={scale:.2f} "
          f"改善={diag['nll_market'] - diag['nll_best']:+.5f}")
    Dte = D[~D.index.astype(str).isin(trids)]
    r_hat = predict_residual(booster, Dte, ["drift"], scale)
    cache: dict[str, dict[int, float]] = {}
    for rid, u, v in zip(Dte.index.astype(str), pd.to_numeric(Dte["馬番"]).to_numpy(),
                         r_hat.to_numpy(), strict=True):
        cache.setdefault(str(rid), {})[int(u)] = float(v)
    for r in test:
        r["residual"] = cache.get(r["race_id"], {})

    res = compare_models(test, lambda r: market_probs(r["odds"]),
                         lambda r: true_probs(r["odds"], r.get("residual", {})),
                         k_extra_params=1)
    print("\n== 早期オッズ変動 VOI（10時単勝→確定・市場=確定）==")
    print(f"  n={res['n_races']:,} ΔNLL={res['d_nll']:+.5f} "
          f"CI=({res['d_nll_ci95'][0]:+.5f},{res['d_nll_ci95'][1]:+.5f}) "
          f"ΔKL={res['d_kl_market']:+.5f} success={res['success']}")
    print("  " + ("変動は最終価格に織り込み済み＝早期オッズ路線も edge 無し"
                  if not res["success"] else "⚠ 変動が最終を超える＝要精査"))
    for name, kw in (("本番", {}), ("placebo残差", {"placebo": True}),
                     ("placeboオッズ", {"placebo_odds": True})):
        o = evaluate_pnl(test, elogw=True, **kw)
        print(f"  {name:<10} 点={o['n_bets']:>4} logW={o['log_growth']:+7.2f} "
              f"flatROI={o['flat_roi']:.3f}")
    cb = calibration_by_odds_band(test, lambda r: market_probs(r["odds"]))
    cc = calibration_by_odds_band(test, lambda r: true_probs(r["odds"], r.get("residual", {})))
    print("人気帯別較正:")
    for lab in cb:
        if cb[lab].get("n"):
            print(f"  {lab:<8} 市場 {cb[lab]['bias']:+.4f} → 変動込み {cc[lab]['bias']:+.4f}")


if __name__ == "__main__":
    main()
