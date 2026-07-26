"""早期オッズ路線 VOI テスト — オッズ変動（早期→最終）は最終市場を超える情報か。

パリミュチュエルの構造: 配当は投票時点に関わらず**最終オッズで確定**する。よって
「早く賭ける」こと自体に edge は無い（我々の JRDB×最終市場テストが VOI=0 を確定済み）。
唯一残る早期オッズの筋は「**市場の動き（早期→最終の変動）が、最終価格にすら織り込まれ
きらない情報を持つか**」— 遅い資金（steam）が動かした方向が最終でも過小/過大反応なら edge。

設計（既存の市場アンカー機構をそのまま再利用）:
  市場 q = 最終フェーズ(t0/t5)オッズ（最も効率的）
  特徴量 drift_i = log(早期オッズ_i / 最終オッズ_i)
     drift>0: オッズが延びた（人気離れ）/ drift<0: 詰まった（steam・遅い資金で本命化）
  残差ヘッド: P = softmax(log q + s·f(drift))。s>0 かつ success なら「最終価格が
     変動に過小/過大反応＝早期オッズ由来の非効率」が存在（＝早期オッズ路線に価値）。
  帰無: 最終市場が効率的（変動は最終価格に完全反映）なら s→0（賭け0）。

入力: odds_snapshots.pkl（列 race_id/bet_type/combo/odds/minutes_to_post/phase）＋
着順（featured or results.pkl）。JRDB を featured_jrdb 経由で併合すれば drift＋JRDB も可。

実行:
    python odds_drift_test.py                          # 変動のみ
    python odds_drift_test.py --min-gap-min 60         # 早期↔最終の最小時間差
"""
from __future__ import annotations

import argparse

import numpy as np
import pandas as pd

from src.constants._bet_types import BetType
from src.constants._local_paths import LocalPaths
from src.policies._market_residual import market_probs, true_probs
from src.simulation._model_compare import calibration_by_odds_band, compare_models
from src.simulation._pnl_objective import evaluate_pnl
from src.training._residual_head import fit_residual_head, predict_residual


def _umaban(combo) -> int | None:
    """combo（tuple/list/スカラー）から単勝の馬番を取り出す。"""
    if isinstance(combo, (tuple, list)) and combo:
        return int(combo[0])
    try:
        return int(combo)
    except (TypeError, ValueError):
        return None


def extract_drift(snapshots: pd.DataFrame, *, min_gap_min: int = 30) -> pd.DataFrame:
    """単勝スナップショット → (race_id, 馬番) 単位の早期/最終オッズと drift を返す。

    早期＝minutes_to_post 最大、最終＝最小。両者の時間差が min_gap_min 以上の馬のみ。
    列: race_id, umaban, early_odds, final_odds, drift(=log(early/final))。
    """
    s = snapshots
    s = s[s["bet_type"] == BetType.TANSHO].copy()
    s["umaban"] = s["combo"].map(_umaban)
    s["odds"] = pd.to_numeric(s["odds"], errors="coerce")
    s["mtp"] = pd.to_numeric(s["minutes_to_post"], errors="coerce")
    s = s.dropna(subset=["umaban", "odds", "mtp"])
    s = s[s["odds"] > 1.0]
    rows = []
    for (rid, u), g in s.groupby([s["race_id"].astype(str), "umaban"]):
        g = g.sort_values("mtp")
        final = g.iloc[0]      # mtp 最小＝最終
        early = g.iloc[-1]     # mtp 最大＝早期
        if early["mtp"] - final["mtp"] < min_gap_min:
            continue
        eo, fo = float(early["odds"]), float(final["odds"])
        if eo > 1.0 and fo > 1.0:
            rows.append({"race_id": str(rid), "umaban": int(u),
                         "early_odds": eo, "final_odds": fo,
                         "drift": float(np.log(eo / fo))})
    return pd.DataFrame(rows)


def load_winners(drift_rids: set[str]) -> dict[str, int]:
    """着順ソースから {race_id: 勝ち馬番}。featured 優先、無ければ results.pkl。"""
    from src.constants._results_cols import ResultsCols
    try:
        from app._model_eval import load_featured_data
        f = load_featured_data()
    except Exception:  # noqa: BLE001
        f = None
    if f is not None and not f.empty and ResultsCols.RANK in f.columns:
        src = f[[ResultsCols.UMABAN, ResultsCols.RANK]].copy()
        src["race_id"] = src.index.astype(str)
    else:
        import os
        if not os.path.exists(LocalPaths.RAW_RESULTS_PATH):
            return {}
        r = pd.read_pickle(LocalPaths.RAW_RESULTS_PATH)
        r["race_id"] = r["race_id"].astype(str) if "race_id" in r.columns else r.index.astype(str)
        src = r.rename(columns={ResultsCols.UMABAN: ResultsCols.UMABAN})[
            ["race_id", ResultsCols.UMABAN, ResultsCols.RANK]]
    src = src[src["race_id"].isin(drift_rids)]
    uma = pd.to_numeric(src[ResultsCols.UMABAN], errors="coerce")
    rank = pd.to_numeric(src[ResultsCols.RANK], errors="coerce")
    win = {}
    for rid, u, rk in zip(src["race_id"], uma, rank, strict=False):
        if pd.notna(u) and rk == 1:
            win[str(rid)] = int(u)
    return win


def build_races(drift: pd.DataFrame, winners: dict[str, int]) -> list[dict]:
    """drift フレーム＋勝ち馬 → レース辞書（odds=最終・feature=drift・winner）。"""
    races = []
    for rid, g in drift.groupby("race_id"):
        if rid not in winners or len(g) < 5:
            continue
        if float((1.0 / g["final_odds"]).sum()) < 1.02:  # 控除ガード
            continue
        omap = {int(u): float(o) for u, o in zip(g["umaban"], g["final_odds"], strict=False)}
        dmap = {int(u): float(d) for u, d in zip(g["umaban"], g["drift"], strict=False)}
        w = winners[rid]
        if w not in omap:
            continue
        races.append({"race_id": str(rid), "odds": omap, "drift": dmap, "winner": w})
    return races


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--snapshots", default=LocalPaths.RAW_ODDS_SNAPSHOT_PATH)
    ap.add_argument("--min-gap-min", type=int, default=30,
                    help="早期↔最終の最小時間差（分）")
    ap.add_argument("--train-frac", type=float, default=0.6)
    args = ap.parse_args()

    import os
    if not os.path.exists(args.snapshots):
        raise SystemExit(f"odds_snapshots がありません: {args.snapshots}")
    snap = pd.read_pickle(args.snapshots)
    if not isinstance(snap, pd.DataFrame):
        snap = pd.DataFrame(snap)
    drift = extract_drift(snap, min_gap_min=args.min_gap_min)
    print(f"単勝の早期/最終ペア: {len(drift):,}頭 / レース {drift['race_id'].nunique():,}")
    if drift.empty:
        raise SystemExit("早期↔最終ペアが0（--min-gap-min を下げるか、スナップショット確認）")
    print(f"drift統計: 中央値{drift['drift'].median():+.3f} "
          f"詰まり(<-0.1){(drift['drift']<-0.1).mean()*100:.0f}% "
          f"延び(>0.1){(drift['drift']>0.1).mean()*100:.0f}%")

    winners = load_winners(set(drift["race_id"]))
    races = build_races(drift, winners)
    # race_id 昇順 ≒ 時系列（同年内）で OOS 分割
    races.sort(key=lambda r: r["race_id"])
    print(f"有効レース（勝ち馬付き）: {len(races):,}")
    if len(races) < 300:
        print("警告: レース数が少ない（スナップショットの蓄積が必要）")

    n_tr = int(len(races) * args.train_frac)
    train_rids = {r["race_id"] for r in races[:n_tr]}
    test_races = races[n_tr:]

    # 残差ヘッド: 市場=最終オッズ、特徴量=drift（per-horse フレームを合成）
    rows = []
    for r in races:
        for u, o in r["odds"].items():
            rows.append({"race_id": r["race_id"], "馬番": u, "単勝": o,
                         "着順": 1 if u == r["winner"] else 2,
                         "drift": r["drift"].get(u, 0.0)})
    df = pd.DataFrame(rows).set_index("race_id")
    df_tr = df[df.index.astype(str).isin(train_rids)]
    booster, scale, diag = fit_residual_head(df_tr, ["drift"], num_boost_round=300)
    print(f"\n残差ヘッド（市場=最終・特徴=変動）: scale={scale:.2f}"
          f"  validNLL 市場{diag['nll_market']:.4f}→{diag['nll_used']:.4f}")

    df_te = df[~df.index.astype(str).isin(train_rids)]
    r_hat = predict_residual(booster, df_te, ["drift"], scale)
    uma_te = pd.to_numeric(df_te["馬番"], errors="coerce").to_numpy()
    cache: dict[str, dict[int, float]] = {}
    for rid, u, v in zip(df_te.index.astype(str), uma_te, r_hat.to_numpy(), strict=True):
        if pd.notna(u):
            cache.setdefault(str(rid), {})[int(u)] = float(v)
    for r in test_races:
        r["residual"] = cache.get(r["race_id"], {})

    res = compare_models(test_races, lambda r: market_probs(r["odds"]),
                         lambda r: true_probs(r["odds"], r.get("residual", {})),
                         k_extra_params=1)
    print("\n== オッズ変動 VOI 判定（市場=最終オッズ・帰無）==")
    print(f"n={res['n_races']:,}  ΔNLL={res['d_nll']:+.5f}  CI95=({res['d_nll_ci95'][0]:+.5f},"
          f" {res['d_nll_ci95'][1]:+.5f})  LRT p={res['lrt_p']:.3g}")
    print(f"ΔECE={res['d_ece']:+.5f}  ΔKL(VOI)={res['d_kl_market']:+.5f} nats/レース")
    print(f"success = {res['success']}"
          + ("  ← 変動は最終価格に織り込み済み（早期オッズにも edge 無し）"
             if not res["success"] else
             "  ← ⚠ 変動が最終価格を上回る＝早期オッズ非効率あり（要 placebo/別期間）"))

    print("\nPnL（E[logW]・最終オッズ配当）:")
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
            print(f"  {lab:<8} 市場 {cb[lab]['bias']:+.4f} → 変動込み {cc[lab]['bias']:+.4f}")


if __name__ == "__main__":
    main()
