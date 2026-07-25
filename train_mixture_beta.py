"""β(脚質×ペース状態) の実データ rolling fit と事前定義判定 CLI（優先順位3の前段）。

前提: train_pace_state.py 済み（data/raw/pace_states.pkl に OOS の P(z) がある）。

やること:
1) pace_states.pkl のレースについて、featured から 単勝オッズ・勝ち馬・各馬の脚質
   （pace_median → style_from_pace_ratio）を取り、レース辞書列を作る。
2) rolling-origin（fit は各 fold の過去のみ）で fit_beta_fast（12パラメータ・λ_β‖β‖²）。
3) 事前定義判定（_model_compare）: β=0（＝市場そのもの）をベースラインに
   ΔNLL / Bootstrap CI / LRT(df=12) / ΔECE / ΔKL(VOI) — success 判定は後知恵調整なし。
4) 人気帯別較正で β あり/なし の bias 変化を表示（favorite-longshot 局在の監査）。

期待値の事前宣言: 市場は明白な展開利得を織り込むため **Δ≈0（success=False）が正常終了**。
成立すれば「展開×脚質は市場が織り込み切れていない」という発見であり、まずリークを疑う。

実行例:
    python train_mixture_beta.py
    python train_mixture_beta.py --l2-beta 0.05
"""
from __future__ import annotations

import argparse
import os

import numpy as np
import pandas as pd

from src.constants._local_paths import LocalPaths
from src.constants._pace_states import PACE_STATES, STYLES
from src.constants._results_cols import ResultsCols
from src.policies._market_residual import market_probs
from src.policies._mixture_pl import fit_beta_fast, mixture_win_probs, style_from_pace_ratio
from src.simulation._model_compare import calibration_by_odds_band
from src.simulation._rolling_origin import rolling_origin_compare


def build_races(featured: pd.DataFrame, store: pd.DataFrame, *, min_horses: int = 5) -> list[dict]:
    """featured × pace_states → レース辞書列（odds/styles/winner/pace_probs/year）。"""
    umaban = ResultsCols.UMABAN
    odds_col = ResultsCols.TANSHO_ODDS
    rank_col = next((c for c in ("着順", "rank_real", "order") if c in featured.columns), None)
    use_rank_win = rank_col is None and "rank_win" in featured.columns

    if umaban not in featured.columns or odds_col not in featured.columns:
        return []
    store_ids = set(store.index.astype(str))
    races: list[dict] = []
    for rid, g in featured.groupby(featured.index.astype(str)):
        rid = str(rid)
        if rid not in store_ids or len(g) < min_horses:
            continue
        uma = pd.to_numeric(g[umaban], errors="coerce")
        odds = pd.to_numeric(g[odds_col], errors="coerce")
        pm = pd.to_numeric(g.get("pace_median"), errors="coerce") if "pace_median" in g.columns else None
        if rank_col is not None:
            is_win = pd.to_numeric(g[rank_col], errors="coerce") == 1
        elif use_rank_win:
            is_win = pd.to_numeric(g["rank_win"], errors="coerce") == 1
        else:
            continue
        omap, styles = {}, {}
        winner = None
        for i in range(len(g)):
            u, o = uma.iloc[i], odds.iloc[i]
            if pd.isna(u) or pd.isna(o) or o <= 1.0:
                continue
            u = int(u)
            omap[u] = float(o)
            styles[u] = style_from_pace_ratio(None if pm is None else pm.iloc[i])
            if bool(is_win.iloc[i]):
                winner = u
        if winner is None or len(omap) < min_horses:
            continue
        row = store.loc[rid]
        pz = {z: float(row[f"p_{z}"]) for z in PACE_STATES if f"p_{z}" in row}
        races.append({"race_id": rid, "year": int(rid[:4]), "odds": omap,
                      "styles": styles, "winner": winner, "pace_probs": pz})
    return races


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--l2-beta", type=float, default=0.1)
    ap.add_argument("--min-train-years", type=int, default=3)
    ap.add_argument("--fit-tail", type=int, default=12000,
                    help="fit に使う直近レース数上限（0=全量。時変性と速度のバランス）")
    args = ap.parse_args()

    from app._model_eval import load_featured_data

    if not os.path.exists(LocalPaths.PACE_STATES_PATH):
        raise SystemExit("pace_states.pkl がありません（先に train_pace_state.py）")
    store = pd.read_pickle(LocalPaths.PACE_STATES_PATH)
    featured = load_featured_data()
    if featured is None or featured.empty:
        raise SystemExit("featured がありません")
    # store 対象レースの行だけに絞る（メモリ・速度）
    featured = featured[featured.index.astype(str).isin(set(store.index.astype(str)))]
    print(f"対象レース: store {len(store):,} / featured 行 {len(featured):,}")

    races = build_races(featured, store)
    yrs = pd.Series([r["year"] for r in races])
    print(f"レース辞書: {len(races):,}（{yrs.min()}–{yrs.max()}）")
    if len(races) < 2000:
        print("警告: レース数が少ない（結合・勝ち馬判定を確認）")

    def fit_chal(train: list) -> dict:
        sub = train[-args.fit_tail:] if args.fit_tail else train
        return fit_beta_fast(sub, l2_beta=args.l2_beta)

    prob_base = lambda p, r: market_probs(r["odds"])  # noqa: E731  β=0 ＝市場そのもの
    prob_chal = lambda b, r: mixture_win_probs(       # noqa: E731
        r["odds"], None, r["styles"], b, r["pace_probs"])

    res = rolling_origin_compare(
        races, lambda t: None, prob_base, fit_chal, prob_chal,
        min_train_years=args.min_train_years, k_extra_params=len(STYLES) * len(PACE_STATES),
    )
    p = res["pooled"]
    print("\n== rolling-origin 事前定義判定（β=0 帰無） ==")
    print(f"n={p['n_races']:,}  ΔNLL={p['d_nll']:+.5f}  CI95=({p['d_nll_ci95'][0]:+.5f},"
          f" {p['d_nll_ci95'][1]:+.5f})  LRT p={p['lrt_p']:.3g}")
    print(f"ΔECE={p['d_ece']:+.5f}  ΔKL(VOI)={p['d_kl_market']:+.5f} nats/レース")
    print(f"success = {p['success']}"
          + ("（事前宣言どおり市場は織り込み済み＝正常終了）" if not p["success"]
             else "（⚠ 成立 — まずリーク/過学習を疑い placebo・別期間で再検証）"))
    print("fold別 ΔNLL:", [(f["year"], round(f["d_nll"], 5)) for f in res["folds"]])

    # 最終 fold の β 表（読める12個の数値 — 効きの方向の解釈用）
    beta = fit_chal(races)
    print("\nβ表（行=脚質・列=状態・状態内ゼロ平均）:")
    print(f"{'':>8}" + "".join(f"{z:>9}" for z in PACE_STATES))
    for s in STYLES:
        print(f"{s:>8}" + "".join(f"{beta[(s, z)]:>+9.4f}" for z in PACE_STATES))

    print("\n人気帯別較正（bias=予測平均−実勝率・正=過大評価）:")
    cb = calibration_by_odds_band(races, lambda r: prob_base(None, r))
    cc = calibration_by_odds_band(races, lambda r: prob_chal(beta, r))
    for lab in cb:
        if cb[lab].get("n"):
            print(f"  {lab:<8} n={cb[lab]['n']:>6,}  市場 {cb[lab]['bias']:+.4f}"
                  f" → β込み {cc[lab]['bias']:+.4f}")


if __name__ == "__main__":
    main()
