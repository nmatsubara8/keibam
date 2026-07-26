"""残差ヘッド r_θ の rolling-origin 実データ検証 CLI（ベースライン台帳の最終ピース／JRDB 土台）。

canonical: P = softmax(log q + s·f_θ(x))。市場 q をアンカーに、featured の既存特徴量で
「市場超の情報」があるかを事前定義4軸（ΔNLL/ΔECE/ΔKL/ΔROI）＋2軸 placebo で判定する。

リーク遮断: fold ループで **各テスト年のレースには「その年より過去だけで fit したモデル」の
残差（OOS）だけ**を焼き込み、compare/PnL/較正のすべてを同じ OOS 集合で評価する。

期待値の事前宣言: **公開データでは Δ≈0（success=False・s→小）が正常終了**。この結果が
「JRDB 投入前のベースライン台帳」になり、以後は同一ハーネスで --drop-prefix を切り替える
だけでカテゴリ別アブレーション（VOI/コスト評価）ができる。

実行例:
    python train_residual.py --since-year 2018
    python train_residual.py --since-year 2018 --drop-prefix jrdb_   # JRDB無しベースライン
"""
from __future__ import annotations

import argparse

import numpy as np
import pandas as pd

from src.constants._results_cols import ResultsCols
from src.policies._market_residual import market_probs, true_probs
from src.simulation._model_compare import calibration_by_odds_band, compare_models
from src.simulation._pnl_objective import evaluate_pnl
from src.simulation._rolling_origin import rolling_origin_folds
from src.training._residual_head import (
    fit_residual_head,
    predict_residual,
    residual_feature_cols,
)


MIN_OVERROUND = 1.02  # 単勝ブックの最低 Σ(1/odds)。実市場は控除で必ず >1（JRA≈1.25）。


def build_frames(featured: pd.DataFrame, since_year: int):
    """featured → (per-horse フレーム, レース辞書列)。レース辞書は評価・PnL 用。

    控除ガード: Σ(1/odds) < MIN_OVERROUND のレースは除外する。オッズ欠損（一部の馬の
    単勝が無い）レースは Z<1 になり得て、その場合**全馬 EV=1/Z>1 の疑似裁定（Dutch book）**
    が生じ、E[logW] 配分が「保証利益」を拾ってしまう（実市場には存在しないデータ穴。
    実データで logW+44 が placebo残差でも消えず placeboオッズでのみ崩壊する形で検出）。
    """
    rid0 = featured.index.astype(str)
    ok_year = pd.to_numeric(pd.Series(rid0.str[:4]), errors="coerce") >= since_year
    df = featured[ok_year.to_numpy()]
    rid = df.index.astype(str)

    uma = pd.to_numeric(df[ResultsCols.UMABAN], errors="coerce")
    odds = pd.to_numeric(df[ResultsCols.TANSHO_ODDS], errors="coerce")
    rank = pd.to_numeric(df[ResultsCols.RANK], errors="coerce")

    races: list[dict] = []
    n_dutch = 0
    key = pd.DataFrame({"rid": rid, "uma": uma.to_numpy(), "odds": odds.to_numpy(),
                        "rank": rank.to_numpy()})
    for r, g in key.groupby("rid"):
        gg = g.dropna(subset=["uma", "odds"])
        gg = gg[gg["odds"] > 1.0]
        if len(gg) < 5:
            continue
        overround = float((1.0 / gg["odds"]).sum())
        if overround < MIN_OVERROUND:  # オッズ欠損起因の疑似裁定ブック → 評価から除外
            n_dutch += 1
            continue
        omap = {int(u): float(o) for u, o in zip(gg["uma"], gg["odds"], strict=False)}
        ranks = {int(u): int(rk) for u, rk in zip(gg["uma"], gg["rank"], strict=False)
                 if pd.notna(rk)}
        winner = next((u for u, rk in ranks.items() if rk == 1), None)
        if winner is None:
            continue
        races.append({"race_id": str(r), "year": int(str(r)[:4]), "odds": omap,
                      "ranks": ranks, "winner": winner})
    if n_dutch:
        print(f"控除ガード: Σ(1/odds)<{MIN_OVERROUND} の {n_dutch:,} レースを除外"
              "（オッズ欠損起因の疑似裁定）")
    return df, races


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--since-year", type=int, default=2018)
    ap.add_argument("--min-train-years", type=int, default=3)
    ap.add_argument("--drop-prefix", action="append", default=[],
                    help="外す特徴量プレフィックス（アブレーション。複数可）")
    ap.add_argument("--features", choices=("all", "market-only", "no-market"), default="all",
                    help="監査用アブレーション: market-only=単勝/人気のみ（FLバイアス再較正の"
                         "切り分け）/ no-market=市場由来列を名前ベースで全除去（単勝*/人気*/odds* — "
                         "raw だけ抜くと派生列 単勝_log 等が市場情報を持ち込むため）")
    ap.add_argument("--organizer", choices=("both", "central", "local"), default="both",
                    help="場フィルタ（race_id 5-6桁目 01-10=中央）。効果の中央/地方局在の監査用")
    ap.add_argument("--featured-pkl", default=None,
                    help="load_featured_data の代わりに読む pickle（scripts/jrdb_build_features.py "
                         "の出力 featured_jrdb.pkl 等）。JRDB 列を含む featured で VOI 評価する用")
    ap.add_argument("--drop-jrdb", action="store_true",
                    help="JRDB 由来列（jrdb_*/prev_*）を全除去してベースライン化。--featured-pkl と "
                         "組で A/B: 付与あり(ΔKL_with) vs --drop-jrdb(ΔKL_base) の差が VOI")
    ap.add_argument("--num-boost-round", type=int, default=600)
    args = ap.parse_args()

    if args.featured_pkl:
        featured = pd.read_pickle(args.featured_pkl)
        print(f"featured 読込: {args.featured_pkl}（{len(featured):,}行）")
    else:
        from app._model_eval import load_featured_data
        featured = load_featured_data()
    if featured is None or featured.empty:
        raise SystemExit("featured がありません")
    df, races = build_frames(featured, args.since_year)
    if args.organizer != "both":
        central = {f"{i:02d}" for i in range(1, 11)}
        keep = (lambda pc: pc in central) if args.organizer == "central" else (
            lambda pc: pc not in central)
        races = [r for r in races if keep(r["race_id"][4:6])]
        rid_pc = df.index.astype(str).str[4:6]
        df = df[[keep(pc) for pc in rid_pc]]
        print(f"organizer={args.organizer}: レース {len(races):,} / 行 {len(df):,}")
    from src.jrdb._augment import JRDB_COLS
    extra_drop = tuple(JRDB_COLS) if args.drop_jrdb else ()
    fcols = residual_feature_cols(df, drop_prefixes=tuple(args.drop_prefix),
                                  extra_drop=extra_drop)
    jrdb_present = [c for c in JRDB_COLS if c in df.columns]
    if jrdb_present:
        cov = {c: f"{df[c].notna().mean():.1%}" for c in jrdb_present}
        print(f"JRDB列 {'除外' if args.drop_jrdb else '使用'}: {cov}")
    market_cols = list(dict.fromkeys(
        c for c in (ResultsCols.TANSHO_ODDS, ResultsCols.POPULARITY) if c in fcols))
    if args.features == "market-only":
        fcols = market_cols
    elif args.features == "no-market":
        # 名前ベースで市場由来列を全除去（raw だけ抜くと 単勝_log/単勝_z/人気_z 等の
        # 派生列が市場情報を持ち込む — 実データ監査で重要度1位が 単勝_log になり判明）
        _mkt_pat = ("単勝", "人気", "odds", "popularity")
        fcols = [c for c in fcols if not any(p in str(c).lower() or p in str(c)
                                             for p in _mkt_pat)]
    print(f"行 {len(df):,} / レース {len(races):,} / 特徴量 {len(fcols)} 列"
          f"（features={args.features}）"
          + (f"（除外: {args.drop_prefix}）" if args.drop_prefix else ""))

    year_all = pd.to_numeric(pd.Series(df.index.astype(str).str[:4], index=df.index),
                             errors="coerce")

    # ── fold ループ: 各テスト年に「過去のみ fit」の OOS 残差を焼き込む ──
    folds = rolling_origin_folds(races, min_train_years=args.min_train_years)
    oos_races: list[dict] = []
    last_booster = None
    for train, test, test_year in folds:
        max_y = max(r["year"] for r in train)
        sub = df[(year_all <= max_y).to_numpy()]
        booster, scale, diag = fit_residual_head(
            sub, fcols, num_boost_round=args.num_boost_round)
        te = df[(year_all == test_year).to_numpy()]
        r_hat = predict_residual(booster, te, fcols, scale)
        # 馬番は te 自身の列から位置合わせで取る（uma_all.loc[te.index] は重複インデックスで
        # 行が爆発し zip がずれる＝残差が別の馬番に付くバグがあった。strict=True で再発防止）
        uma_te = pd.to_numeric(te[ResultsCols.UMABAN], errors="coerce").to_numpy()
        cache: dict[str, dict[int, float]] = {}
        for rr, u, v in zip(te.index.astype(str), uma_te, r_hat.to_numpy(), strict=True):
            if pd.notna(u):
                cache.setdefault(str(rr), {})[int(u)] = float(v)
        for r in test:
            rr2 = dict(r)
            rr2["residual"] = cache.get(r["race_id"], {})
            oos_races.append(rr2)
        print(f"  fit〜{max_y}→{test_year}: scale={scale:.2f} it={diag['best_iteration']}"
              f"  validNLL 市場{diag['nll_market']:.4f}→{diag['nll_best']:.4f}"
              f"  test {len(test):,}レース")
        last_booster = booster

    if not oos_races:
        yrs = sorted({r["year"] for r in races})
        raise SystemExit(
            f"OOS fold が作れません（年度: {yrs}・min_train_years={args.min_train_years}）。"
            f"年数が {args.min_train_years + 1} 未満です — --min-train-years 1 で再実行してください"
        )

    prob_base = lambda r: market_probs(r["odds"])                       # noqa: E731
    prob_chal = lambda r: true_probs(r["odds"], r.get("residual", {}))  # noqa: E731

    res = compare_models(oos_races, prob_base, prob_chal, k_extra_params=len(fcols))
    print("\n== 事前定義判定（市場帰無・全OOS） ==")
    print(f"n={res['n_races']:,}  ΔNLL={res['d_nll']:+.5f}  CI95=({res['d_nll_ci95'][0]:+.5f},"
          f" {res['d_nll_ci95'][1]:+.5f})  LRT p={res['lrt_p']:.3g}")
    print(f"ΔECE={res['d_ece']:+.5f}  ΔKL(VOI)={res['d_kl_market']:+.5f} nats/レース")
    print(f"success = {res['success']}"
          + ("（事前宣言どおり＝ベースライン台帳確定）" if not res["success"]
             else "（⚠ 成立 — placebo・別期間・リーク監査を先に）"))
    from src.simulation._model_compare import race_nll
    fold_d = {}
    for r in oos_races:
        d = race_nll(prob_chal(r), r["winner"]) - race_nll(prob_base(r), r["winner"])
        fold_d.setdefault(r["year"], []).append(d)
    print("fold別 ΔNLL:", [(y, round(float(np.mean(v)), 5)) for y, v in sorted(fold_d.items())])

    # ── ΔROI 軸: E[logW] 配分 + 2軸 placebo（全て同じ OOS 残差で） ──
    print("\nPnL（E[logW]配分・OOS残差のみ）:")
    for name, kw in (("本番", {}), ("placebo残差", {"placebo": True}),
                     ("placeboオッズ", {"placebo_odds": True})):
        out = evaluate_pnl(oos_races, elogw=True, **kw)
        print(f"  {name:<12} 点={out['n_bets']:>6,} logW={out['log_growth']:+9.2f} "
              f"flatROI={out['flat_roi']:.3f} MDD={out['max_drawdown']:.2f} ES5={out['es_5']:.3f}")

    print("\n人気帯別較正（bias=予測平均−実勝率）:")
    cb = calibration_by_odds_band(oos_races, prob_base)
    cc = calibration_by_odds_band(oos_races, prob_chal)
    for lab in cb:
        if cb[lab].get("n"):
            print(f"  {lab:<8} 市場 {cb[lab]['bias']:+.4f} → 残差込み {cc[lab]['bias']:+.4f}")

    # 監査: f_θ を駆動している特徴量（gain 上位）。人気/オッズ系が支配なら
    # 「FLバイアス再較正」story、見慣れない列が上位なら まずリークを疑う。
    if last_booster is not None:
        imp = last_booster.feature_importance(importance_type="gain")
        pairs = sorted(zip(fcols, imp, strict=False), key=lambda t: -float(t[1]))[:20]
        print("\n特徴量重要度（最終fold・gain 上位20）:")
        for name, v in pairs:
            print(f"  {name:<40} {float(v):>12,.1f}")


if __name__ == "__main__":
    main()
