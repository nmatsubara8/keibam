"""「どちらでもない」難レースの事前識別＝本命選択に向かないレースを見送れるか（点5・別研究）。

研究テーマ（市場×モデル不一致の説明）の点5。不一致の 54–58% は LGBM本命も市場本命も勝たない
（＝どちらを信じるかの二択が無意味な難レース）。ここでは「勝者選択」ではなく、
    P(モデル本命も市場本命も負ける)  ＝ neither
を**完全OOS**（学習は評価年より前のみ）で予測できるかだけを見る。収益目的ではない。

leak 規律:
  - 目的変数 neither は確定着順由来（settlement）だが、学習/評価は年で分離（walk-forward）。
  - 特徴は購入時点で観測可能なもの（CSV: prob_diff/odds_diff/市場人気順位/LGBMオッズ/校正確率、
    --featured 指定時は二頭差分 d_* とレース文脈）。確定オッズ・払戻は特徴に使わない。

出力:
  ① 各評価年の base rate（neither の実割合）と、多変量 OOS AUC / Brier（LightGBM。無ければ最良単変量）。
  ② 特徴ごとの walk-forward 単変量 AUC（学習年で向きを決め評価年で測る・完全leak-safe）。
     両評価年で安定して >0.55 なら『難レースは事前に見送れる』候補。全て≒0.5 なら見送り不能。

使い方:
  python scripts/difficulty_estimation.py --csv data/disagreement.csv
  python scripts/difficulty_estimation.py --csv data/disagreement.csv --featured
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def neither_label(df):
    """LGBM本命も市場本命も勝たなかった＝1（難レース）。着順不明行は NaN。"""
    import pandas as pd
    lh = pd.to_numeric(df["lgbm_hit"], errors="coerce")
    mh = pd.to_numeric(df["market_hit"], errors="coerce")
    y = 1.0 - lh - mh
    y[(lh.isna()) | (mh.isna())] = float("nan")
    return y.clip(0, 1)


def walk_forward_splits(years):
    """年を昇順に並べ、各評価年 Y に対し (学習年集合=Y未満, 評価年=Y) を返す（Y未満が空なら除外）。"""
    ys = sorted({str(y) for y in years})
    out = []
    for i, y in enumerate(ys):
        train = ys[:i]
        if train:
            out.append((tuple(train), y))
    return out


def _oriented_auc(train_x, train_y, test_x, test_y):
    """学習年で符号の向きを決め、評価年で単変量 AUC を測る（>0.5 に揃える完全 leak-safe な向き付け）。"""
    from src.simulation._bet_eval import _auc
    a_tr = _auc(list(zip(train_x, train_y, strict=False)))
    if a_tr is None:
        return None
    sign = 1.0 if a_tr >= 0.5 else -1.0
    return _auc(list(zip([sign * v for v in test_x], test_y, strict=False)))


def _lgbm_oos(Xtr, ytr, Xte, yte):
    """LightGBM で多変量 OOS AUC/Brier。LightGBM 不在や単一クラスなら None。"""
    try:
        import lightgbm as lgb
    except Exception:  # noqa: BLE001
        return None
    import numpy as np
    if len(set(ytr)) < 2 or len(ytr) < 30:
        return None
    ds = lgb.Dataset(np.asarray(Xtr, float), label=np.asarray(ytr, float))
    params = {"objective": "binary", "verbosity": -1, "num_leaves": 15,
              "learning_rate": 0.05, "min_data_in_leaf": 20, "feature_fraction": 0.8}
    booster = lgb.train(params, ds, num_boost_round=100)
    p = booster.predict(np.asarray(Xte, float))
    from src.simulation._bet_eval import _auc
    auc = _auc(list(zip(p, yte, strict=False)))
    brier = float(np.mean((np.asarray(p) - np.asarray(yte, float)) ** 2))
    return {"auc": auc, "brier": brier, "n_train": len(ytr), "n_test": len(yte)}


def main() -> int:
    import numpy as np
    import pandas as pd

    ap = argparse.ArgumentParser(description="難レース(どちらでもない)の完全OOS事前識別")
    ap.add_argument("--csv", default="data/disagreement.csv")
    ap.add_argument("--featured", nargs="?", const="", default=None)
    args = ap.parse_args()

    if not Path(args.csv).exists():
        print(f"CSV がありません: {args.csv}", file=sys.stderr)
        return 1
    df = pd.read_csv(args.csv, dtype={"race_id": str, "year": str})
    for c in ("lgbm_hit", "market_hit"):
        df[c] = pd.to_numeric(df.get(c), errors="coerce")
    df["_neither"] = neither_label(df)
    df = df.dropna(subset=["_neither"]).copy()

    feats = ["prob_diff", "odds_diff", "market_rank_of_lgbm", "lgbm_top_odds", "lgbm_prob_cal"]
    if args.featured is not None:
        from scripts.analyze_disagreement import _featured_join
        df, _race_cols, diff_cols = _featured_join(df, args.featured or None)
        feats = feats + diff_cols

    print(f"=== 難レース(どちらでもない)の完全OOS識別 {args.csv} ===")
    print(f"不一致 {len(df):,} / neither 全体割合 {df['_neither'].mean():.1%}"
          f" / 年={sorted(df['year'].unique())}")
    splits = walk_forward_splits(df["year"])
    if not splits:
        print("評価年より前の学習年が無く walk-forward できません（年が1つ）。", file=sys.stderr)
        return 2

    print("\n[① 多変量 完全OOS（学習=評価年より前のみ）]")
    for train_years, test_y in splits:
        tr = df[df["year"].isin(train_years)]
        te = df[df["year"] == test_y]
        base = te["_neither"].mean()
        Xtr = tr[feats].apply(pd.to_numeric, errors="coerce").fillna(0.0).to_numpy()
        Xte = te[feats].apply(pd.to_numeric, errors="coerce").fillna(0.0).to_numpy()
        res = _lgbm_oos(Xtr, tr["_neither"].to_numpy(), Xte, te["_neither"].to_numpy())
        tag = f"学習{'+'.join(train_years)}→評価{test_y}"
        if res is None:
            print(f"  {tag}: base={base:.1%} n_te={len(te):,}（LightGBM不可/単一クラス→②の単変量参照）")
        else:
            lift = (res["auc"] - 0.5) if res["auc"] is not None else None
            print(f"  {tag}: base={base:.1%} OOS_AUC={res['auc']:.3f}"
                  f"（0.5比 {lift:+.3f}） Brier={res['brier']:.4f} n_tr={res['n_train']:,} n_te={len(te):,}")
    print("  → OOS_AUC が安定して >0.55 なら『難レースを事前に見送れる』。≒0.5 なら見送り不能。")

    print("\n[② walk-forward 単変量 AUC（学習年で向き決定→評価年で測定）]")
    test_years = [t for _, t in splits]
    print(f"  {'特徴':<24}" + "".join(f"{y:>10}" for y in test_years))
    for f in feats:
        cells = []
        for train_years, test_y in splits:
            tr = df[df["year"].isin(train_years)]
            te = df[df["year"] == test_y]
            a = _oriented_auc(pd.to_numeric(tr[f], errors="coerce").fillna(0.0).tolist(),
                              tr["_neither"].astype(int).tolist(),
                              pd.to_numeric(te[f], errors="coerce").fillna(0.0).tolist(),
                              te["_neither"].astype(int).tolist())
            cells.append(f"{a:.3f}" if a is not None else "  -  ")
        print(f"  {f:<24}" + "".join(f"{c:>10}" for c in cells))
    print("  → 両評価年で安定して >0.55 の特徴があれば難レース識別の事前登録候補。全て≒0.5 なら不能。")
    print("\n※ これは難易度推定（見送り判断）であり勝者選択ではない。収益化前に、まず OOS で"
          "『予測できるか』のみを問う設計。条件化は事前登録し将来年度で検証。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
