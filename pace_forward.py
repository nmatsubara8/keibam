"""ペースは発走前に予測できるか？を前進検証する。

発走前特徴（隊列構成・力・条件）から実ペース(pace_diff, laptime由来)を回帰し、未来foldで
OOS 相関を測る。sim が使った素朴仮定「先行率→ペース」の相関も並べ、駆け引きを含めて正しく
学べばペースが読めるのか、それとも本質的に事前予測不能かを判定する。

判定:
- モデル OOS corr が有意に正（例 >0.25） → ペースは事前予測可能。sim にペース外挿として注入でき、
  忠実度(1)を改善できる見込み。
- モデル OOS corr ≈ 0 → 隊列や力からはペースを読めない＝**発走前にペースは本質的に不確実**
  （同日の駆け引き/騎手判断が支配）。展開予測は原理的に困難、と確定。

実行例: python pace_forward.py --folds 6 --limit 60000 --max-year 2021
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))


def _corr(a, b):
    import numpy as np
    a = np.asarray(a, float); b = np.asarray(b, float)
    m = np.isfinite(a) & np.isfinite(b)
    if m.sum() < 3 or a[m].std() == 0 or b[m].std() == 0:
        return float("nan")
    return float(np.corrcoef(a[m], b[m])[0, 1])


def main():
    ap = argparse.ArgumentParser(description="ペースの事前予測可能性を前進検証")
    ap.add_argument("--folds", type=int, default=6)
    ap.add_argument("--limit", type=int, default=60000, help="直近Nレース")
    ap.add_argument("--max-year", type=int, default=2021)
    ap.add_argument("--model", choices=["gbm", "ridge"], default="gbm")
    args = ap.parse_args()

    import numpy as np
    import pandas as pd

    from app._model_eval import load_featured_data
    from src.constants._local_paths import LocalPaths
    from src.simulation._pace_model import PACE_FEATURE_NAMES, features_to_row, pace_features

    featured = load_featured_data()
    if featured is None or featured.empty:
        print("featured_data がありません")
        return
    pace_path = Path(LocalPaths.RAW_DIR) / "race_pace.pkl"
    if not pace_path.exists():
        print(f"{pace_path} が無い。import_archive_laptime.py で作成してください。")
        return
    rp = pd.read_pickle(pace_path)
    pace = dict(zip(rp["race_id"].astype(str), pd.to_numeric(rp["pace_diff"], errors="coerce")))

    date = pd.to_datetime(featured["date"]).groupby(level=0).first().sort_values()
    order = [r for r in date.index
             if (not args.max_year or (str(r)[:4].isdigit() and int(str(r)[:4]) <= args.max_year))
             and np.isfinite(pace.get(str(r), np.nan))]
    if args.limit and len(order) > args.limit:
        order = order[-args.limit:]

    print(f"ペース事前予測 前進検証 / {len(order):,}レース / model={args.model}")
    print("特徴量:", ", ".join(PACE_FEATURE_NAMES))

    # 特徴行列を構築（1レース1行）
    X, y, fr = [], [], []
    for rid in order:
        rd = featured.loc[[rid]] if not isinstance(featured.loc[rid], pd.DataFrame) else featured.loc[rid]
        if len(rd) < 4:
            continue
        feat = pace_features(rd)
        X.append(features_to_row(feat)); y.append(pace[str(rid)]); fr.append(feat["front_ratio"])
    X = np.array(X, float); y = np.array(y, float); fr = np.array(fr, float)
    print(f"有効レース: {len(y):,}")

    n = len(y)
    bounds = [round(i * n / args.folds) for i in range(args.folds + 1)]
    model_corrs, naive_corrs, base_rmse, model_rmse = [], [], [], []
    for k in range(1, args.folds):
        tr = slice(0, bounds[k]); te = slice(bounds[k], bounds[k + 1])
        if bounds[k + 1] - bounds[k] < 50:
            continue
        Xtr, ytr, Xte, yte = X[tr], y[tr], X[te], y[te]
        if args.model == "gbm":
            try:
                from sklearn.ensemble import HistGradientBoostingRegressor
                m = HistGradientBoostingRegressor(max_depth=4, max_iter=200, learning_rate=0.05)
            except Exception:
                from sklearn.linear_model import Ridge
                m = Ridge(alpha=1.0)
        else:
            from sklearn.linear_model import Ridge
            m = Ridge(alpha=1.0)
        m.fit(Xtr, ytr)
        pred = m.predict(Xte)
        model_corrs.append(_corr(pred, yte))
        naive_corrs.append(_corr(fr[te], yte))            # sim の素朴仮定 先行率→ペース
        base_rmse.append(float(np.sqrt(np.mean((yte - ytr.mean()) ** 2))))
        model_rmse.append(float(np.sqrt(np.mean((yte - pred) ** 2))))

    mc = float(np.nanmean(model_corrs)); nc = float(np.nanmean(naive_corrs))
    print("-" * 60)
    print(f"モデル OOS corr(予測, 実ペース)   = {mc:+.3f}")
    print(f"素朴 OOS corr(先行率, 実ペース)   = {nc:+.3f}   ← sim が使っていた仮定")
    print(f"RMSE  baseline(平均)={np.nanmean(base_rmse):.3f}  model={np.nanmean(model_rmse):.3f}")
    print("-" * 60)
    if mc > 0.25:
        print("→ ペースは発走前に有意に予測可能。sim にペース外挿として注入する価値あり。")
    elif mc > 0.1:
        print("→ 弱いが予測可能。改善余地はあるが効果は限定的。")
    else:
        print("→ 隊列・力からペースはほぼ読めない＝発走前にペースは本質的に不確実（同日の駆け引きが支配）。"
              "\n   展開予測は原理的に困難、と確定。")


if __name__ == "__main__":
    main()
