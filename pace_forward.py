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

    from src.simulation._pace_model import PACE_FEATURE_NAMES as PN
    idx = {name: i for i, name in enumerate(PN)}
    COND = [idx["field_size"], idx["course_len"], idx["is_dirt"]]          # レース条件のみ
    COMP = [idx["n_front"], idx["front_ratio"], idx["n_front_sq"], idx["ability_mean"],
            idx["ability_std"], idx["front_ability_max"], idx["back_ability_max"]]  # 隊列構成/力

    def _new_model():
        if args.model == "gbm":
            try:
                from sklearn.ensemble import HistGradientBoostingRegressor
                return HistGradientBoostingRegressor(max_depth=4, max_iter=200, learning_rate=0.05)
            except Exception:
                from sklearn.linear_model import Ridge
                return Ridge(alpha=1.0)
        from sklearn.linear_model import Ridge
        return Ridge(alpha=1.0)

    n = len(y)
    bounds = [round(i * n / args.folds) for i in range(args.folds + 1)]
    c_all, c_cond, c_comp, c_resid = [], [], [], []
    for k in range(1, args.folds):
        tr = slice(0, bounds[k]); te = slice(bounds[k], bounds[k + 1])
        if bounds[k + 1] - bounds[k] < 50:
            continue
        Xtr, ytr, Xte, yte = X[tr], y[tr], X[te], y[te]
        # 全部
        m = _new_model(); m.fit(Xtr, ytr); c_all.append(_corr(m.predict(Xte), yte))
        # 条件のみ
        mc = _new_model(); mc.fit(Xtr[:, COND], ytr); cond_pred_te = mc.predict(Xte[:, COND])
        c_cond.append(_corr(cond_pred_te, yte))
        # 構成のみ
        mp = _new_model(); mp.fit(Xtr[:, COMP], ytr); c_comp.append(_corr(mp.predict(Xte[:, COMP]), yte))
        # 条件を除いた残差を構成で説明できるか（＝駆け引きの純寄与）
        cond_pred_tr = mc.predict(Xtr[:, COND])
        mr = _new_model(); mr.fit(Xtr[:, COMP], ytr - cond_pred_tr)
        c_resid.append(_corr(mr.predict(Xte[:, COMP]), yte - cond_pred_te))

    a = float(np.nanmean(c_all)); cc = float(np.nanmean(c_cond))
    cp = float(np.nanmean(c_comp)); cr = float(np.nanmean(c_resid))
    print("-" * 64)
    print(f"OOS corr(予測, 実ペース):")
    print(f"  全部(条件+構成)         = {a:+.3f}")
    print(f"  条件のみ(距離/芝ダ/頭数) = {cc:+.3f}   ← 距離等で決まる平均的ペース形")
    print(f"  構成のみ(隊列/力)        = {cp:+.3f}   ← 駆け引き由来の候補")
    print(f"  条件除去後の残差を構成で = {cr:+.3f}   ★駆け引きの純寄与（条件と独立）")
    print("-" * 64)
    if cr > 0.2:
        print("→ 条件と独立に、隊列構成からペースの上振れ/下振れ(＝展開)が読める。sim注入の価値あり。")
    elif cr > 0.08:
        print("→ 駆け引きの純寄与は弱いが存在。効果は限定的。")
    else:
        print("→ ★駆け引きの純寄与 ≈ 0。ペース予測力はほぼ距離/条件由来で、同一条件内の展開は"
              "\n   発走前に読めない（＝騎手の駆け引き・当日要因が支配）。展開予測は原理的に困難。")


if __name__ == "__main__":
    main()
