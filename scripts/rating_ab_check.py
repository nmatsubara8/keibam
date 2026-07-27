"""Phase 2-5 レーティング特徴量の On/Off A/B（隔離検証ハーネス）。

main の core（data_merger / feature_engineering / retrain）を一切変更せず、
featured_data から Phase 2-5（TrueSkill / 条件別 TrueSkill / 能力 Kalman /
階層ベイズ）の as-of レーティング特徴量を生成して featured に付与し、有無 2 条件で
LightGBM を学習して test AUC / logloss を比較する自己完結スクリプト。

背景:
    Elo（Phase 1）は main に統合済みで、実データ A/B（2026-07-15・434,655 行）の結果
    「冗長（AUC ±0.001）」と判明済み。本スクリプトは未統合の Phase 2-5 が、その Elo
    冗長ベースラインを超えるエッジ（予測精度改善）を持つかを確かめるためのもの。
    改善が確認できて初めて production 本配線（data_merger 等）を検討する。

    featured_data が既に elo_* 列を含む場合、本 A/B は「Elo ありベースライン」に対する
    Phase 2-5 の**限界的**寄与を測る（＝Elo に上乗せする価値があるか）。

使い方:
    python scripts/rating_ab_check.py
    python scripts/rating_ab_check.py --featured data/featured/featured.parquet \
        --target rank_win --since-year 2016
    python scripts/rating_ab_check.py --limit-races 3000   # スモーク

出力:
    ベースライン（featured そのまま）と +Phase25（featured + 24 レーティング列）の
    test AUC / logloss、その差分（ΔAUC）、および各ファミリーの被覆率（prior でない
    行の割合）を表示する。ΔAUC が Elo の ±0.001 を実質的に超えるかで判断する。
"""

from __future__ import annotations

import argparse
import logging
import sys

logger = logging.getLogger("rating_ab_check")

# 各ファミリの純粋関数が入力に要求する列（存在しないと prior 退化 or エラー）。
_REQUIRED_COLS = ["horse_id", "date", "着順"]  # ResultsCols.RANK == '着順'


def _load_featured(path: str):
    """featured_data を parquet（dtype 復元）または pickle でロードする。"""
    import os

    import pandas as pd

    if path.endswith(".parquet"):
        from src.storage._featured import load_parquet

        return load_parquet(path)
    if os.path.exists(path):
        return pd.read_pickle(path)
    raise FileNotFoundError(f"featured が見つかりません: {path}")


def build_phase25_features(featured):
    """featured から Phase 2-5 の as-of レーティング特徴量を生成する（純粋・リーク無し）。

    Returns
    -------
    (features, coverage) :
        features : PHASE25_RATING_FEATURE_COLS を列に持つ DataFrame（featured と同じ
            行順・行数。位置対応で featured に付与できる）。
        coverage : {family: 非 prior 行の割合} の被覆率レポート。
    """
    import numpy as np
    import pandas as pd

    from src.preprocessing._ability_kalman import compute_ability_kalman_history
    from src.preprocessing._conditional_trueskill import compute_conditional_trueskill_history
    from src.preprocessing._hier_bayes_trueskill import compute_hier_bayes_history
    from src.preprocessing._trueskill import compute_trueskill_history

    # Phase 2: TrueSkill（他ファミリの前提 = ts_mu/ts_sigma/ts_field_mean を供給）
    ts, _ = compute_trueskill_history(featured)
    # Phase 3: 条件別 TrueSkill（条件列が無い次元は prior に退化）
    cond, _ = compute_conditional_trueskill_history(featured)
    # Phase 4: 能力 Kalman（ts_field_mean があれば観測水準に使う）
    kf_input = featured.copy()
    kf_input["ts_field_mean"] = ts["ts_field_mean"].to_numpy()
    kf, _ = compute_ability_kalman_history(kf_input)
    # Phase 5: 階層ベイズ（ts_mu/ts_sigma を個体尤度に、単勝/peds_0 を事前に使う）
    hb_input = featured.copy()
    hb_input["ts_mu"] = ts["ts_mu"].to_numpy()
    hb_input["ts_sigma"] = ts["ts_sigma"].to_numpy()
    hb, _ = compute_hier_bayes_history(hb_input)

    feats = pd.concat(
        [df.reset_index(drop=True) for df in (ts, cond, kf, hb)], axis=1
    )

    # 被覆率: それまでの出走数 > 0（＝ prior でない as-of 値を持つ）行の割合。
    def _frac(col):
        if col not in feats.columns:
            return float("nan")
        return float(np.mean(pd.to_numeric(feats[col], errors="coerce").fillna(0) > 0))

    coverage = {
        "trueskill (ts_n_races>0)": _frac("ts_n_races"),
        "conditional (ts_surface_n_races>0)": _frac("ts_surface_n_races"),
        "kalman (kf_workload>0)": _frac("kf_workload"),
        "hier_bayes (hb_shrinkage<1)": (
            float(np.mean(pd.to_numeric(feats.get("hb_shrinkage", 1.0), errors="coerce").fillna(1.0) < 1.0))
            if "hb_shrinkage" in feats.columns else float("nan")
        ),
    }
    return feats, coverage


def _metrics(model, x_test, y_test):
    """test AUC と logloss を返す（evaluate_test と同じく単勝列は除外）。"""
    import numpy as np
    from sklearn.metrics import log_loss
    from sklearn.metrics import roc_auc_score

    from src.constants._results_cols import ResultsCols

    x = x_test.drop([ResultsCols.TANSHO_ODDS], axis=1, errors="ignore")
    proba = np.asarray(model.predict_proba(x))[:, 1]
    y = np.asarray(y_test)
    return {
        "auc": float(roc_auc_score(y, proba)),
        "logloss": float(log_loss(y, proba, labels=[0, 1])),
    }


def _train_eval(featured, target_col, test_size, valid_size):
    """featured を DataSplitter で分割・LightGBM 学習し test メトリクスを返す。"""
    from src.pipeline._retrain import evaluate_test  # noqa: F401  (存在確認・整合の明示)
    from src.training._keiba_ai_factory import KeibaAIFactory

    ai = KeibaAIFactory.create(
        featured, test_size=test_size, valid_size=valid_size, target_col=target_col
    )
    ai.train_without_tuning()
    return _metrics(ai.effective_model, ai.datasets.X_test, ai.datasets.y_test)


def run_ab(featured, *, target_col, test_size, valid_size):
    """ベースライン vs +Phase25 の A/B を実行し、両メトリクスと被覆率・ΔAUC を返す。"""
    baseline = _train_eval(featured, target_col, test_size, valid_size)

    feats, coverage = build_phase25_features(featured)
    treated = featured.copy()
    for col in feats.columns:
        treated[col] = feats[col].to_numpy()  # 位置対応で付与（race_id 重複に安全）
    treatment = _train_eval(treated, target_col, test_size, valid_size)

    return {
        "baseline": baseline,
        "treatment": treatment,
        "delta_auc": treatment["auc"] - baseline["auc"],
        "delta_logloss": treatment["logloss"] - baseline["logloss"],
        "coverage": coverage,
        "n_added_cols": len(feats.columns),
        "elo_present": any(c.startswith("elo_") for c in featured.columns),
    }


def _filter(featured, since_year, limit_races):
    """--since-year / --limit-races でサンプルを絞る（date 列基準）。"""
    import pandas as pd

    df = featured
    if since_year is not None and "date" in df.columns:
        years = pd.to_datetime(df["date"], errors="coerce").dt.year
        df = df[years >= since_year]
    if limit_races is not None:
        keep = pd.Index(df.index.unique())[:limit_races]
        df = df[df.index.isin(keep)]
    return df


def _parse_args(argv):
    from src.constants._local_paths import LocalPaths

    p = argparse.ArgumentParser(description="Phase 2-5 レーティング特徴量の On/Off A/B（隔離検証）")
    p.add_argument("--featured", default=getattr(LocalPaths, "FEATURED_DATA_PATH", "data/featured/featured.parquet"),
                   help="featured_data のパス（.parquet or .pkl）")
    p.add_argument("--target", default="rank", choices=("rank", "rank_win"),
                   help="目的変数。rank=複勝(top3) / rank_win=単勝(1着)")
    p.add_argument("--since-year", type=int, default=None, help="この年以降のレースに限定")
    p.add_argument("--limit-races", type=int, default=None, help="先頭 N レースに限定（スモーク用）")
    p.add_argument("--test-size", type=float, default=0.3)
    p.add_argument("--valid-size", type=float, default=0.3)
    return p.parse_args(argv)


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = _parse_args(argv if argv is not None else sys.argv[1:])

    logger.info("featured をロード: %s", args.featured)
    featured = _load_featured(args.featured)
    featured = _filter(featured, args.since_year, args.limit_races)

    missing = [c for c in _REQUIRED_COLS if c not in featured.columns]
    if missing:
        logger.error("featured に必須列 %s がありません。レーティング生成不可。", missing)
        return 2

    n_races = int(featured.index.nunique())
    logger.info("A/B 開始: rows=%d races=%d target=%s", len(featured), n_races, args.target)

    result = run_ab(featured, target_col=args.target, test_size=args.test_size, valid_size=args.valid_size)

    b, t = result["baseline"], result["treatment"]
    print("\n" + "=" * 68)
    print(f" Phase 2-5 レーティング A/B  (target={args.target}, races={n_races})")
    print(f" featured に Elo 列あり: {result['elo_present']}（True なら Elo 上乗せの限界寄与を測定）")
    print("=" * 68)
    print(f" {'条件':<18}{'AUC':>12}{'logloss':>12}")
    print(f" {'ベースライン':<18}{b['auc']:>12.4f}{b['logloss']:>12.4f}")
    print(f" {'+Phase25 (' + str(result['n_added_cols']) + '列)':<18}{t['auc']:>12.4f}{t['logloss']:>12.4f}")
    print("-" * 68)
    print(f" ΔAUC     = {result['delta_auc']:+.4f}   (Elo ベースラインの基準は ±0.001)")
    print(f" Δlogloss = {result['delta_logloss']:+.4f}   (負=改善)")
    print("-" * 68)
    print(" 被覆率（as-of の非 prior 行の割合）:")
    for k, v in result["coverage"].items():
        print(f"   {k:<36}{v:>8.3f}")
    print("=" * 68)
    verdict = (
        "改善の兆候あり → 本配線を検討する価値" if result["delta_auc"] > 0.001
        else "Elo 同様に冗長（±0.001 以内）→ 本配線は非推奨"
    )
    print(f" 判定: ΔAUC={result['delta_auc']:+.4f} → {verdict}")
    print("=" * 68 + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
