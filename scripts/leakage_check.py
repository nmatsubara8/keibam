"""データリークの経験的検査（ラベルシャッフル試験）。

featured_data を時系列分割（DataSplitter）し、本物ラベルとシャッフルラベルで
それぞれ LightGBM を学習して test AUC を比較する。

- 本物ラベル AUC は高く、シャッフル AUC が ~0.5（偶然）まで落ちれば健全（PASS）。
- シャッフルしても AUC が高い（SUSPECT）なら、train/test 汚染・特徴量への目的変数
  混入など「学習以外の経路で正解が漏れている」兆候。

使い方:
    python scripts/leakage_check.py                 # 既定 featured_data・5 試行
    python scripts/leakage_check.py --n-trials 10 --seed 42
    python scripts/leakage_check.py --featured path/to/featured_data.pkl
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.constants._local_paths import LocalPaths
from src.constants._results_cols import ResultsCols
from src.training._data_splitter import DataSplitter
from src.training._leakage_check import label_shuffle_test

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="ラベルシャッフルによるデータリーク検査")
    parser.add_argument("--featured", default=LocalPaths.FEATURED_DATA_PATH,
                        help="featured_data.pkl のパス")
    parser.add_argument("--n-trials", type=int, default=5, help="シャッフル試行回数")
    parser.add_argument("--seed", type=int, default=0, help="乱数シード（再現性）")
    parser.add_argument("--test-size", type=float, default=0.2, help="テスト割合（時系列後方）")
    parser.add_argument("--suspect-threshold", type=float, default=0.6,
                        help="シャッフル平均 AUC がこれを超えたら SUSPECT 判定")
    args = parser.parse_args()

    if not Path(args.featured).exists():
        logger.error("featured_data がありません: %s", args.featured)
        logger.error("先に取込・特徴量生成（retrain/ingest）を実行してください。")
        sys.exit(2)

    logger.info("featured_data 読込: %s", args.featured)
    featured = pd.read_pickle(args.featured)

    # 実パイプラインと同じ時系列分割。X_train は学習用に着順/rank/date/単勝を除外済み。
    ds = DataSplitter(featured, test_size=args.test_size, valid_size=0.2)
    X_train, y_train = ds.X_train, ds.y_train
    # X_test は EV 計算用に単勝を残しているため、学習特徴量に合わせて除外する。
    X_test = ds.X_test.drop([ResultsCols.TANSHO_ODDS], axis=1, errors="ignore")
    y_test = ds.y_test

    logger.info("train=%d rows / test=%d rows / features=%d",
                len(X_train), len(X_test), X_train.shape[1])

    result = label_shuffle_test(
        X_train, y_train, X_test, y_test,
        n_trials=args.n_trials,
        rng=np.random.default_rng(args.seed),
        suspect_threshold=args.suspect_threshold,
    )

    print("\n" + "=" * 56)
    print("  ラベルシャッフル・データリーク検査")
    print("=" * 56)
    print(f"  本物ラベル AUC          : {result.baseline_auc:.4f}")
    print(f"  シャッフル AUC（平均）  : {result.shuffled_mean:.4f} ± {result.shuffled_std:.4f}")
    print(f"  各試行                  : {result.shuffled_aucs}")
    print(f"  乖離 (baseline - shuf)  : {result.gap:.4f}")
    print(f"  判定閾値                : {result.suspect_threshold}")
    print("-" * 56)
    if result.verdict == "PASS":
        print("  ✅ PASS: シャッフルで AUC が偶然付近まで低下。")
        print("     学習以外の経路で正解が漏れている兆候はありません。")
    else:
        print("  ⚠️ SUSPECT: シャッフルしても AUC が高いままです。")
        print("     train/test 汚染・特徴量への目的変数混入などを確認してください。")
    print("=" * 56 + "\n")

    sys.exit(0 if result.verdict == "PASS" else 1)


if __name__ == "__main__":
    main()
