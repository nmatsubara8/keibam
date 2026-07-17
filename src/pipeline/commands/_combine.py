"""build-combined: 分離学習した GBDT スタックと NN 単体を meta 融合して保存する。

分離NN + 遅延スタッキングの Phase 3。GBDT スタック（configs/base_models_gbdt.json で全データ学習）と
NN 単体（retrain --nn-standalone）を読み込み、holdout（--meta-years・両 base の学習に未使用）の予測で
meta 学習器を学習し、CombinedModel を effective_model として保存する。以後の予測・backtest は
この combined バージョンを指定すればそのまま使える（effective_model.predict_proba 契約）。
"""
from __future__ import annotations

import argparse
import logging

logger = logging.getLogger(__name__)


def _build_combined(args: argparse.Namespace) -> None:
    import pandas as pd

    from src.constants._local_paths import LocalPaths
    from src.pipeline._nn_standalone import load_nn_standalone
    from src.pipeline._retrain import evaluate_test
    from src.pipeline._retrain import version_name
    from src.training._combined_model import CombinedModel
    from src.training._combined_model import NnDerivedPredictor
    from src.training._data_splitter import DataSplitter
    from src.training._keiba_ai_factory import KeibaAIFactory

    # 1) 分離学習済みの2 base を読み込む
    gbdt_ai = KeibaAIFactory.load(args.gbdt_model)
    nn_model, nn_scaler = load_nn_standalone(args.nn_model)

    # 2) meta 融合用の holdout featured（両 base の学習に未使用の年を推奨: --meta-years）
    featured_path = args.featured_path or LocalPaths.FEATURED_DATA_PATH
    featured = pd.read_pickle(featured_path)
    if args.meta_years:
        yset = {str(y) for y in args.meta_years}
        rid = featured.index.astype(str)
        featured = featured[rid.str[:4].isin(yset)]
        logger.info("[build-combined] meta 用 holdout: %s 年 %d 行", sorted(yset), len(featured))
    if featured.empty:
        raise SystemExit("[build-combined] meta 用の holdout が空です（--meta-years を確認）。")

    ds = DataSplitter(featured, test_size=args.test_size, valid_size=args.valid_size, target_col="rank")

    # 3) CombinedModel（meta スタッキング）を holdout の train 側で学習
    base_predictors = [gbdt_ai.effective_model, NnDerivedPredictor(nn_model, nn_scaler)]
    combined = CombinedModel(base_predictors)
    combined.fit(ds.X_train, ds.y_train)

    # 4) 融合の効果を holdout の test 側で評価（参考: base 単体との比較）
    m_comb = evaluate_test(combined, ds.X_test, ds.y_test)
    m_gbdt = evaluate_test(gbdt_ai.effective_model, ds.X_test, ds.y_test)
    logger.info(
        "[build-combined] auc_test: combined=%.4f / gbdt単体=%.4f（Δ=%+.4f）",
        m_comb["auc_test"], m_gbdt["auc_test"], m_comb["auc_test"] - m_gbdt["auc_test"],
    )

    # 5) CombinedModel を effective_model として保存（gbdt_ai を器に再利用）。
    #    effective_model プロパティは _calibrated_model を返すのでそこへ差し込む。
    gbdt_ai._calibrated_model = combined
    vname = args.version_name or version_name()
    KeibaAIFactory.save(gbdt_ai, vname, suffix="__combined")
    logger.info("[build-combined] 保存: %s__combined（effective_model=CombinedModel）", vname)
