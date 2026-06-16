"""ラベルシャッフル（permutation）によるデータリーク経験的検査。

目的変数 y_train をランダムに並べ替えて学習し、本物の y_test に対する AUC を測る。
特徴量と目的変数の正当な関係が壊れているため、リークが無ければ AUC は 0.5（偶然）
付近に落ちる。シャッフルしても AUC が高いままなら、train/test 汚染・行の重複・
特徴量への目的変数混入など「学習以外の経路で正解が漏れている」兆候となる。

設計: 純粋ロジック（I/O なし）。X/y と「真っさらなモデルを返す factory」を引数で受け、
featured_data の読込や分割は呼出側（scripts/leakage_check.py）が行う。

レイヤ: training（lightgbm / sklearn.metrics に依存可）。
"""

from __future__ import annotations

import dataclasses
import logging
from typing import Any, Callable, Optional, Sequence

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

logger = logging.getLogger(__name__)

# シャッフル AUC がこの値を超えたらリークを疑う（偶然 0.5 からの許容上振れ）。
DEFAULT_SUSPECT_THRESHOLD = 0.6

# 呼ぶたびに未学習の分類器（fit / predict_proba を持つ）を返す factory。
ModelFactory = Callable[[], Any]


@dataclasses.dataclass(frozen=True)
class ShuffleResult:
    baseline_auc: float
    shuffled_aucs: list[float]
    shuffled_mean: float
    shuffled_std: float
    n_trials: int
    suspect_threshold: float

    @property
    def verdict(self) -> str:
        """PASS: シャッフルで偶然付近まで落ちた / SUSPECT: 落ちない（リーク疑い）。"""
        return "PASS" if self.shuffled_mean <= self.suspect_threshold else "SUSPECT"

    @property
    def gap(self) -> float:
        """本物 AUC とシャッフル平均 AUC の差（大きいほど健全＝学習が本物に依存）。"""
        return self.baseline_auc - self.shuffled_mean


def _default_model_factory() -> object:
    """リポジトリの実モデルに準じた軽量 LightGBM 分類器（高速・決定的寄り）。"""
    import lightgbm as lgb

    from src.constants._bet_thresholds import TrainingWeights

    return lgb.LGBMClassifier(
        n_estimators=200,
        learning_rate=0.05,
        num_leaves=31,
        scale_pos_weight=TrainingWeights.SCALE_POS_WEIGHT,
        objective="binary",
        n_jobs=-1,
        verbose=-1,
    )


def _fit_predict_auc(
    model_factory: ModelFactory,
    X_train: pd.DataFrame,
    y_train: Any,
    X_test: pd.DataFrame,
    y_test: Any,
) -> float:
    """真っさらなモデルを学習し、X_test 予測の AUC を返す。"""
    model = model_factory()
    model.fit(X_train, np.asarray(y_train))
    proba = np.asarray(model.predict_proba(X_test))[:, 1]
    return float(roc_auc_score(np.asarray(y_test), proba))


def label_shuffle_test(
    X_train: pd.DataFrame,
    y_train: Sequence,
    X_test: pd.DataFrame,
    y_test: Sequence,
    *,
    model_factory: Optional[ModelFactory] = None,
    n_trials: int = 5,
    rng: Optional[np.random.Generator] = None,
    suspect_threshold: float = DEFAULT_SUSPECT_THRESHOLD,
) -> ShuffleResult:
    """ラベルシャッフル試験を実行する。

    Parameters
    ----------
    X_train, y_train, X_test, y_test : 時系列分割済みの学習/評価データ。
        X は学習に使う特徴量のみ（目的変数・着順・オッズ・date 等は事前に除外しておく）。
    model_factory : 呼ぶたびに未学習モデルを返す callable（既定: 軽量 LightGBM）。
    n_trials : シャッフルの試行回数（平均と分散を取る）。
    rng : 乱数生成器（再現性のため注入可能）。
    suspect_threshold : シャッフル平均 AUC がこれを超えたら SUSPECT 判定。

    Returns
    -------
    ShuffleResult : baseline_auc / shuffled_aucs / verdict 等。
    """
    if rng is None:
        rng = np.random.default_rng()
    if model_factory is None:
        model_factory = _default_model_factory

    y_train_arr = np.asarray(y_train)

    baseline_auc = _fit_predict_auc(model_factory, X_train, y_train_arr, X_test, y_test)
    logger.info("[leakage] baseline AUC (本物ラベル) = %.4f", baseline_auc)

    shuffled_aucs: list[float] = []
    for i in range(n_trials):
        y_shuffled = rng.permutation(y_train_arr)
        auc = _fit_predict_auc(model_factory, X_train, y_shuffled, X_test, y_test)
        shuffled_aucs.append(auc)
        logger.info("[leakage] shuffled AUC trial %d/%d = %.4f", i + 1, n_trials, auc)

    shuffled_mean = float(np.mean(shuffled_aucs))
    shuffled_std = float(np.std(shuffled_aucs))

    result = ShuffleResult(
        baseline_auc=round(baseline_auc, 4),
        shuffled_aucs=[round(a, 4) for a in shuffled_aucs],
        shuffled_mean=round(shuffled_mean, 4),
        shuffled_std=round(shuffled_std, 4),
        n_trials=n_trials,
        suspect_threshold=suspect_threshold,
    )
    logger.info(
        "[leakage] verdict=%s baseline=%.4f shuffled_mean=%.4f (gap=%.4f)",
        result.verdict, result.baseline_auc, result.shuffled_mean, result.gap,
    )
    return result
