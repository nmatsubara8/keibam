"""Layer1 学習のサンプル重み計算ユーティリティ（§2 EV境界重み付け / §2h レース内正規化）。

KB shard-43「期待値予測を用いた学習データの重み付けワークフロー」に対応。
EV = pred × odds が 1.0（損益分岐点）を超える領域に学習を集中させるため、
sigmoid 重み `1/(1+exp(-k*(EV-center)))` を計算する。さらに §2h により
レース内で重みの合計を 1 に正規化し、頭数の多寡による学習貢献度の不均衡を除く。

純粋な numpy 関数のみで構成し、LightGBM / NN いずれの base 学習器からも利用できる。
"""

from __future__ import annotations

import numpy as np

from src.constants._bet_thresholds import TrainingWeights


def ev_sigmoid_weights(
    pred: np.ndarray,
    odds: np.ndarray,
    k: float = TrainingWeights.SIGMOID_K,
    center: float = TrainingWeights.EV_CENTER,
) -> np.ndarray:
    """EV 境界 sigmoid 重みを計算する。

    EV_i = pred_i × odds_i、重み = 1/(1+exp(-k*(EV_i - center)))。
    EV が center を超えるほど 1 に、下回るほど 0 に近づく。

    Parameters
    ----------
    pred : 各サンプルの予測勝率（初期モデルのブートストラップ予測）。
    odds : 各サンプルの単勝オッズ（TANSHO_ODDS）。
    k : sigmoid の鋭さ。
    center : sigmoid の中心（EV の損益分岐点 = 1.0）。
    """
    pred_arr = np.asarray(pred, dtype=float)
    odds_arr = np.asarray(odds, dtype=float)
    ev = pred_arr * odds_arr
    # オーバーフロー防止のため exponent をクリップ
    z = np.clip(k * (ev - center), -500.0, 500.0)
    return 1.0 / (1.0 + np.exp(-z))


def normalize_within_race(weights: np.ndarray, race_ids) -> np.ndarray:
    """§2h: レース内（同一 race_id）で重みの合計が 1 になるよう正規化する。

    頭数 16 頭のレースと 6 頭のレースが等価な学習貢献度を持つようにする。
    1 レース内の重み合計が 0 の場合はそのレースを均等重み（1/頭数）にフォールバック。

    Parameters
    ----------
    weights : サンプルごとの重み（ev_sigmoid_weights の出力など）。
    race_ids : 各サンプルの race_id（weights と同じ長さの array-like）。
    """
    w = np.asarray(weights, dtype=float)
    race_arr = np.asarray(race_ids)
    if len(w) != len(race_arr):
        raise ValueError("weights と race_ids の長さが一致しません。")

    result = np.empty_like(w)
    for race in np.unique(race_arr):
        mask = race_arr == race
        group = w[mask]
        total = group.sum()
        if total > 0:
            result[mask] = group / total
        else:
            # 全重み 0 のレースは均等配分
            result[mask] = 1.0 / len(group)
    return result


def compute_ev_weights(
    pred: np.ndarray,
    odds: np.ndarray,
    race_ids,
    k: float = TrainingWeights.SIGMOID_K,
    center: float = TrainingWeights.EV_CENTER,
    normalize: bool = True,
) -> np.ndarray:
    """EV sigmoid 重みを計算し、必要ならレース内正規化まで一括で行う便利関数。"""
    weights = ev_sigmoid_weights(pred, odds, k=k, center=center)
    if normalize:
        weights = normalize_within_race(weights, race_ids)
    return weights
