"""確信度（confidence）の算出。

期待値とは独立に「その馬券にどれだけ確信が持てるか」を [0,1] で表す複合スコア。
構成要素（モデル不確実性・オッズ不確実性・EVマージン）はシグナルとして外部から注入し、
このモジュールは合成ロジックのみを担う（疎結合・単一責務）。欠落シグナルは中立扱い。
"""

from __future__ import annotations

import dataclasses
import math
from abc import ABC
from abc import abstractmethod


@dataclasses.dataclass(frozen=True)
class ConfidenceSignals:
    """確信度の構成要素。None は「情報なし」を意味し合成から除外する。"""

    model_agreement: float | None = None  # base学習器間の一致度 [0,1]
    odds_certainty: float | None = None  # Layer2分位点の狭さ [0,1]
    ev_margin: float | None = None  # EV - 閾値（>0 で edge あり）


class AbstractConfidenceScorer(ABC):
    @abstractmethod
    def score(self, signals: ConfidenceSignals) -> float:
        raise NotImplementedError


def _clip01(x: float) -> float:
    return max(0.0, min(1.0, x))


def agreement_from_predictions(predictions, sensitivity: float = 4.0) -> float:
    """複数 base モデルの予測のばらつきから一致度 [0,1] を算出する。

    標準偏差が小さいほど 1 に近づく（exp(-sensitivity*std)）。
    """
    preds = list(predictions)
    if len(preds) < 2:
        return 1.0
    mean = sum(preds) / len(preds)
    var = sum((p - mean) ** 2 for p in preds) / len(preds)
    std = math.sqrt(var)
    return _clip01(math.exp(-sensitivity * std))


class CompositeConfidenceScorer(AbstractConfidenceScorer):
    """利用可能なシグナルの重み付き平均で確信度を合成する。

    Parameters
    ----------
    weights : {"model","odds","ev": 重み}。存在するシグナルのみで正規化する。
    ev_scale : EVマージンを [0,1) に圧縮する際のスケール（margin/(margin+ev_scale)）。
    """

    def __init__(self, weights: dict | None = None, ev_scale: float = 0.5) -> None:
        self._weights = weights or {"model": 1.0, "odds": 1.0, "ev": 1.0}
        self._ev_scale = ev_scale

    def score(self, signals: ConfidenceSignals) -> float:
        components: dict[str, float] = {}
        if signals.model_agreement is not None:
            components["model"] = _clip01(signals.model_agreement)
        if signals.odds_certainty is not None:
            components["odds"] = _clip01(signals.odds_certainty)
        if signals.ev_margin is not None:
            margin = max(0.0, signals.ev_margin)
            components["ev"] = margin / (margin + self._ev_scale)

        if not components:
            return 1.0  # 情報がなければ中立

        weight_sum = sum(self._weights.get(name, 0.0) for name in components)
        if weight_sum <= 0:
            return sum(components.values()) / len(components)
        return sum(self._weights.get(name, 0.0) * value for name, value in components.items()) / weight_sum
