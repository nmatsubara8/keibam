"""券種別の最適化パラメータ（DTO + 永続化）。

単勝勝率モデル + Harville を土台に、券種ごとに EV 選定の挙動を調整する
パラメータを管理する（独立 ML モデルではなく「最適化レイヤ」方式）。

調整対象:
- ``ev_threshold`` : EV（=的中確率×オッズ）の採用閾値。
- ``temperature``  : Harville に渡す勝率の指数 β。>1 で人気側を尖らせ（favorites 寄り）、
                     <1 で平坦化する。レース内の組合せ確率の相対分布を変える唯一の構造的ノブ。
- ``prob_scale``   : Harville 組合せ確率の較正係数（乗算）。ワイド（馬連で近似）など
                     既知の系統的バイアスを券種別に補正する。
- ``ev_max``       : EV 上限。超高倍率・極小確率の馬券を除外する（None=∞）。

レイヤ: policies（DTO + パラメータストア）。policy クラス本体（_bet_policy）は I/O を
持たず、本モジュールが読み込んだ params を DI で受け取る。永続化（models/ への JSON）は
上位レイヤ（app の最適化・pipeline のライブ選定）から呼ばれる薄い I/O ヘルパに留める。
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import logging
import math
import os
from typing import Mapping

from src.constants._bet_thresholds import BetThresholds
from src.constants._bet_types import BetType

logger = logging.getLogger(__name__)

BET_TYPE_PARAMS_FILENAME = "bet_type_params.json"

# 最適化対象の券種（枠連は Harville 非対応のため EV 最適化の対象外。
# 枠連は従来の score 閾値 BOX でのみ扱う）。
OPTIMIZABLE_BET_TYPES = (
    BetType.TANSHO,
    BetType.FUKUSHO,
    BetType.UMAREN,
    BetType.UMATAN,
    BetType.WIDE,
    BetType.SANRENPUKU,
    BetType.SANRENTAN,
)


@dataclasses.dataclass(frozen=True)
class BetTypeParams:
    """1 券種の EV 選定パラメータ（frozen DTO）。"""

    ev_threshold: float = 1.0
    temperature: float = 1.0
    prob_scale: float = 1.0
    ev_max: float = math.inf

    def to_dict(self) -> dict:
        # JSON は inf を表現できないため ev_max=∞ は None で保存する。
        return {
            "ev_threshold": self.ev_threshold,
            "temperature": self.temperature,
            "prob_scale": self.prob_scale,
            "ev_max": None if math.isinf(self.ev_max) else self.ev_max,
        }

    @classmethod
    def from_dict(cls, raw: Mapping) -> "BetTypeParams":
        ev_max_raw = raw.get("ev_max")
        ev_max = math.inf if ev_max_raw is None else float(ev_max_raw)
        return cls(
            ev_threshold=float(raw.get("ev_threshold", 1.0)),
            temperature=float(raw.get("temperature", 1.0)),
            prob_scale=float(raw.get("prob_scale", 1.0)),
            ev_max=ev_max,
        )


def default_params(bet_type: str) -> BetTypeParams:
    """券種の既定パラメータ（EV 閾値は BetThresholds 由来、他は中立値）。"""
    th = BetThresholds()
    ev_threshold = {
        BetType.TANSHO: th.TANSHO,
        BetType.FUKUSHO: th.FUKUSHO,
        BetType.UMAREN: th.UMAREN,
        BetType.UMATAN: th.UMATAN,
        BetType.WIDE: th.WIDE,
        BetType.SANRENPUKU: th.SANRENPUKU,
        BetType.SANRENTAN: th.SANRENTAN,
    }.get(bet_type, 1.0)
    return BetTypeParams(ev_threshold=ev_threshold)


def default_params_set() -> dict[str, BetTypeParams]:
    """最適化対象の全券種の既定パラメータ集合。"""
    return {bt: default_params(bt) for bt in OPTIMIZABLE_BET_TYPES}


def params_for(bet_type: str, params_map: Mapping[str, BetTypeParams] | None) -> BetTypeParams:
    """券種のパラメータを取り出す。未登録・None は既定値にフォールバックする。"""
    if params_map and bet_type in params_map:
        return params_map[bet_type]
    return default_params(bet_type)


def apply_temperature(win_probs: Mapping[int, float], temperature: float) -> dict[int, float]:
    """勝率に温度（指数 β）を適用する（純粋関数）。正規化は Harville 側が行う。

    temperature==1.0 はコピーを返す（恒等）。β>1 で大きい勝率がより強調される。
    """
    if temperature == 1.0:
        return dict(win_probs)
    return {u: float(p) ** float(temperature) for u, p in win_probs.items()}


# ---------------------------------------------------------------------------
# 永続化（models/bet_type_params.json）— バージョン管理
# ---------------------------------------------------------------------------


def bet_type_params_path(models_dir: str = "models") -> str:
    return os.path.join(models_dir, BET_TYPE_PARAMS_FILENAME)


def save_bet_type_params(
    params_map: Mapping[str, BetTypeParams],
    path: str,
    *,
    objective: str | None = None,
    metrics: Mapping[str, dict] | None = None,
) -> None:
    """券種別パラメータのスナップショットを JSON 追記保存する（同日付は置換）。

    1 スナップショット = {saved_at, objective, params:{券種:...}, metrics:{券種:...}}。
    ファイルはスナップショットの履歴（リスト）で、最新は saved_at が最大のもの。
    """
    now = dt.datetime.now().isoformat()
    day = now[:10]
    existing = [r for r in load_bet_type_params_records(path) if r.get("saved_at", "")[:10] != day]
    snapshot = {
        "saved_at": now,
        "objective": objective,
        "params": {bt: p.to_dict() for bt, p in params_map.items()},
        "metrics": {bt: dict(m) for bt, m in (metrics or {}).items()},
    }
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(existing + [snapshot], f, ensure_ascii=False, indent=2)
    logger.info("[bet_type_params] %s: %d 券種を保存", path, len(params_map))


def load_bet_type_params_records(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return json.load(f)


def latest_bet_type_params(path: str) -> dict[str, BetTypeParams]:
    """保存済みの最新スナップショットから {券種: BetTypeParams} を返す（無ければ空）。"""
    records = load_bet_type_params_records(path)
    if not records:
        return {}
    latest = max(records, key=lambda r: r.get("saved_at", ""))
    return {bt: BetTypeParams.from_dict(p) for bt, p in latest.get("params", {}).items()}
