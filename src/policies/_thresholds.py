"""EV 選定の馬券種別 閾値マップ（単一ソース）。

`BetThresholds`（constants・各券種の既定 EV 閾値）から {馬券種: 閾値} dict を作る。
simulation/_backtest と app/_prediction_service が同じマップを共有するためのヘルパ。
constants は他レイヤを import できない（flat 制約）ため、BetType と BetThresholds を束ねる
このロジックは policies 層に置く（simulation/app からは下方向で import 可能）。
"""

from __future__ import annotations

from src.constants._bet_thresholds import BetThresholds
from src.constants._bet_types import BetType


def bet_threshold_map(thresholds: BetThresholds | None = None) -> dict:
    """BetThresholds から {馬券種: EV 閾値} dict を作る。

    枠連（WAKUREN）は枠番ベースで Harville（馬番）が未対応のため対象外（含めると
    combo_probability が例外）。本番 run_prediction とバックテストの双方で同一のマップを使う。
    """
    th = thresholds or BetThresholds()
    return {
        BetType.TANSHO: th.TANSHO,
        BetType.FUKUSHO: th.FUKUSHO,
        BetType.UMAREN: th.UMAREN,
        BetType.UMATAN: th.UMATAN,
        BetType.WIDE: th.WIDE,
        BetType.SANRENPUKU: th.SANRENPUKU,
        BetType.SANRENTAN: th.SANRENTAN,
    }
