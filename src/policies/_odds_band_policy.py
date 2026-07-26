"""オッズ帯（人気）別の買い/見送りポリシー（純粋ロジック・OOS 検証前提）。

回収率は「オッズに対して過小評価された馬（正 EV）」に賭けることで上がるが、そのエッジは
人気帯に偏ることが多い（人気薄だけ、あるいは本命だけで成立する等）。本モジュールは
**回収率の良いオッズ帯だけ買う** ための帯選定を提供する。

過適合防止（本リポジトリの `_loss_minimization.require_oos_threshold` の思想に一致）:
- 帯の採用は必ず **学習期間 (train) と別期間 (val) の両方で** 回収率が閾値以上、かつ両方で
  的中数が信頼水準以上のものだけに限る。in-sample で回収率が高いだけの帯は採用しない。
- これにより「300% 等の見かけ利益を生む後付け最適化」を構造的に避ける。

レイヤ: policies（ドメイン）。I/O は save/load の JSON のみ。simulation はこれを import 可。
"""

from __future__ import annotations

import dataclasses
import json
import os
from typing import Sequence

from src.constants._bet_thresholds import MIN_BETS_FOR_RELIABLE_STAT

# オッズ帯（人気）別 ROI を切り分ける既定バケット（診断 walk_forward/kelly_backtest と統一）。
DEFAULT_ODDS_BANDS: tuple[tuple[float, float], ...] = (
    (1.0, 3.0), (3.0, 7.0), (7.0, 15.0), (15.0, 50.0), (50.0, float("inf")),
)


def odds_band_label(odds: float, bands: Sequence[tuple[float, float]] = DEFAULT_ODDS_BANDS) -> str | None:
    """オッズが属する帯のラベル（"3-7" / "50-∞"）を返す。どの帯にも入らなければ None。"""
    for lo, hi in bands:
        if lo <= odds < hi:
            hi_s = "∞" if hi == float("inf") else f"{hi:.0f}"
            return f"{lo:.0f}-{hi_s}"
    return None


def select_profitable_bands(
    train_by_band: dict,
    val_by_band: dict,
    bands: Sequence[tuple[float, float]] = DEFAULT_ODDS_BANDS,
    *,
    roi_floor: float = 1.0,
    min_hits: int = MIN_BETS_FOR_RELIABLE_STAT,
    use_roi_ex_top: bool = True,
) -> list[tuple[float, float]]:
    """train と val の帯別 ROI から、両期間で回収率が閾値以上の帯だけを採用する。

    Parameters
    ----------
    train_by_band, val_by_band : {帯ラベル: BetTypeStats}（roi / n_hits / roi_ex_top を持つ）。
    roi_floor : 採用する回収率の下限（既定 1.0＝プラス収支のみ。控除率環境では厳しめ）。
    min_hits : train/val 双方で要求する最小的中数（フロック 1 本での見かけ ROI を排除）。
    use_roi_ex_top : True なら「最大払戻 1 本除外後 ROI」も floor 以上を要求（万馬券依存を排除）。

    Returns
    -------
    採用する帯 (lo, hi) のリスト（bands の順序を保つ）。両期間で条件を満たす帯のみ。
    """
    allowed: list[tuple[float, float]] = []
    for band in bands:
        label = odds_band_label(band[0], bands)
        st = train_by_band.get(label)
        sv = val_by_band.get(label)
        if st is None or sv is None:
            continue
        if st.n_hits < min_hits or sv.n_hits < min_hits:
            continue  # 両期間で信頼水準の的中がある帯だけ
        if st.roi < roi_floor or sv.roi < roi_floor:
            continue  # 両期間で回収率 floor 以上（OOS で再現する帯だけ）
        if use_roi_ex_top and (st.roi_ex_top < roi_floor or sv.roi_ex_top < roi_floor):
            continue  # 最大払戻 1 本を除いても floor 以上（フロック依存を排除）
        allowed.append(band)
    return allowed


def filter_candidates_by_odds(candidates: Sequence, allowed_bands: Sequence[tuple[float, float]]) -> list:
    """候補のうち、オッズが allowed_bands のいずれかに入るものだけ残す。

    allowed_bands が空なら「採用帯なし＝買わない」（損失最小化の既定に一致）。
    """
    def _in_allowed(odds: float) -> bool:
        return any(lo <= odds < hi for lo, hi in allowed_bands)

    return [c for c in candidates if _in_allowed(float(getattr(c, "odds", 0.0) or 0.0))]


@dataclasses.dataclass(frozen=True)
class OddsBandPolicy:
    """採用オッズ帯のスナップショット（models/odds_band_policy.json に永続化）。"""

    allowed_bands: tuple[tuple[float, float], ...]
    roi_floor: float = 1.0
    train_years: tuple = ()
    val_years: tuple = ()
    created_at: str = ""

    def filter(self, candidates: Sequence) -> list:
        return filter_candidates_by_odds(candidates, self.allowed_bands)


def odds_band_policy_path(models_dir: str = "models") -> str:
    return os.path.join(models_dir, "odds_band_policy.json")


def _band_to_json(band: tuple[float, float]) -> list:
    lo, hi = band
    return [lo, None if hi == float("inf") else hi]  # inf は null で保存


def _band_from_json(pair: Sequence) -> tuple[float, float]:
    lo, hi = pair
    return (float(lo), float("inf") if hi is None else float(hi))


def save_odds_band_policy(policy: OddsBandPolicy, path: str) -> None:
    payload = {
        "allowed_bands": [_band_to_json(b) for b in policy.allowed_bands],
        "roi_floor": policy.roi_floor,
        "train_years": list(policy.train_years),
        "val_years": list(policy.val_years),
        "created_at": policy.created_at,
    }
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def load_odds_band_policy(path: str) -> OddsBandPolicy | None:
    """保存済みポリシーを読む（無ければ None）。"""
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        d = json.load(f)
    return OddsBandPolicy(
        allowed_bands=tuple(_band_from_json(b) for b in d.get("allowed_bands", [])),
        roi_floor=float(d.get("roi_floor", 1.0)),
        train_years=tuple(d.get("train_years", [])),
        val_years=tuple(d.get("val_years", [])),
        created_at=d.get("created_at", ""),
    )
