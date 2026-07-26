"""Model 1: `ManjiScorer` — 卍式の加点減点を統制する透明加算モデル。

score(h) = Σ_f  w_f · points[f][ bucket_f(h) ]

- points[f][bucket] は Model 2（回収率較正）が学習期間から決める符号付き点数（既定0）。
- w_f は因子重み（既定1、0で実質OFF）。
- 得点の上に卍さんの3ゲートを重ねる:
    (1) ゾーンゲート : score∈[s_lo,s_hi] かつ odds∈[o_lo,o_hi] かつ race内score順位≤top_k
    (2) レース選別  : max(score) < race_min_score なら不買（高得点馬不在レース除外）
    (3) サイジング  : stake = 残高 × κ ÷ odds（100円未満は不買＝大穴の機械排除＋追い下げ）

win予測ではなく回収率で符号が決まる点が本質。KeibaAI に一切依存しない純関数群。
"""
from __future__ import annotations

import dataclasses
from typing import Mapping

import numpy as np
import pandas as pd

from src.constants._results_cols import ResultsCols
from src.policies._manji_factors import buckets


@dataclasses.dataclass(frozen=True)
class ManjiScorerConfig:
    """ManjiScorer のパラメータ一式（Model 2 が最適化する対象）。"""
    points: Mapping[str, Mapping[str, float]]     # factor -> {bucket: 点数}
    weights: Mapping[str, float] = dataclasses.field(default_factory=dict)  # factor -> 重み（既定1）
    zone_odds: tuple[float, float] = (3.0, 50.0)  # 買うオッズ帯（低配当/高配当の隅を除外）
    zone_score: tuple[float, float] = (-np.inf, np.inf)  # 買う得点帯
    top_k: int = 3                                # race内 得点順位の上限（人気しすぎ回避の相方）
    race_min_score: float = -np.inf              # 高得点馬不在レースの除外閾値
    sizing_kappa: float = 0.08                   # stake = 残高×κ÷odds（卍: 0.08）
    min_stake: float = 100.0                     # これ未満は不買（大穴排除）

    def factor_names(self) -> list[str]:
        return list(self.points)


class ManjiScorer:
    """設定 (ManjiScorerConfig) に従って featured バッチを採点し、買い目を選ぶ。"""

    def __init__(self, config: ManjiScorerConfig):
        self.config = config

    # --- 採点 --------------------------------------------------------------
    def score(self, df: pd.DataFrame) -> pd.Series:
        """行ごとの合計得点 Σ w_f · points[f][bucket] を返す（index=df.index）。"""
        names = self.config.factor_names()
        if not names:
            return pd.Series(0.0, index=df.index)
        bk = buckets(df, names)
        total = pd.Series(0.0, index=df.index)
        for f in names:
            w = float(self.config.weights.get(f, 1.0))
            if w == 0.0:
                continue
            pmap = self.config.points.get(f, {})
            pts = bk[f].map(lambda b, pmap=pmap: float(pmap.get(b, 0.0)))
            total = total + w * pts.astype(float)
        return total

    # --- 買い目選択（3ゲート） --------------------------------------------
    def select(self, df: pd.DataFrame, odds_col: str = ResultsCols.TANSHO_ODDS) -> pd.DataFrame:
        """買い目を (race_id, umaban, odds, score) の DataFrame で返す。

        3ゲート（ゾーン/レース選別/得点順位）を適用。サイジングは stake() で別途。
        """
        cfg = self.config
        s = self.score(df)
        odds = pd.to_numeric(df[odds_col], errors="coerce")
        uma = pd.to_numeric(df[ResultsCols.UMABAN], errors="coerce")
        work = pd.DataFrame(
            {"score": s.to_numpy(), "odds": odds.to_numpy(), "umaban": uma.to_numpy()},
            index=df.index,
        )
        # レース内 得点降順順位（1=最高得点）と レース最高得点
        work["rank"] = work.groupby(level=0)["score"].rank(ascending=False, method="min")
        work["race_max"] = work.groupby(level=0)["score"].transform("max")

        o_lo, o_hi = cfg.zone_odds
        s_lo, s_hi = cfg.zone_score
        keep = (
            work["odds"].between(o_lo, o_hi)
            & work["score"].between(s_lo, s_hi)
            & (work["rank"] <= cfg.top_k)
            & (work["race_max"] >= cfg.race_min_score)
            & work["odds"].notna()
            & work["umaban"].notna()
        )
        chosen = work[keep].reset_index().rename(columns={"index": "race_id"})
        if "race_id" not in chosen.columns:
            chosen = chosen.rename(columns={chosen.columns[0]: "race_id"})
        chosen["race_id"] = chosen["race_id"].astype(str)
        chosen["umaban"] = chosen["umaban"].astype(int)
        return chosen[["race_id", "umaban", "odds", "score"]]

    # --- サイジング（追い下げ / 大穴排除） --------------------------------
    def stake(self, bankroll: float, odds: float) -> float:
        """stake = 残高 × κ ÷ odds。min_stake 未満なら 0（不買）。"""
        if not np.isfinite(odds) or odds <= 0:
            return 0.0
        raw = bankroll * self.config.sizing_kappa / odds
        return float(raw) if raw >= self.config.min_stake else 0.0
