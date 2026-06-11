"""「市場の重力」— 人気順位バケット別のシェア遷移統計（Layer: training）。

経験的に、オッズ市場には人気順位別の遷移傾向がある（30 分前に 1 番人気 2.4 倍なら
最終 2.0〜2.3 倍へ収束しやすい一方、12 番人気 80 倍は 50〜120 倍の範囲でしか動かない）。
これを「隣接フェーズ間の CLR 座標差分の平均（drift）と標準偏差（vol）」として
人気順バケット別に推定し、Kalman / Particle / Dirichlet 全モデルの事前分布に使う。

データが少ないバケットは既定値（drift 0・diffuse な vol）へ縮小推定するため、
スナップショットが 1 件も無くても成立する（= 恒等予測 + 既定拡散に退化）。
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import logging
import os

import numpy as np
import pandas as pd

from src.constants._odds_dynamics import DEFAULT_DRIFT
from src.constants._odds_dynamics import DEFAULT_VOL_FALLBACK
from src.constants._odds_dynamics import DEFAULT_VOL_PER_STEP
from src.constants._odds_dynamics import GRAVITY_FILENAME
from src.constants._odds_dynamics import MIN_BUCKET_COUNT
from src.constants._odds_dynamics import RANK_BUCKETS
from src.constants._odds_dynamics import bucket_for_rank
from src.constants._odds_phases import PHASE_TIMELINE
from src.training._simplex import clr
from src.training._simplex import popularity_ranks

logger = logging.getLogger(__name__)


def default_vol(phase_from: str, phase_to: str) -> float:
    return DEFAULT_VOL_PER_STEP.get((phase_from, phase_to), DEFAULT_VOL_FALLBACK)


@dataclasses.dataclass
class GravityStats:
    """(phase_from, phase_to, bucket) → {drift, vol, n} のテーブル。

    lookup は常に縮小推定後の値を返すため、未観測の組合せでも安全に使える。
    """

    table: dict = dataclasses.field(default_factory=dict)
    fitted_at: str = ""

    def lookup(self, phase_from: str, phase_to: str, rank: int) -> tuple[float, float]:
        """人気順位 rank の馬の (drift, vol) を返す（縮小推定済み）。"""
        bucket = bucket_for_rank(rank)
        rec = self.table.get((phase_from, phase_to, bucket))
        v0 = default_vol(phase_from, phase_to)
        if rec is None:
            return DEFAULT_DRIFT, v0
        n = rec["n"]
        k = MIN_BUCKET_COUNT
        drift = n * rec["drift"] / (n + k)
        vol = float(np.sqrt((n * rec["vol"] ** 2 + k * v0**2) / (n + k)))
        return drift, vol


def adjacent_phase_pairs(phases: list[str]) -> list[tuple[str, str]]:
    """観測されたフェーズ列（時系列順）から隣接遷移の対を返す（純粋関数）。"""
    ordered = [p for p in PHASE_TIMELINE if p in phases]
    return list(zip(ordered[:-1], ordered[1:], strict=True))


def fit_gravity(sequences: dict) -> GravityStats:
    """レース別シェア系列から人気順バケット別の遷移統計を推定する。

    Parameters
    ----------
    sequences : `race_share_sequences` の出力（race_id → {phase: シェア Series}）。
    """
    samples: dict = {}
    for per_phase in sequences.values():
        phases = list(per_phase.keys())
        for phase_from, phase_to in adjacent_phase_pairs(phases):
            s_from = per_phase[phase_from]
            s_to = per_phase[phase_to]
            common = s_from.index.intersection(s_to.index)
            if len(common) < 2:
                continue
            # 両フェーズに共通する馬で再正規化してから CLR 差分をとる
            f = s_from.loc[common].to_numpy()
            t = s_to.loc[common].to_numpy()
            f = f / f.sum()
            t = t / t.sum()
            dx = clr(t) - clr(f)
            ranks = popularity_ranks(f)
            for i in range(len(common)):
                key = (phase_from, phase_to, bucket_for_rank(int(ranks[i])))
                samples.setdefault(key, []).append(float(dx[i]))

    table = {}
    for key, values in samples.items():
        arr = np.asarray(values)
        table[key] = {
            "drift": float(arr.mean()),
            "vol": float(arr.std(ddof=1)) if len(arr) >= 2 else default_vol(key[0], key[1]),
            "n": int(len(arr)),
        }
    return GravityStats(table=table, fitted_at=dt.datetime.now().isoformat())


# ---------------------------------------------------------------------------
# 永続化（models/odds_gravity.json — _tuning_history.py と同パターン）
# ---------------------------------------------------------------------------


def gravity_path(models_dir: str = "models") -> str:
    return os.path.join(models_dir, GRAVITY_FILENAME)


def save_gravity(stats: GravityStats, path: str) -> None:
    payload = {
        "fitted_at": stats.fitted_at,
        "rank_buckets": list(RANK_BUCKETS),
        "table": [
            {"phase_from": k[0], "phase_to": k[1], "bucket": k[2], **v}
            for k, v in stats.table.items()
        ],
    }
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info("[gravity] %s: %d cells saved", path, len(stats.table))


def load_gravity(path: str) -> GravityStats:
    """保存済み重力統計を読み込む（無ければ空 = 既定値のみで動作）。"""
    if not os.path.exists(path):
        return GravityStats()
    with open(path) as f:
        payload = json.load(f)
    table = {
        (r["phase_from"], r["phase_to"], int(r["bucket"])): {
            "drift": r["drift"], "vol": r["vol"], "n": r["n"],
        }
        for r in payload.get("table", [])
    }
    return GravityStats(table=table, fitted_at=payload.get("fitted_at", ""))


def gravity_to_frame(stats: GravityStats) -> pd.DataFrame:
    """UI 表示用に DataFrame 化する。"""
    if not stats.table:
        return pd.DataFrame(columns=["phase_from", "phase_to", "bucket", "drift", "vol", "n"])
    rows = [
        {"phase_from": k[0], "phase_to": k[1], "bucket": k[2], **v}
        for k, v in stats.table.items()
    ]
    return pd.DataFrame(rows).sort_values(["phase_from", "phase_to", "bucket"]).reset_index(drop=True)
