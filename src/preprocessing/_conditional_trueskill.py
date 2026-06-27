"""条件別 TrueSkill（芝/ダ・距離・回り）の純粋計算ロジック — Phase 3。

レイヤ規約: preprocessing 層。constants と numpy/pandas（third-party）、および同層の
_trueskill（共通の更新式）にのみ依存し、ファイル I/O は行わない。

モデル:
- 馬の地力は条件で変わる（芝向き/ダート向き、短距離/長距離 等）という前提のもと、
  各馬について「条件次元 × バケット」ごとに独立した TrueSkill (μ, σ) を保持する。
- 1 レースはある特定の条件（例: 芝・マイル・右）に属するので、その条件の
  バケットだけを当該レース結果で更新する（更新式は Phase 2 の update_ranking を共用）。

特徴量（各次元 d について）:
- ts_<d>_conservative : 当該レース条件での保守的スキル μ - 3σ
- ts_<d>_n_races      : 当該条件での出走数（信頼度の代理）
- ts_<d>_vs_field     : 同条件の保守的スキルのレース内相対値

リーク無し as-of:
- compute_conditional_trueskill_history は日付昇順に走査し、各出走の「出走前」値を
  当該条件バケットから記録してから結果で更新する（更新後値は次レース以降のみ反映）。
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING
from typing import Callable

from src.constants._feature_cols import COND_DIMENSION_COLUMN
from src.constants._feature_cols import COND_DIMENSIONS
from src.constants._feature_cols import COND_DISTANCE_BIN_UNITS
from src.constants._feature_cols import COND_DISTANCE_LABELS
from src.constants._feature_cols import COND_TS_FEATURE_COLS
from src.constants._feature_cols import TS_MU
from src.constants._feature_cols import TS_SIGMA
from src.constants._results_cols import ResultsCols
from src.preprocessing._trueskill import conservative
from src.preprocessing._trueskill import field_features
from src.preprocessing._trueskill import update_ranking

if TYPE_CHECKING:
    import pandas as pd

_N_SUFFIX = 3  # 1 次元あたりの特徴量列数（conservative / n_races / vs_field）


# ──────────────────────────────────────────
# バケッタ（純粋関数・解釈不能は None でスキップ）
# ──────────────────────────────────────────


def _str_bucket(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    s = str(value).strip()
    return s or None


def surface_bucket(value: object) -> str | None:
    """race_type（芝/ダート/障害）をそのままバケットにする。"""
    return _str_bucket(value)


def around_bucket(value: object) -> str | None:
    """around（右/左/直線）をそのままバケットにする。"""
    return _str_bucket(value)


def distance_bucket(value: object) -> str | None:
    """course_len（100m 単位）を sprint/mile/middle/long に量子化する。"""
    try:
        v = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if math.isnan(v):
        return None
    for edge, label in zip(COND_DISTANCE_BIN_UNITS, COND_DISTANCE_LABELS, strict=False):
        if v < edge:
            return label
    return COND_DISTANCE_LABELS[-1]


_BUCKETERS: dict[str, Callable[[object], str | None]] = {
    "surface": surface_bucket,
    "distance": distance_bucket,
    "around": around_bucket,
}


# ──────────────────────────────────────────
# as-of 履歴ウォーク（pandas）
# ──────────────────────────────────────────


def compute_conditional_trueskill_history(
    df: "pd.DataFrame",
    *,
    mu0: float = TS_MU,
    sigma0: float = TS_SIGMA,
) -> "tuple[pd.DataFrame, dict]":
    """全レースを日付昇順に走査し、リーク無し as-of 条件別 TrueSkill 特徴量を返す。

    各出走について、当該レース条件（次元ごとのバケット）での「出走前 (μ, σ)」から
    特徴量（COND_TS_FEATURE_COLS）を入力と同じ行順・インデックスで返す。条件列が
    無い/値が欠損の次元は事前分布（prior）値を出力し、更新もスキップする。

    Returns
    -------
    (features, snapshot) :
        features : COND_TS_FEATURE_COLS を列に持つ DataFrame（df と同じ行順）。
        snapshot : {horse_id(str): {dim: {bucket: {"mu", "sigma", "n_races"}}}}。
    """
    import numpy as np
    import pandas as pd

    work = df.reset_index()
    if "race_id" in work.columns:
        rid = work["race_id"].astype(str)
    else:
        rid = work[df.index.name or "index"].astype(str)
    work = work.assign(
        __pos=np.arange(len(work)),
        __rid=rid.to_numpy(),
        __hid=work["horse_id"].astype(str).to_numpy(),
        __date=pd.to_datetime(work["date"], errors="coerce").to_numpy(),
        __finish=pd.to_numeric(work[ResultsCols.RANK], errors="coerce").to_numpy(),
    )
    work = work.sort_values(["__date", "__rid", "__pos"], kind="stable")

    # 状態: (horse_id, dim, bucket) -> 値
    mus: dict[tuple, float] = {}
    sigmas: dict[tuple, float] = {}
    counts: dict[tuple, int] = {}
    # snapshot: horse -> dim -> bucket -> {mu, sigma, n_races}
    snapshot: dict[str, dict] = {}

    prior_cons = conservative(mu0, sigma0)
    out = np.full((len(df), len(COND_TS_FEATURE_COLS)), np.nan, dtype=float)
    present_dims = [
        d for d in COND_DIMENSIONS if COND_DIMENSION_COLUMN[d] in work.columns
    ]

    for _rid, sub in work.groupby("__rid", sort=False):
        positions = sub["__pos"].to_numpy()
        hids = sub["__hid"].tolist()
        finishes = [float(x) for x in sub["__finish"].tolist()]
        valid = [k for k, f in enumerate(finishes) if not math.isnan(f)]

        for dim_idx, dim in enumerate(COND_DIMENSIONS):
            base = dim_idx * _N_SUFFIX
            col = COND_DIMENSION_COLUMN[dim]
            bucket = None
            if dim in present_dims:
                bucket = _BUCKETERS[dim](sub[col].iloc[0])

            if bucket is None:
                # 条件不明 → prior を出力（更新なし）
                for pos in positions:
                    out[pos, base + 0] = prior_cons
                    out[pos, base + 1] = 0.0
                    out[pos, base + 2] = 0.0
                continue

            keys = [(h, dim, bucket) for h in hids]
            cur_mu = [mus.get(key, mu0) for key in keys]
            cur_sigma = [sigmas.get(key, sigma0) for key in keys]
            ncnt = [counts.get(key, 0) for key in keys]
            cons = [conservative(m, s) for m, s in zip(cur_mu, cur_sigma, strict=True)]
            field_mean, vs_field = field_features(cons)

            for k, pos in enumerate(positions):
                out[pos, base + 0] = cons[k]
                out[pos, base + 1] = float(ncnt[k])
                out[pos, base + 2] = vs_field[k]

            if len(valid) >= 2:
                v_mu = [cur_mu[k] for k in valid]
                v_sigma = [cur_sigma[k] for k in valid]
                v_finish = [finishes[k] for k in valid]
                new_mu, new_sigma = update_ranking(v_mu, v_sigma, v_finish)
                for vi, k in enumerate(valid):
                    mus[keys[k]] = new_mu[vi]
                    sigmas[keys[k]] = new_sigma[vi]
            for key in keys:
                counts[key] = counts.get(key, 0) + 1

    # スナップショット構築
    for (h, dim, bucket), mu in mus.items():
        snapshot.setdefault(h, {}).setdefault(dim, {})[bucket] = {
            "mu": round(float(mu), 4),
            "sigma": round(float(sigmas.get((h, dim, bucket), sigma0)), 4),
            "n_races": int(counts.get((h, dim, bucket), 0)),
        }

    features = pd.DataFrame(out, index=df.index, columns=list(COND_TS_FEATURE_COLS))
    return features, snapshot


def race_buckets(row: "pd.Series") -> dict[str, str | None]:
    """1 行（出走）の条件列から各次元のバケットを解決する（ライブ経路で使用）。"""
    result: dict[str, str | None] = {}
    for dim in COND_DIMENSIONS:
        col = COND_DIMENSION_COLUMN[dim]
        result[dim] = _BUCKETERS[dim](row[col]) if col in row.index else None
    return result
