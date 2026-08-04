"""能力 Kalman（局所線形トレンド・成長/疲労）の純粋計算ロジック — Phase 4。

レイヤ規約: preprocessing 層。constants と numpy/pandas（third-party）、標準ライブラリ
（statistics.NormalDist）にのみ依存し、ファイル I/O は行わない。

モデル（状態空間・局所線形トレンド）:
    状態 x_t = [level_t, trend_t]（能力 level と成長率 trend）
    予測:  level_t = level_{t-1} + trend_{t-1}    ← 出走前にこれを特徴量として出力
           trend_t = ρ · trend_{t-1}              （ρ<1 で成長率を平均回帰）
           P_t = F P_{t-1} Fᵀ + Q                 （Q は休養間隔で変調＝疲労/リフレッシュ）
    観測:  y_t = フィールド強度（TS 保守的スキル平均）+ scale · 着順正規スコア
           （強い相手に好走するほど高い絶対能力シグナル）→ レース後に Kalman 更新

リーク無し as-of:
- 出力する kf_level は「出走前の 1 ステップ先予測」（過去レースのみで決まる）。当該レースの
  着順は観測 y_t として更新にのみ使い、特徴量には入れない。

成長 = trend 状態が経験的に上昇/下降を捕捉。疲労 = 出走間隔で予測分散を増やしつつ、
減衰加重出走数（workload）を疲労 covariate として出力し下流 GBDT に学習させる。
"""

from __future__ import annotations

import math
from statistics import NormalDist
from typing import TYPE_CHECKING

from src.constants._feature_cols import KF_FEATURE_COLS
from src.constants._feature_cols import KF_INIT_LEVEL
from src.constants._feature_cols import KF_INIT_TREND
from src.constants._feature_cols import KF_INIT_VAR_LEVEL
from src.constants._feature_cols import KF_INIT_VAR_TREND
from src.constants._feature_cols import KF_INTERVAL_REF_DAYS
from src.constants._feature_cols import KF_PERF_SCALE
from src.constants._feature_cols import KF_Q_LEVEL
from src.constants._feature_cols import KF_Q_TREND
from src.constants._feature_cols import KF_R_OBS
from src.constants._feature_cols import KF_TREND_DECAY
from src.constants._feature_cols import KF_WINPROB_SCALE
from src.constants._feature_cols import KF_WORKLOAD_HALFLIFE_DAYS
from src.constants._results_cols import ResultsCols

if TYPE_CHECKING:
    import pandas as pd

_NORM = NormalDist()


# ──────────────────────────────────────────
# 観測スコア
# ──────────────────────────────────────────


def normal_score(rank: float, n_horses: int) -> float:
    """着順を正規スコアに変換する（勝ち馬ほど高い）。

    q = (n - rank + 0.5) / n ∈ (0,1) の逆正規 Φ⁻¹(q)。1 着→大きい正、最下位→負。
    解釈不能（rank が NaN / n<2）は NaN。
    """
    if n_horses is None or n_horses < 2:
        return float("nan")
    if rank != rank:  # NaN
        return float("nan")
    q = (n_horses - rank + 0.5) / n_horses
    q = min(max(q, 1e-6), 1 - 1e-6)
    return _NORM.inv_cdf(q)


# ──────────────────────────────────────────
# 2x2 局所線形トレンド Kalman（純粋関数）
# ──────────────────────────────────────────


def kalman_predict(
    level: float,
    trend: float,
    p00: float,
    p01: float,
    p11: float,
    *,
    q_level: float = KF_Q_LEVEL,
    q_trend: float = KF_Q_TREND,
    rho: float = KF_TREND_DECAY,
) -> tuple[float, float, float, float, float]:
    """予測ステップ。F=[[1,1],[0,ρ]]、Q=diag(q_level,q_trend)。

    Returns (level', trend', p00', p01', p11')。
    """
    level_p = level + trend
    trend_p = rho * trend
    p00_p = p00 + 2.0 * p01 + p11 + q_level
    p01_p = rho * (p01 + p11)
    p11_p = rho * rho * p11 + q_trend
    return level_p, trend_p, p00_p, p01_p, p11_p


def kalman_update(
    level: float,
    trend: float,
    p00: float,
    p01: float,
    p11: float,
    y: float,
    *,
    r: float = KF_R_OBS,
) -> tuple[float, float, float, float, float]:
    """更新ステップ（観測 H=[1,0]、観測ノイズ r）。

    Returns (level⁺, trend⁺, p00⁺, p01⁺, p11⁺)。
    """
    s = p00 + r
    if s <= 0:
        return level, trend, p00, p01, p11
    k0 = p00 / s
    k1 = p01 / s
    innovation = y - level
    level_u = level + k0 * innovation
    trend_u = trend + k1 * innovation
    p00_u = (1.0 - k0) * p00
    p01_u = (1.0 - k0) * p01
    p11_u = p11 - k1 * p01
    return level_u, trend_u, p00_u, p01_u, p11_u


def field_features(values: "list[float]") -> "tuple[float, list[float]]":
    """値列 → (field_mean, [vs_field...])。"""
    arr = [float(v) for v in values]
    if not arr:
        return float("nan"), []
    fm = sum(arr) / len(arr)
    return fm, [v - fm for v in arr]


def ability_win_probabilities(levels: "list[float]") -> "list[float]":
    """能力 level 列 → 勝率（softmax 近似）。Rating Lab の即時照会に使う。"""
    arr = [float(v) for v in levels]
    if not arr:
        return []
    mx = max(arr)
    weights = [math.exp((v - mx) / KF_WINPROB_SCALE) for v in arr]
    total = sum(weights)
    if total <= 0:
        n = len(arr)
        return [1.0 / n] * n
    return [w / total for w in weights]


# ──────────────────────────────────────────
# as-of 履歴ウォーク（pandas）
# ──────────────────────────────────────────


def compute_ability_kalman_history(df: "pd.DataFrame") -> "tuple[pd.DataFrame, dict]":
    """全レースを日付昇順に走査し、リーク無し as-of 能力 Kalman 特徴量を返す。

    各出走について「出走前の 1 ステップ先予測」を特徴量（KF_FEATURE_COLS）として
    入力と同じ行順・インデックスで返す。観測 y_t（フィールド TS 強度 + 着順正規スコア）
    による Kalman 更新は特徴量確定後に行うためリークしない。

    Parameters
    ----------
    df : race_id をインデックス（または 'race_id' 列）に持ち、'horse_id' /
        ResultsCols.UMABAN / ResultsCols.RANK / 'date' を含む DataFrame。
        'n_horses' があれば正規スコアに使い、'ts_field_mean' があれば観測の絶対水準に使う。

    Returns
    -------
    (features, snapshot) :
        features : KF_FEATURE_COLS（df と同じ行順）。
        snapshot : {horse_id: {"level","trend","var_level","workload","n_races","last_date"}}。
    """
    import numpy as np
    import pandas as pd

    work = df.reset_index()
    if "race_id" in work.columns:
        rid = work["race_id"].astype(str)
    else:
        rid = work[df.index.name or "index"].astype(str)

    has_n = "n_horses" in work.columns
    has_field = "ts_field_mean" in work.columns
    work = work.assign(
        __pos=np.arange(len(work)),
        __rid=rid.to_numpy(),
        __hid=work["horse_id"].astype(str).to_numpy(),
        __date=pd.to_datetime(work["date"], errors="coerce").to_numpy(),
        __finish=pd.to_numeric(work[ResultsCols.RANK], errors="coerce").to_numpy(),
    )
    work = work.sort_values(["__date", "__rid", "__pos"], kind="stable")

    # 状態: horse -> dict(level, trend, p00, p01, p11, workload, last_date, n_races)
    state: dict[str, dict] = {}
    out = np.full((len(df), len(KF_FEATURE_COLS)), np.nan, dtype=float)
    halflife = KF_WORKLOAD_HALFLIFE_DAYS

    for _rid, sub in work.groupby("__rid", sort=False):
        positions = sub["__pos"].to_numpy()
        hids = sub["__hid"].tolist()
        finishes = [float(x) for x in sub["__finish"].tolist()]
        race_date = pd.Timestamp(sub["__date"].iloc[0]) if pd.notna(sub["__date"].iloc[0]) else None
        n_horses = int(sub["n_horses"].iloc[0]) if has_n else len(sub)
        field_baseline = float(sub["ts_field_mean"].iloc[0]) if has_field else 0.0
        if field_baseline != field_baseline:  # NaN
            field_baseline = 0.0

        # 予測ステップ → 特徴量を確定（出走前値）
        predicted: list[dict] = []
        levels_pred: list[float] = []
        for h in hids:
            st = state.get(h)
            if st is None:
                lvl, trd = KF_INIT_LEVEL, KF_INIT_TREND
                p00, p01, p11 = KF_INIT_VAR_LEVEL, 0.0, KF_INIT_VAR_TREND
                workload = 0.0
                interval_days = None
            else:
                interval_days = None
                if race_date is not None and st["last_date"] is not None:
                    interval_days = max(0.0, (race_date - st["last_date"]).days)
                # 休養間隔でプロセスノイズを変調（長期休養→能力変化の可能性増）
                q_scale = 1.0 + (interval_days / KF_INTERVAL_REF_DAYS if interval_days else 0.0)
                lvl, trd, p00, p01, p11 = kalman_predict(
                    st["level"], st["trend"], st["p00"], st["p01"], st["p11"],
                    q_level=KF_Q_LEVEL * q_scale,
                )
                # 疲労 workload を当該日まで減衰
                if interval_days is not None and halflife > 0:
                    workload = st["workload"] * (0.5 ** (interval_days / halflife))
                else:
                    workload = st["workload"]
            predicted.append(
                {"level": lvl, "trend": trd, "p00": p00, "p01": p01, "p11": p11,
                 "workload": workload}
            )
            levels_pred.append(lvl)

        field_mean, vs_field = field_features(levels_pred)
        for k, pos in enumerate(positions):
            pr = predicted[k]
            out[pos, 0] = pr["level"]
            out[pos, 1] = pr["trend"]
            out[pos, 2] = vs_field[k]
            out[pos, 3] = math.sqrt(max(pr["p00"], 0.0))
            out[pos, 4] = pr["workload"]

        # 更新ステップ（着順が有効な馬のみ）
        for k, h in enumerate(hids):
            pr = predicted[k]
            finish = finishes[k]
            if not math.isnan(finish):
                perf = normal_score(finish, n_horses)
                if math.isnan(perf):
                    lvl_u, trd_u = pr["level"], pr["trend"]
                    p00_u, p01_u, p11_u = pr["p00"], pr["p01"], pr["p11"]
                else:
                    y = field_baseline + KF_PERF_SCALE * perf
                    lvl_u, trd_u, p00_u, p01_u, p11_u = kalman_update(
                        pr["level"], pr["trend"], pr["p00"], pr["p01"], pr["p11"], y
                    )
            else:
                lvl_u, trd_u = pr["level"], pr["trend"]
                p00_u, p01_u, p11_u = pr["p00"], pr["p01"], pr["p11"]

            prev = state.get(h)
            state[h] = {
                "level": lvl_u, "trend": trd_u,
                "p00": p00_u, "p01": p01_u, "p11": p11_u,
                "workload": pr["workload"] + 1.0,  # この出走で +1
                "last_date": race_date if race_date is not None else (prev["last_date"] if prev else None),
                "n_races": (prev["n_races"] if prev else 0) + 1,
            }

    features = pd.DataFrame(out, index=df.index, columns=list(KF_FEATURE_COLS))
    snapshot = {
        h: {
            "level": round(float(st["level"]), 4),
            "trend": round(float(st["trend"]), 4),
            "var_level": round(float(st["p00"]), 4),
            "workload": round(float(st["workload"]), 4),
            "n_races": int(st["n_races"]),
            "last_date": st["last_date"].strftime("%Y-%m-%d") if st["last_date"] is not None else None,
        }
        for h, st in state.items()
    }
    return features, snapshot
