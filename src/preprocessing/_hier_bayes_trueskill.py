"""階層ベイズ TrueSkill（市場オッズ事前分布・3段）の純粋計算ロジック — Phase 5。

レイヤ規約: preprocessing 層。constants と numpy/pandas（third-party）にのみ依存し、
ファイル I/O は行わない。

設計（ガウス共役の 3 段階層ベイズ・forward-pass 閉形式）:
    個体（データ尤度）: N(ts_mu, ts_sigma²)            ← Phase 2 の as-of スキル（自前）
    市場（事前）:        N(m_skill, τ_market²)          ← 単勝 de-vig → implied 勝率 → logit 換算
    群（事前）:          N(g_skill, τ_group²)           ← 種牡馬産駒の as-of 平均スキル（→ 全体平均）
    事後平均 = Σ(mean_k · prec_k) / Σ prec_k           （prec = 1/分散）

不確実性で縮小: 出走が浅い（ts_sigma 大）ほど個体精度が低く、市場・群事前へ寄る
（コールドスタートを補完）。実績が増える（ts_sigma 小）ほど自前データ主導になる。

EV エッジ保護: 中核特徴量は hb_vs_market = ts_mu - m_skill（＝我々が市場より高評価する
度合い）。raw 単勝は学習に入れず（_data_splitter で除外）、派生 hb 特徴量のみ学習に入る。
人気馬（低オッズ・実績薄）の hb_vs_market は ≈0 になり「人気＝勝つ」を学習しない。

リーク無し as-of:
- 個体 ts_mu/ts_sigma は Phase 2 の as-of 値（出走前）。市場は出走前オッズ（SP は発走前に
  確定し賭け時に利用可）。群平均は当該レースより前の産駒のみで算出。当該レース結果は不使用。
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING
from typing import Sequence

from src.constants._feature_cols import HB_FEATURE_COLS
from src.constants._feature_cols import HB_MARKET_SCALE
from src.constants._feature_cols import HB_SIGMA_FLOOR
from src.constants._feature_cols import HB_TAU_GROUP
from src.constants._feature_cols import HB_TAU_MARKET
from src.constants._feature_cols import TS_MU
from src.constants._results_cols import ResultsCols

if TYPE_CHECKING:
    import pandas as pd

_GLOBAL_KEY = "__global__"


# ──────────────────────────────────────────
# 市場オッズ → スキル
# ──────────────────────────────────────────


def implied_probabilities(odds: Sequence[float]) -> list[float]:
    """単勝オッズ列 → de-vig（控除率除去）した implied 勝率。

    inv_i = 1/odds_i を有効オッズ（>1）について正規化する。無効値（NaN/<=1）は NaN。
    全て無効なら全 NaN。
    """
    inv: list[float] = []
    for o in odds:
        try:
            v = float(o)
        except (TypeError, ValueError):
            inv.append(float("nan"))
            continue
        inv.append(1.0 / v if (v == v and v > 1.0) else float("nan"))
    total = sum(x for x in inv if x == x)
    if total <= 0:
        return [float("nan")] * len(inv)
    return [(x / total if x == x else float("nan")) for x in inv]


def market_skills(odds: Sequence[float], *, scale: float = HB_MARKET_SCALE) -> list[float]:
    """単勝オッズ列 → 市場推定スキル（μ スケール）。

    implied 勝率 → logit → レース内平均で中心化 → scale 倍して TS_MU 周りに配置する。
    無効オッズの馬は NaN（市場事前を持たない）。
    """
    probs = implied_probabilities(odds)
    logits: list[float] = []
    for p in probs:
        if p != p:
            logits.append(float("nan"))
            continue
        pc = min(max(p, 1e-6), 1 - 1e-6)
        logits.append(math.log(pc / (1.0 - pc)))
    valid = [x for x in logits if x == x]
    if not valid:
        return [float("nan")] * len(logits)
    mean_logit = sum(valid) / len(valid)
    return [
        (TS_MU + scale * (x - mean_logit) if x == x else float("nan")) for x in logits
    ]


# ──────────────────────────────────────────
# 3 段の精度加重合成
# ──────────────────────────────────────────


def combine_levels(
    ts_mu: float,
    ts_sigma: float,
    m_skill: float | None,
    g_skill: float | None,
    *,
    tau_market: float = HB_TAU_MARKET,
    tau_group: float = HB_TAU_GROUP,
    sigma_floor: float = HB_SIGMA_FLOOR,
) -> tuple[float, float]:
    """個体⊕市場⊕群を精度加重で合成し (事後平均, 縮小度) を返す（純粋関数）。

    縮小度 = 事前（市場+群）精度 / 総精度 ∈[0,1]。1 に近いほど事前（市場・群）依存。
    """
    prec_ind = 1.0 / (max(float(ts_sigma), sigma_floor) ** 2)
    num = ts_mu * prec_ind
    total = prec_ind
    if m_skill is not None and m_skill == m_skill:
        pm = 1.0 / (tau_market ** 2)
        num += m_skill * pm
        total += pm
    if g_skill is not None and g_skill == g_skill:
        pg = 1.0 / (tau_group ** 2)
        num += g_skill * pg
        total += pg
    post = num / total
    shrinkage = (total - prec_ind) / total
    return post, shrinkage


# ──────────────────────────────────────────
# as-of 履歴ウォーク（pandas）
# ──────────────────────────────────────────


def compute_hier_bayes_history(df: "pd.DataFrame") -> "tuple[pd.DataFrame, dict]":
    """全レースを日付昇順に走査し、リーク無し as-of 階層ベイズ特徴量を返す。

    Parameters
    ----------
    df : race_id をインデックス（または 'race_id' 列）に持ち、'horse_id' / 'date' /
        'ts_mu' / 'ts_sigma' を含む DataFrame。ResultsCols.TANSHO_ODDS(単勝) があれば
        市場事前に、'peds_0'（種牡馬 id）があれば群事前に使う。

    Returns
    -------
    (features, groups) :
        features : HB_FEATURE_COLS（df と同じ行順・インデックス）。
        groups : {peds_0: {"mean", "count"}, "__global__": {...}} の as-of 群平均
            スナップショット（ライブの群事前用）。
    """
    import numpy as np
    import pandas as pd

    if "ts_mu" not in df.columns or "ts_sigma" not in df.columns:
        # TrueSkill 未計算: 階層ベイズは個体尤度を欠くため空（prior）で返す
        features = pd.DataFrame(
            np.full((len(df), len(HB_FEATURE_COLS)), 0.0),
            index=df.index, columns=list(HB_FEATURE_COLS),
        )
        return features, {}

    work = df.reset_index()
    if "race_id" in work.columns:
        rid = work["race_id"].astype(str)
    else:
        rid = work[df.index.name or "index"].astype(str)

    has_odds = ResultsCols.TANSHO_ODDS in work.columns
    has_sire = "peds_0" in work.columns
    work = work.assign(
        __pos=np.arange(len(work)),
        __rid=rid.to_numpy(),
        __date=pd.to_datetime(work["date"], errors="coerce").to_numpy(),
        __mu=pd.to_numeric(work["ts_mu"], errors="coerce").to_numpy(),
        __sigma=pd.to_numeric(work["ts_sigma"], errors="coerce").to_numpy(),
    )
    work = work.sort_values(["__date", "__rid", "__pos"], kind="stable")

    # 群（種牡馬）as-of 累積: key -> [sum, count]
    group_sum: dict[str, float] = {_GLOBAL_KEY: 0.0}
    group_cnt: dict[str, float] = {_GLOBAL_KEY: 0.0}
    out = np.full((len(df), len(HB_FEATURE_COLS)), np.nan, dtype=float)

    for _rid, sub in work.groupby("__rid", sort=False):
        positions = sub["__pos"].to_numpy()
        mus = [float(x) for x in sub["__mu"].tolist()]
        sigmas = [float(x) for x in sub["__sigma"].tolist()]
        sires = sub["peds_0"].astype(str).tolist() if has_sire else [None] * len(sub)
        m_skills = (
            market_skills(sub[ResultsCols.TANSHO_ODDS].tolist())
            if has_odds else [float("nan")] * len(sub)
        )
        # 全体 as-of 平均（群が無い/未知のときのバックオフ先）
        global_mean = (
            group_sum[_GLOBAL_KEY] / group_cnt[_GLOBAL_KEY]
            if group_cnt[_GLOBAL_KEY] > 0 else TS_MU
        )

        hb_skills: list[float] = []
        for k in range(len(positions)):
            sire = sires[k]
            if sire is not None and group_cnt.get(sire, 0) > 0:
                g_skill = group_sum[sire] / group_cnt[sire]
            else:
                g_skill = global_mean
            m = m_skills[k] if m_skills[k] == m_skills[k] else None
            post, shrink = combine_levels(mus[k], sigmas[k], m, g_skill)
            hb_skills.append(post)
            vs_market = (mus[k] - m_skills[k]) if m_skills[k] == m_skills[k] else 0.0
            pos = positions[k]
            out[pos, 0] = post
            out[pos, 1] = vs_market
            out[pos, 3] = shrink

        field_mean = sum(hb_skills) / len(hb_skills) if hb_skills else 0.0
        for k in range(len(positions)):
            out[positions[k], 2] = hb_skills[k] - field_mean

        # 群累積を更新（当該レースの as-of ts_mu を産駒データとして加算）
        for k in range(len(positions)):
            mu = mus[k]
            if mu != mu:
                continue
            group_sum[_GLOBAL_KEY] += mu
            group_cnt[_GLOBAL_KEY] += 1
            sire = sires[k]
            if sire is not None and sire not in ("nan", "None", ""):
                group_sum[sire] = group_sum.get(sire, 0.0) + mu
                group_cnt[sire] = group_cnt.get(sire, 0.0) + 1

    features = pd.DataFrame(out, index=df.index, columns=list(HB_FEATURE_COLS))
    groups = {
        key: {"mean": round(group_sum[key] / group_cnt[key], 4), "count": int(group_cnt[key])}
        for key in group_cnt
        if group_cnt[key] > 0
    }
    return features, groups
