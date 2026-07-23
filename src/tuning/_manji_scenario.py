"""①.5 シナリオ compile — factor_table（Step1）× posterior（Step2）で補正列を安価に作る。

ハイブリッド設計 Step 3。①（featured）は不変のまま、卍補正シナリオ (1)…(i) ごとに
「②で使う学習データ」を生成する。

効率設計（要）:
- **posterior はシナリオ非依存**。各因子の事後はその因子のデータだけで決まり（他因子に依存しない）、
  σ² も窓グローバル。よって時系列を n_blocks に分け、各ブロックの as-of 事後を**全因子まとめて
  1 回**較正しておけば（build_block_posteriors）、全シナリオで使い回せる。
- シナリオは「どの因子を採るか＋重み」を選ぶだけ。補正列 = Σ_f w_f·point_f[bucket] は
  factor_table のバケットを posterior 点に引くだけの**線形結合**で、特徴量を再計算しない。

前進安全（リーク防止）:
- ブロック bi の行に使う事後は「bi の開始日より**前**の証拠のみ」で較正する（build_block_posteriors）。
  最初のブロックは過去が無く補正 0（中立）。これで学習データの補正列が未来を覗かない。

②への入り方（earlier の決定＝ハイブリッド: 合成スコア1列＋因子バケットも native 特徴量）:
- `manji_score`（数値・補正列）を1列。
- 各シナリオ因子のバケットを **one-hot 数値列** `manji_bkt_<factor>__<bucket>` として付与
  （string 列は _score_policy._coerce_for_predict で数値強制され壊れるため、one-hot に統一）。
"""
from __future__ import annotations

import dataclasses
from typing import Mapping

import numpy as np
import pandas as pd

from src.constants._results_cols import ResultsCols
from src.policies._manji_factors import NA, FACTORS
from src.tuning._manji_posterior import (
    PosteriorConfig,
    calibrate_points_bayes,
    default_half_lives,
)


@dataclasses.dataclass(frozen=True)
class Scenario:
    """補正シナリオ（仮説セット）。因子部分集合＋重み＋買いゾーン。"""
    name: str
    factors: tuple[str, ...]
    weights: Mapping[str, float] = dataclasses.field(default_factory=dict)
    zone_odds: tuple[float, float] = (3.0, 50.0)
    top_k: int = 3
    include_bucket_features: bool = True
    description: str = ""

    def weight(self, factor: str) -> float:
        return float(self.weights.get(factor, 1.0))


# --- 手動シナリオ登録（3〜6個の仮説。OOS 回収率＋placebo で最良を選ぶ＝Step4） --------
SCENARIOS: dict[str, Scenario] = {
    "recent_form": Scenario(
        "recent_form",
        factors=("recent3_form", "recent5_form", "recent3_recovery", "recent5_recovery",
                 "career_form", "popularity"),
        description="近況・近走回収重視（忘却割引つき履歴依拠因子が主）",
    ),
    "pedigree_class": Scenario(
        "pedigree_class",
        factors=("sire_line", "race_class", "age", "sex", "season_sex"),
        description="血統系統・クラス・馬齢/性の普遍傾向重視",
    ),
    "pace_dev": Scenario(
        "pace_dev",
        factors=("pace_pressure", "leg_type", "waku", "kinryo_rank", "ground"),
        description="展開（想定ペース×脚質）・枠・斤量・馬場重視",
    ),
    "market_value": Scenario(
        "market_value",
        factors=("popularity", "weight_diff", "rotation", "dist_change", "body_weight"),
        description="市場妙味（人気）×状態（馬体・ローテ・距離変更）重視",
    ),
    "value_jinba": Scenario(
        "value_jinba",
        factors=(
            "jockey", "trainer", "sire", "popularity",
            # 人×条件（名鑑の条件別妙味: 芝ダ・距離帯・場・道悪・クラス）
            "jockey*race_type", "jockey*dist_band", "jockey*place",
            "jockey*ground", "jockey*race_class",
            "trainer*dist_band", "trainer*race_class",
            # 種牡馬×条件（芝ダ・距離変更・距離帯・場・道悪）
            "sire*race_type", "sire*dist_change", "sire*dist_band",
            "sire*place", "sire*ground",
        ),
        include_bucket_features=False,  # 高カード＆クロスは manji_score のみ（one-hot 爆発回避）
        description="卍流『妙味度』: 騎手/厩舎/種牡馬 × 条件の過小評価を補正回収率で捕捉",
    ),
    "all": Scenario(
        "all",
        factors=tuple(FACTORS),
        description="全因子（対照・上限）",
    ),
}

# one-hot native 特徴量にするバケット数の上限（超える高カード因子・クロスは manji_score のみ）。
MAX_ONEHOT_CARD = 24


def list_scenarios() -> list[str]:
    return list(SCENARIOS)


def _validate_registry() -> None:
    for s in SCENARIOS.values():
        bad = [f for f in s.factors if f.split("*")[0] not in FACTORS]
        if bad:
            raise ValueError(f"scenario {s.name}: 未知の因子 {bad}")


_validate_registry()


# --- 時系列ブロックと as-of 事後（シナリオ非依存・全因子まとめて1回） -----------------

def time_blocks(featured: pd.DataFrame, n_blocks: int = 8):
    """発走日順にレース単位で n_blocks 分割し、(ブロック開始日, 行マスク) を返す。"""
    race_date = pd.to_datetime(featured["date"], errors="coerce").groupby(level=0).first().sort_values()
    order = list(race_date.index)
    n = len(order)
    if n == 0:
        return []
    bounds = [round(i * n / n_blocks) for i in range(n_blocks + 1)]
    rid_arr = featured.index.astype(str).to_numpy()
    blocks = []
    for i in range(n_blocks):
        rids = order[bounds[i]:bounds[i + 1]]
        if not rids:
            continue
        cutoff = race_date.loc[rids[0]]  # ブロック内 最古日 = 開始日
        mask = np.isin(rid_arr, np.array([str(r) for r in rids]))
        blocks.append((pd.Timestamp(cutoff), mask))
    return blocks


def build_block_posteriors(
    featured: pd.DataFrame,
    factor_names: list[str] | None = None,
    *,
    n_blocks: int = 8,
    cfg: PosteriorConfig | None = None,
    factor_half_life: dict[str, float] | None = None,
):
    """各ブロックについて (行マスク, points[factor][bucket]) を返す（全シナリオ共有）。

    ブロック bi の points は「bi 開始日より前の証拠のみ」で較正（前進安全）。
    factor_names は全シナリオ因子の和（既定=全 FACTORS）。事後は因子独立なので一括較正で足りる。
    """
    cfg = cfg or PosteriorConfig()
    factor_names = factor_names or list(FACTORS)
    if factor_half_life is None:
        factor_half_life = default_half_lives()
    dates = pd.to_datetime(featured["date"], errors="coerce")
    out = []
    for cutoff, mask in time_blocks(featured, n_blocks):
        train = featured[dates < cutoff]
        pts = {} if train.empty else calibrate_points_bayes(
            train, factor_names, cfg=cfg, factor_half_life=factor_half_life,
        )
        out.append((mask, pts))
    return out


# --- factor_table 整列と補正列の compile --------------------------------------

def align_buckets(featured: pd.DataFrame, factor_table: pd.DataFrame) -> pd.DataFrame:
    """factor_table を featured の行順に整列した DataFrame を返す（キー (race_id,馬番) で merge）。"""
    key = pd.DataFrame({
        "race_id": featured.index.astype(str).to_numpy(),
        "馬番": pd.to_numeric(featured[ResultsCols.UMABAN], errors="coerce").astype("Int64").to_numpy(),
    })
    merged = key.merge(factor_table, on=["race_id", "馬番"], how="left", sort=False)
    return merged


def compile_correction(
    aligned: pd.DataFrame,
    block_posteriors,
    scenario: Scenario,
) -> np.ndarray:
    """補正列 corr[row] = Σ_f w_f · point_f[bucket_f(row)] を返す（行のブロックの事後を使う）。

    factor_table のバケット × posterior 点の線形結合のみ（特徴量再計算なし）。
    """
    n = len(aligned)
    corr = np.zeros(n, dtype=float)
    for mask, pts in block_posteriors:
        if not pts:
            continue
        idx = np.flatnonzero(mask)
        if idx.size == 0:
            continue
        for f in scenario.factors:
            pmap = pts.get(f)
            if not pmap:
                continue
            w = scenario.weight(f)
            if w == 0.0:
                continue
            if f not in aligned.columns:
                continue
            col = aligned[f].to_numpy()[idx]
            vals = np.fromiter((pmap.get(b, 0.0) for b in col), dtype=float, count=idx.size)
            corr[idx] += w * vals
    return corr


def build_scenario_training_data(
    featured: pd.DataFrame,
    scenario: Scenario,
    *,
    factor_table: pd.DataFrame,
    block_posteriors=None,
    n_blocks: int = 8,
    cfg: PosteriorConfig | None = None,
    factor_half_life: dict[str, float] | None = None,
) -> pd.DataFrame:
    """シナリオ j の「②で使う学習データ」= ①features ⊕ manji_score ⊕ 因子バケット one-hot。

    ① は変更しない（copy を返す）。block_posteriors を渡せば再較正しない（複数シナリオで共有）。
    """
    aligned = align_buckets(featured, factor_table)
    if block_posteriors is None:
        block_posteriors = build_block_posteriors(
            featured, list(FACTORS), n_blocks=n_blocks, cfg=cfg, factor_half_life=factor_half_life,
        )
    corr = compile_correction(aligned, block_posteriors, scenario)

    out = featured.copy()  # ① は不変。augmented copy を返す
    out["manji_score"] = corr  # 位置代入（長さ一致）
    if scenario.include_bucket_features:
        for f in scenario.factors:
            # クロス（"A*B"）と factor_table に無い因子は one-hot 化しない（manji_score へ集約）。
            if "*" in f or f not in aligned.columns:
                continue
            col = aligned[f].astype(object).to_numpy()
            buckets = sorted({b for b in col if b is not None and b != NA and pd.notna(b)})
            # 高カード（jockey/sire/place 等）は列爆発するので one-hot 化せず manji_score のみ。
            if len(buckets) > MAX_ONEHOT_CARD:
                continue
            for b in buckets:
                out[f"manji_bkt_{f}__{b}"] = (col == b).astype(float)
    return out


def prepare_shared(
    featured: pd.DataFrame,
    *,
    factor_table: pd.DataFrame | None = None,
    n_blocks: int = 8,
    cfg: PosteriorConfig | None = None,
    factor_half_life: dict[str, float] | None = None,
):
    """全シナリオ共有の重い成果物（factor_table・block_posteriors）を1回作る。

    Returns: (factor_table, block_posteriors)
    """
    if factor_table is None:
        from src.tuning._manji_factor_store import build_factor_table, load_factor_table
        factor_table = load_factor_table()
        if factor_table is None:
            factor_table = build_factor_table(featured)
    block_posteriors = build_block_posteriors(
        featured, list(FACTORS), n_blocks=n_blocks, cfg=cfg, factor_half_life=factor_half_life,
    )
    return factor_table, block_posteriors
