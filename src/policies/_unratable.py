"""unratable（初出走・データ無し）馬の公衆フォールバック（ベンター 1994 §3）。

ベンターは「ハンデキャップ用データの無い馬（初出走等）には**公衆の implied 勝率を
そのまま割り当てる**」とする。ファンダ・モデルは過去走特徴に依存するため、初出走馬には
無意味な値しか出せない。素のモデル勝率を使うと連系 Harville の確率が歪むため、公衆値で
置換し、評価可能な馬のモデル勝率を残余質量へ再正規化する。

初出走馬**のみ**のレース（新馬戦の一部）はモデルが全く効かないため、ベンターは除外する
（香港データで全体の ~5%）。本モジュールは `is_unratable_only` でその判定を提供する。

純関数（policies 層・他レイヤ非依存）。馬番 → 勝率の Mapping を入出力に取る。
"""

from __future__ import annotations

from typing import Iterable, Mapping

import pandas as pd

from src.constants._results_cols import ResultsCols

# 初出走判定に使うキャリア出走数の列（DataMerger._add_career_stats が付与）。
# 過去走の無い馬は left-merge で NaN になる（過去走ありは必ず >=1）。
CAREER_STARTS_COL = "career_starts"


def public_fallback(
    model_probs: Mapping[int, float],
    public_probs: Mapping[int, float],
    unratable: Iterable[int],
) -> dict[int, float]:
    """unratable 馬を公衆 implied 勝率で置換し、レース内で Σ=1 に再正規化する。

    - unratable かつ公衆値を持つ馬 → 公衆 implied 勝率を割り当て（質量 m_unr を予約）。
    - それ以外（評価可能）の馬 → モデル勝率を残余質量 (1 − m_unr) へ比例再正規化。
    - 公衆値の無い unratable 馬は置換できないためモデル勝率を残す（評価可能扱い）。

    どの馬も結果に残り Σ=1。model_probs/public_probs はレース内 Σ=1 でなくても良い
    （public は内部で正規化前提だが、未正規化でも相対比は保たれる）。
    """
    unr = {h for h in unratable if h in public_probs}
    m_unr = sum(public_probs[h] for h in unr)
    ratable = {h: float(model_probs[h]) for h in model_probs if h not in unr}

    out: dict[int, float] = {}
    sr = sum(ratable.values())
    if sr > 0 and 0.0 <= m_unr < 1.0:
        scale = (1.0 - m_unr) / sr
        out.update({h: p * scale for h, p in ratable.items()})
    else:
        # 縮退（評価可能馬が無い／公衆質量が 1 以上）はモデル勝率をそのまま残す
        out.update(ratable)
    for h in unr:
        out[h] = float(public_probs[h])

    s = sum(out.values())
    return {h: p / s for h, p in out.items()} if s > 0 else out


def is_unratable_only(umabans: Iterable[int], unratable: Iterable[int]) -> bool:
    """レースの全馬が unratable（初出走のみ）か。True なら除外対象（モデルが効かない）。"""
    horses = list(umabans)
    unr = set(unratable)
    return bool(horses) and all(u in unr for u in horses)


def build_unratable_by_race(
    X: pd.DataFrame, *, career_col: str = CAREER_STARTS_COL
) -> dict[object, set[int]]:
    """featured X から {race_id: {初出走の馬番...}} を作る。

    初出走 = career_starts が NaN もしくは 0（過去走なし）。career_col が無い／
    馬番列が無い場合は空 dict（フォールバック無効）。X は race_id を index に持つ。
    """
    if career_col not in X.columns or ResultsCols.UMABAN not in X.columns:
        return {}
    starts = pd.to_numeric(X[career_col], errors="coerce")
    debut_mask = starts.isna() | (starts <= 0)
    out: dict[object, set[int]] = {}
    if not debut_mask.any():
        return out
    sub = X.loc[debut_mask, [ResultsCols.UMABAN]]
    for race_id, umaban in zip(sub.index, sub[ResultsCols.UMABAN], strict=False):
        try:
            out.setdefault(race_id, set()).add(int(umaban))
        except (TypeError, ValueError):
            continue
    return out
