"""ペアワイズ Elo レーティング（着差補正つき）の純粋計算ロジック — Phase 1。

レイヤ規約: preprocessing 層。constants と numpy/pandas（third-party）のみに依存し、
ファイル I/O やスクレイピングは行わない（_data_merger / pipeline 側が配線・永続化する）。

設計（リーク無し as-of）:
- 各レースについて「出走前時点の現行レーティング」を特徴量として記録し、
  その後にレース結果でレーティングを更新する（更新後値は次レース以降にのみ反映）。
- これにより date < 対象レース日 の情報のみで特徴量が決まり、目的変数リークしない。
- フィールドサイズ非依存にするため、1 レースの更新量は全対戦相手平均（÷(n-1)）で算出する。

着差補正:
- netkeiba のレース結果ページの「着差」はテキスト（クビ/ハナ/1.1/2 等）。
  parse_margin() で馬身に変換し、着順順に累積して「勝ち馬からの累積着差」を作る。
  ペア (i, j) の着差は |累積_i - 累積_j|。着差が解釈できない場合は K 一定へフォールバック。

ロードマップ（Phase 2-5）: TrueSkill 等への拡張でも ELO_FEATURE_COLS を起点に
列を増やす設計とする（compute_rating_history の返す特徴量列を拡張する）。
"""

from __future__ import annotations

import math
import re
from typing import TYPE_CHECKING
from typing import Sequence

from src.constants._feature_cols import ELO_BASE_K
from src.constants._feature_cols import ELO_FEATURE_COLS
from src.constants._feature_cols import ELO_INITIAL_RATING
from src.constants._feature_cols import ELO_MARGIN_REF
from src.constants._results_cols import ResultsCols

if TYPE_CHECKING:
    import pandas as pd

# ──────────────────────────────────────────
# 着差テキスト → 馬身（length）
# ──────────────────────────────────────────

# netkeiba 着差テキストの語 → おおよその馬身。クビ/ハナ等は競馬の慣用尺度に基づく近似。
_MARGIN_WORDS: dict[str, float] = {
    "同着": 0.0,
    "ハナ": 0.05,
    "アタマ": 0.2,
    "クビ": 0.3,
    "大差": 10.0,
}

_RE_MIXED = re.compile(r"^(\d+)\.(\d+)/(\d+)$")  # 例: 1.1/2 → 1 + 1/2
_RE_FRAC = re.compile(r"^(\d+)/(\d+)$")          # 例: 3/4 → 3/4
_RE_NUM = re.compile(r"^\d+(?:\.\d+)?$")          # 例: 3 / 2.5


def parse_margin(value: object) -> float:
    """着差テキストを馬身（>= 0）に変換する。解釈できない値は NaN を返す。

    勝ち馬（空文字 / "0" / NaN 相当）は 0.0。数値（馬の過去成績ページ由来）は絶対値。
    """
    if value is None:
        return float("nan")
    if isinstance(value, bool):
        return float("nan")
    if isinstance(value, (int, float)):
        v = float(value)
        return float("nan") if math.isnan(v) else abs(v)

    s = str(value).strip()
    if s in ("", "0", "0.0"):
        return 0.0
    # 単位語を除去
    s = s.replace("馬身", "").replace(" ", "")
    if s in _MARGIN_WORDS:
        return _MARGIN_WORDS[s]
    # 語が含まれる（稀: 接頭の数字なし語）
    for word, length in _MARGIN_WORDS.items():
        if s == word:
            return length

    m = _RE_MIXED.match(s)
    if m:
        whole, num, den = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return whole + (num / den if den else 0.0)
    m = _RE_FRAC.match(s)
    if m:
        num, den = int(m.group(1)), int(m.group(2))
        return num / den if den else float("nan")
    if _RE_NUM.match(s):
        return float(s)
    return float("nan")


# ──────────────────────────────────────────
# Elo コア（純粋数値関数）
# ──────────────────────────────────────────


def expected_score(r_a: float, r_b: float) -> float:
    """レーティング r_a の馬が r_b の馬に勝つ期待スコア（ロジスティック）。"""
    return 1.0 / (1.0 + 10.0 ** ((r_b - r_a) / 400.0))


def margin_k(base_k: float, margin: float | None, n_horses: int | None = None) -> float:
    """着差に応じて更新係数 K を増減する。

    着差大 → 更新大（ただし log で逓減し頭打ち）。margin が None/NaN のときは base_k。
    n_horses は将来のフィールドサイズ調整用に受け取るが、現状は更新側の
    「全対戦相手平均」で正規化するため未使用。
    """
    if margin is None:
        return float(base_k)
    if isinstance(margin, float) and math.isnan(margin):
        return float(base_k)
    m = max(0.0, float(margin))
    return float(base_k) * (1.0 + math.log1p(m) / math.log1p(ELO_MARGIN_REF))


def update_pairwise(
    ratings: Sequence[float],
    finish_order: Sequence[float],
    margins: Sequence[float] | None = None,
    *,
    base_k: float = ELO_BASE_K,
) -> list[float]:
    """1 レース内の全ペア勝敗から各馬のレーティングを更新して返す（純粋関数）。

    Parameters
    ----------
    ratings : 各馬の出走前レーティング（entrant 順）。
    finish_order : 各馬の着順（小さいほど上位、同値は同着）。
    margins : 各馬の「勝ち馬からの累積着差（馬身）」。None または NaN を含む場合、
        その対戦は K 一定（base_k）へフォールバックする。
    base_k : 基本更新係数。

    Returns
    -------
    list[float] : 更新後レーティング（entrant 順）。期待スコアは全対戦相手平均で
        算出し、更新量を (n-1) で平均してフィールドサイズに依存しないようにする。
    """
    n = len(ratings)
    if n < 2:
        return [float(r) for r in ratings]

    new = [float(r) for r in ratings]
    for i in range(n):
        delta = 0.0
        for j in range(n):
            if i == j:
                continue
            fi, fj = finish_order[i], finish_order[j]
            if fi < fj:
                s = 1.0
            elif fi > fj:
                s = 0.0
            else:
                s = 0.5
            e = expected_score(ratings[i], ratings[j])
            if margins is not None:
                mi, mj = margins[i], margins[j]
                if (isinstance(mi, float) and math.isnan(mi)) or (
                    isinstance(mj, float) and math.isnan(mj)
                ):
                    k = float(base_k)
                else:
                    k = margin_k(base_k, abs(float(mi) - float(mj)))
            else:
                k = float(base_k)
            delta += k * (s - e)
        new[i] = float(ratings[i]) + delta / (n - 1)
    return new


def field_features(ratings: Sequence[float]) -> tuple[float, list[float]]:
    """出走馬レーティング列 → (field_mean, [vs_field...]) を返す（学習・ライブ共通）。"""
    arr = [float(r) for r in ratings]
    if not arr:
        return float("nan"), []
    fm = sum(arr) / len(arr)
    return fm, [r - fm for r in arr]


def elo_win_probabilities(ratings: Sequence[float]) -> list[float]:
    """レーティング列から各馬の勝率（Bradley-Terry / Elo 多頭版）を算出する。

    p_i ∝ 10**(r_i / 400)。再学習不要の即時照会（Rating Lab タブ1）に使う。
    """
    arr = [float(r) for r in ratings]
    if not arr:
        return []
    mx = max(arr)
    weights = [10.0 ** ((r - mx) / 400.0) for r in arr]
    total = sum(weights)
    if total <= 0:
        n = len(arr)
        return [1.0 / n] * n
    return [w / total for w in weights]


# ──────────────────────────────────────────
# as-of 履歴ウォーク（pandas）
# ──────────────────────────────────────────


def _cumulative_margins(finishes: list[float], raw_margins: list[object]) -> list[float]:
    """着順順に着差を累積し「勝ち馬からの累積着差」を entrant 順で返す。

    着順の小さい順に並べ、parse_margin した値を累積する。勝ち馬（最小着順）は 0。
    途中で解釈不能（NaN）があると、それ以降の累積は NaN になり、K 一定へ退化する。
    """
    n = len(finishes)
    order = sorted(range(n), key=lambda i: (finishes[i] if finishes[i] == finishes[i] else float("inf")))
    cum = [float("nan")] * n
    running = 0.0
    broken = False
    for rank_pos, idx in enumerate(order):
        if rank_pos == 0:
            cum[idx] = 0.0
            continue
        if broken:
            cum[idx] = float("nan")
            continue
        gap = parse_margin(raw_margins[idx])
        if math.isnan(gap):
            broken = True
            cum[idx] = float("nan")
            continue
        running += gap
        cum[idx] = running
    return cum


def compute_rating_history(
    df: "pd.DataFrame",
    *,
    base_k: float = ELO_BASE_K,
    initial: float = ELO_INITIAL_RATING,
) -> "tuple[pd.DataFrame, dict]":
    """全レースを日付昇順に 1 パス走査し、リーク無し as-of レーティング特徴量を返す。

    入力 df の各行（= 1 出走）に対し、出走前時点のレーティング特徴量
    （ELO_FEATURE_COLS）を入力と同じ行順・インデックスで返す。レース結果による
    更新は当該レースの特徴量を確定させた後に行うため、目的変数リークしない。

    Parameters
    ----------
    df : race_id をインデックス（または 'race_id' 列）に持ち、'horse_id' /
        ResultsCols.UMABAN(馬番) / ResultsCols.RANK(着順) / 'date' 列を含む DataFrame。
        ResultsCols.RANK_DIFF(着差) 列があれば着差補正に使う。

    Returns
    -------
    (features, snapshot) :
        features : ELO_FEATURE_COLS を列に持つ DataFrame（df と同じインデックス・行順）。
        snapshot : {horse_id(str): {"rating": float, "n_races": int, "last_date": str}}。
            最新レーティングのスナップショット（ライブ予測で参照）。
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
    has_margin = ResultsCols.RANK_DIFF in work.columns

    # 日付 → race_id → 元の行位置 の安定ソートで走査順を確定する。
    work = work.sort_values(["__date", "__rid", "__pos"], kind="stable")

    ratings: dict[str, float] = {}
    counts: dict[str, int] = {}
    last_date: dict[str, str] = {}

    out = np.full((len(df), len(ELO_FEATURE_COLS)), np.nan, dtype=float)

    for _rid, sub in work.groupby("__rid", sort=False):
        positions = sub["__pos"].to_numpy()
        hids = sub["__hid"].tolist()
        finishes = [float(x) for x in sub["__finish"].tolist()]
        cur = [ratings.get(h, initial) for h in hids]
        ncnt = [counts.get(h, 0) for h in hids]
        field_mean, vs_field = field_features(cur)

        for k, pos in enumerate(positions):
            out[pos, 0] = cur[k]            # elo_rating
            out[pos, 1] = float(ncnt[k])    # elo_n_races
            out[pos, 2] = field_mean        # elo_field_mean
            out[pos, 3] = vs_field[k]       # elo_vs_field

        # 着順が有効な entrant のみで更新する（取消・除外行は更新に含めない）。
        valid = [k for k, f in enumerate(finishes) if not math.isnan(f)]
        if len(valid) >= 2:
            v_ratings = [cur[k] for k in valid]
            v_finish = [finishes[k] for k in valid]
            v_margins = None
            if has_margin:
                raw = sub[ResultsCols.RANK_DIFF].tolist()
                v_margins = _cumulative_margins(v_finish, [raw[k] for k in valid])
            updated = update_pairwise(v_ratings, v_finish, v_margins, base_k=base_k)
            for vi, k in enumerate(valid):
                ratings[hids[k]] = updated[vi]
        # 出走数・最終出走日は全 entrant について加算する。
        for k, h in enumerate(hids):
            counts[h] = counts.get(h, 0) + 1
            d = sub["__date"].iloc[k]
            if pd.notna(d):
                last_date[h] = pd.Timestamp(d).strftime("%Y-%m-%d")

    features = pd.DataFrame(out, index=df.index, columns=list(ELO_FEATURE_COLS))
    snapshot = {
        h: {
            "rating": round(float(ratings[h]), 2),
            "n_races": int(counts.get(h, 0)),
            "last_date": last_date.get(h),
        }
        for h in ratings
    }
    return features, snapshot
