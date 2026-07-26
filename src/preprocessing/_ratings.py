"""ペアワイズ Elo レーティング（着差補正つき）の純粋計算。

各馬の「地力」を対戦結果（同一レースでの着順）から推定する潜在量。1 レースを
出走頭数ぶんの 1 対 1 対戦の集合（最大 18C2=153 ペア）とみなし、先着＝勝ち／後着＝負け
として全ペアで Elo 更新する。着差（馬身差）が大きい勝ちほど更新幅を増やす補正を掛ける。

リーク安全性: フィーチャは常に「そのレースの**出走前**の現行レーティング」を書き出し、
レース結果での更新は次レース以降にのみ反映する（`build_rating_frame` が日付昇順で 1 パス）。
これにより `date < 対象レース日` の情報しか特徴量に入らない（as-of）。

レイヤ: preprocessing。pandas と constants（_feature_cols）のみ依存（取得・I/O なし）。
スタッキングの入力特徴量として使うことを意図する（独立予測器ではない）。
"""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Hashable, Mapping, Sequence, TypeVar

import pandas as pd

from src.constants._feature_cols import ELO_FEATURE_COLS

H = TypeVar("H", bound=Hashable)

# ---------------------------------------------------------------------------
# ドメイン定数
# ---------------------------------------------------------------------------

ELO_INITIAL = 1500.0      # 初出走馬の事前レーティング
ELO_DIVISOR = 400.0       # Elo の標準スケール（400 差で勝率 ~0.91）
ELO_BASE_K = 24.0         # 1 対戦あたりの基本更新係数
MARGIN_SCALE = 0.5        # 着差補正の強さ（0 で着差無視＝純位置 Elo）
MARGIN_MULT_CAP = 2.0     # 着差補正倍率の上限（大差の暴れを抑える）


def expected_score(r_a: float, r_b: float) -> float:
    """A が B に先着する Elo 期待勝率。``1/(1+10**((r_b-r_a)/400))``。"""
    return 1.0 / (1.0 + 10.0 ** ((r_b - r_a) / ELO_DIVISOR))


def margin_multiplier(length_gap: float) -> float:
    """着差（馬身差）→ K の倍率。僅差は ~1.0、大差ほど大きく（上限 cap）。

    `length_gap<=0`（同着・着差不明）は 1.0。`1+MARGIN_SCALE*ln(1+gap)` を cap で抑える。
    """
    if length_gap <= 0:
        return 1.0
    return min(1.0 + MARGIN_SCALE * math.log1p(length_gap), MARGIN_MULT_CAP)


def field_win_probs(ratings: Mapping[H, float]) -> dict[H, float]:
    """レーティング → レース内の正規化勝率（Bradley-Terry/Elo 強度比）。

    強度 ``10**(r/400)`` をレース内で正規化（Σ=1）。再学習不要のスタンドアロン勝率
    （Rating Lab の照会・対市場診断に使う）。
    """
    strengths = {h: 10.0 ** (float(v) / ELO_DIVISOR) for h, v in ratings.items()}
    total = sum(strengths.values())
    if total <= 0:
        return {}
    return {h: s / total for h, s in strengths.items()}


def update_race(
    ratings: Mapping[H, float],
    finish_order: Sequence[H],
    margins: Mapping[H, float] | None = None,
    *,
    base_k: float = ELO_BASE_K,
) -> dict[H, float]:
    """1 レースのペアワイズ Elo 更新後のレーティングを返す（純粋関数）。

    Parameters
    ----------
    ratings : {horse_id: 現行レーティング}（出走前。欠損は ELO_INITIAL）。
    finish_order : 着順昇順（1着→最下位）に並べた horse_id。
    margins : {horse_id: 着差}（着順1つ上との馬身差）。None なら着差補正なし。
    base_k : 基本 K。

    各馬は他全馬と 1 回ずつ対戦し、その合計を ``(n-1)`` で平均して 1 レース=1 更新に
    正規化する。ペアの K は両馬で同一（abs 着差差で対称）なのでレース内のレーティング
    総和は保存される（零和）。
    """
    n = len(finish_order)
    if n < 2:
        return {h: float(ratings.get(h, ELO_INITIAL)) for h in finish_order}

    r = {h: float(ratings.get(h, ELO_INITIAL)) for h in finish_order}
    # 着差の累積（勝ち馬からの総馬身差）。マージン不明は 0 扱い。
    cum: dict[H, float] = {}
    acc = 0.0
    for h in finish_order:
        if margins is not None:
            acc += max(0.0, float(margins.get(h, 0.0) or 0.0))
        cum[h] = acc

    delta: dict[H, float] = defaultdict(float)
    for a_idx in range(n):
        a = finish_order[a_idx]
        for b_idx in range(a_idx + 1, n):
            b = finish_order[b_idx]  # a が b に先着（a_idx < b_idx）
            e_a = expected_score(r[a], r[b])
            k = base_k * margin_multiplier(cum[b] - cum[a]) if margins is not None else base_k
            # a は勝ち(1)、b は負け(0)。同一 K・対称更新で零和。
            delta[a] += k * (1.0 - e_a)
            delta[b] += k * (0.0 - (1.0 - e_a))

    return {h: r[h] + delta[h] / (n - 1) for h in finish_order}


def _field_features(
    field_ids: Sequence[H],
    ratings: Mapping[H, float],
    n_races: Mapping[H, int],
) -> dict[H, dict[str, float]]:
    """出走前の現行状態から、各馬の Elo 特徴量を作る（学習・ライブ共通）。

    elo_rating / elo_n_races / elo_field_mean(レース内一定) / elo_vs_field / elo_win_prob。
    """
    cur = {h: float(ratings.get(h, ELO_INITIAL)) for h in field_ids}
    field_mean = sum(cur.values()) / len(cur) if cur else ELO_INITIAL
    win_p = field_win_probs(cur)
    out: dict[H, dict[str, float]] = {}
    for h in field_ids:
        out[h] = {
            "elo_rating": cur[h],
            "elo_n_races": float(n_races.get(h, 0)),
            "elo_field_mean": field_mean,
            "elo_vs_field": cur[h] - field_mean,
            "elo_win_prob": float(win_p.get(h, 0.0)),
        }
    return out


def build_rating_frame(
    results: pd.DataFrame, *, base_k: float = ELO_BASE_K
) -> tuple[pd.DataFrame, dict]:
    """着順履歴から (race_id, 馬番) 粒度の Elo 特徴フレームとスナップショットを作る。

    Parameters
    ----------
    results : 列 ``race_id, date, 馬番, 着順, horse_id`` を持つ全レース結果。
        ``着差``（数値・馬身差）があれば着差補正に使う（無ければ純位置 Elo）。
    base_k : 基本 K。

    Returns
    -------
    (frame, snapshot)
        frame : columns = ["race_id", "馬番", *ELO_FEATURE_COLS]。各行は
            「そのレースの**出走前**」の Elo（リーク無し）。
        snapshot : {horse_id: {"rating": float, "n_races": int}} 最新値。
            ライブ予測（未来レース）で参照する。
    """
    cols = ["race_id", "馬番", *ELO_FEATURE_COLS]
    need = {"race_id", "date", "馬番", "着順", "horse_id"}
    if results is None or results.empty or not need.issubset(results.columns):
        return pd.DataFrame(columns=cols), {}

    df = results.copy()
    df["race_id"] = df["race_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    df["horse_id"] = df["horse_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    df["_rank"] = pd.to_numeric(df["着順"], errors="coerce")
    df["_uma"] = pd.to_numeric(df["馬番"], errors="coerce")
    df["_date"] = pd.to_datetime(df["date"], errors="coerce")
    has_margin = "着差" in df.columns
    if has_margin:
        df["_margin"] = pd.to_numeric(df["着差"], errors="coerce")

    ratings: dict[object, float] = {}
    n_races: dict[object, int] = defaultdict(int)
    rows: list[dict] = []

    # 日付昇順 → 同日内は race_id 昇順で 1 パス（as-of 保証）。
    for _rid, g in sorted(
        df.groupby("race_id"),
        key=lambda kv: (kv[1]["_date"].min(), kv[0]),
    ):
        rid = str(_rid)
        field = g.dropna(subset=["horse_id"])
        field_ids = list(field["horse_id"])
        if not field_ids:
            continue

        # 1) 出走前の現行レーティングで特徴量を書き出す（リーク無し）
        feats = _field_features(field_ids, ratings, n_races)
        for _, row in field.iterrows():
            hid = row["horse_id"]
            uma = row["_uma"]
            if pd.isna(uma):
                continue
            rows.append({"race_id": rid, "馬番": int(uma), **feats[hid]})

        # 2) 着順が揃っている馬だけで Elo 更新（次レース以降に反映）
        ranked = field.dropna(subset=["_rank"]).sort_values("_rank")
        order = list(ranked["horse_id"])
        if len(order) >= 2:
            margins = (
                {h: float(m) for h, m in zip(ranked["horse_id"], ranked["_margin"], strict=False)
                 if pd.notna(m)}
                if has_margin
                else None
            )
            updated = update_race(ratings, order, margins, base_k=base_k)
            ratings.update(updated)
            for h in order:
                n_races[h] += 1

    snapshot = {str(h): {"rating": float(r), "n_races": int(n_races.get(h, 0))}
                for h, r in ratings.items()}
    frame = pd.DataFrame(rows).reindex(columns=cols) if rows else pd.DataFrame(columns=cols)
    return frame, snapshot


def features_from_snapshot(
    field_ids: Sequence[H], snapshot: Mapping[str, Mapping[str, float]]
) -> dict[H, dict[str, float]]:
    """スナップショット（最新 Elo）から未来レースの出走馬の特徴量を作る（ライブ予測用）。

    `build_rating_frame` の as-of 書き出しと同じ `_field_features` を使い、学習時と
    同一の特徴量を再現する。未知馬（初出走）は ELO_INITIAL。
    """
    ratings = {str(h): float(snapshot.get(str(h), {}).get("rating", ELO_INITIAL))
               for h in field_ids}
    n_races = {str(h): int(snapshot.get(str(h), {}).get("n_races", 0)) for h in field_ids}
    keyed = {str(h): h for h in field_ids}
    feats = _field_features(list(ratings.keys()), ratings, n_races)
    return {keyed[k]: v for k, v in feats.items()}
