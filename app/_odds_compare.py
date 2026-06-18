"""連系オッズの実績 vs Harville 推定の比較（照会用の計算ロジック）。

fetch-final-odds で取得した確定オッズ実績（odds_snapshots）と、単勝オッズから
Harville で推定した連系オッズを組合せ単位で突き合わせる。Streamlit 非依存。
"""

from __future__ import annotations

import pandas as pd

from src.constants._bet_types import BetType


# 連系（単勝・複勝以外）の比較対象券種
COMBO_BET_TYPES = (
    BetType.WAKUREN, BetType.UMAREN, BetType.UMATAN,
    BetType.WIDE, BetType.SANRENPUKU, BetType.SANRENTAN,
)


def tansho_odds_by_race(snapshots) -> dict:
    """スナップショットから {race_id: {馬番: 最新単勝オッズ}} を構築する。"""
    latest: dict = {}  # (race_id, umaban) -> (captured_at, odds)
    for s in snapshots:
        if s.bet_type != BetType.TANSHO or not s.combo:
            continue
        if s.odds is None or float(s.odds) <= 0:
            continue
        key = (str(s.race_id), int(s.combo[0]))
        prev = latest.get(key)
        if prev is None or s.captured_at >= prev[0]:
            latest[key] = (s.captured_at, float(s.odds))
    out: dict = {}
    for (race_id, umaban), (_, odds) in latest.items():
        out.setdefault(race_id, {})[umaban] = odds
    return out


def available_combo_targets(snapshots) -> list[tuple[str, str]]:
    """実績オッズがある (race_id, bet_type) の一覧（連系のみ）を返す。"""
    seen = set()
    for s in snapshots:
        if s.bet_type in COMBO_BET_TYPES and s.odds and float(s.odds) > 0:
            seen.add((str(s.race_id), s.bet_type))
    return sorted(seen)


def compare_combo_odds(
    snapshots, race_id: str, bet_type: str, takeout: float = 0.2
) -> pd.DataFrame:
    """指定レース・券種の「実績オッズ」vs「Harville 推定オッズ」の比較表を返す。

    - 実績: fetch-final-odds で取得した確定オッズ（odds_snapshots）。
    - 推定: 同レースの単勝オッズ → 市場勝率 → Harville 組合せ確率 →
            ``(1 - takeout) / prob`` の推定オッズ（HistoricalOddsProvider と同一換算）。

    Returns
    -------
    DataFrame[buy(買い目) / actual(実績) / harville(推定) / ratio(実績/推定)]
    （実績の降順）。単勝オッズが無く推定できない組合せは harville=NaN。
    """
    from src.preparing._odds_snapshot import build_final_odds_lookup
    from src.policies._odds_provider import HistoricalOddsProvider

    race_id = str(race_id)
    lookup = build_final_odds_lookup(snapshots, bet_types=[bet_type])
    tansho_map = tansho_odds_by_race(snapshots)
    has_tansho = race_id in tansho_map and len(tansho_map[race_id]) >= 2
    hist = HistoricalOddsProvider(tansho_map, takeout=takeout) if has_tansho else None

    rows = []
    for (rid, bt, combo_str), actual in lookup.items():
        if rid != race_id or bt != bet_type:
            continue
        combo = tuple(int(x) for x in combo_str.split("-"))
        harville = float("nan")
        if hist is not None:
            try:
                harville = hist.get_odds(race_id, bet_type, combo)
            except Exception:
                harville = float("nan")
        ratio = actual / harville if harville and harville == harville and harville > 0 else float("nan")
        rows.append({
            "buy": combo_str,
            "actual": round(float(actual), 1),
            "harville": round(harville, 1) if harville == harville else None,
            "ratio": round(ratio, 2) if ratio == ratio else None,
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.sort_values("actual", ascending=False).reset_index(drop=True)
