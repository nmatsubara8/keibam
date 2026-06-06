"""表示フォーマット純粋関数。

streamlit に依存しないため単体テスト可能。UI ページはこのモジュールを呼び出して
表示文字列や DataFrame を組み立てる。
"""

from __future__ import annotations

import dataclasses

import pandas as pd

from src.constants._bet_types import BetType


def format_ev(ev: float, threshold: float = 1.0) -> str:
    """期待値を "1.45 ✓" / "0.87 ✗" 形式で返す。"""
    mark = "✓" if ev >= threshold else "✗"
    return f"{ev:.2f} {mark}"


def format_combo(combo: tuple, bet_type: str) -> str:
    """馬番タプルを馬券種に応じた表記に変換する。

    単勝/複勝 → "3"  馬連/馬単/ワイド → "3-5"  三連系 → "1-2-3"
    """
    sep = "-" if bet_type not in (BetType.TANSHO, BetType.FUKUSHO) else ""
    return sep.join(str(h) for h in combo) if sep else str(combo[0])


def format_prob(p: float) -> str:
    """確率を "35.2%" 形式で返す。"""
    return f"{p * 100:.1f}%"


def format_odds(odds: float) -> str:
    """オッズを "x 3.5" 形式で返す。"""
    return f"x {odds:.1f}"


def format_stake(stake: float) -> str:
    """掛け金を "¥1,200" 形式で返す（100円単位切り捨て）。"""
    rounded = int(stake // 100) * 100
    return f"¥{rounded:,}"


def candidates_to_display_df(candidates: list) -> pd.DataFrame:
    """BetCandidate リストを Streamlit に渡す表示用 DataFrame に変換する。

    EV が高い順に並べ替えて返す。
    """
    if not candidates:
        return pd.DataFrame()
    rows = []
    for c in candidates:
        rows.append(
            {
                "race_id": c.race_id,
                "馬券種": c.bet_type,
                "組合せ": format_combo(c.combo, c.bet_type),
                "的中確率": format_prob(c.probability),
                "オッズ": format_odds(c.odds),
                "EV": format_ev(c.expected_value),
                "確信度": f"{c.confidence:.2f}",
                "推奨金額": format_stake(c.stake),
                "_ev_raw": c.expected_value,
            }
        )
    df = pd.DataFrame(rows).sort_values("_ev_raw", ascending=False)
    return df.drop(columns=["_ev_raw"]).reset_index(drop=True)


def snapshots_to_chart_df(snapshots: list) -> pd.DataFrame:
    """OddsSnapshot リストを折れ線グラフ用 DataFrame に変換する。

    列: captured_at, minutes_to_post, umaban(combo[0]), odds
    """
    if not snapshots:
        return pd.DataFrame(columns=["captured_at", "minutes_to_post", "umaban", "odds"])
    rows = [
        {
            "captured_at": s.captured_at,
            "minutes_to_post": s.minutes_to_post,
            "umaban": s.combo[0] if s.combo else None,
            "odds": s.odds,
            "phase": s.phase,
        }
        for s in snapshots
    ]
    return pd.DataFrame(rows).sort_values("captured_at")
