"""payoffs.pkl（アーカイブ払戻を正規化した縦持ち）から決済用ルックアップを作る。

払戻テーブル: race_id | bet_type | combo_key | payoff_yen | popularity
  - payoff_yen は 100円あたりの払戻金（当選 combo のみ。非当選 combo は行が無い）。

単一馬券（単勝/複勝）は combo_key が単一馬番なので、{(race_id, 馬番): payoff_yen} に落とせる。
非当選（複勝圏外・非勝馬）は lookup に無い→払戻0＝ハズレ、で決済できる。
"""
from __future__ import annotations

import pandas as pd

SINGLE_HORSE = {"tansho", "fukusho"}


def load_payoffs(path: str) -> pd.DataFrame:
    """payoffs.pkl を読む（無ければ空）。"""
    import os
    if not os.path.exists(path):
        return pd.DataFrame(columns=["race_id", "bet_type", "combo_key", "payoff_yen", "popularity"])
    return pd.read_pickle(path)


def single_horse_payoff_lookup(payoffs: pd.DataFrame, bet_type: str) -> dict:
    """単一馬券（tansho/fukusho）の {(race_id:str, 馬番:int): payoff_yen:float}。

    combo_key は "5" のような単一馬番文字列。当選馬のみが入る（非当選=キー無し=0円）。
    """
    if bet_type not in SINGLE_HORSE:
        raise ValueError(f"single_horse_payoff_lookup は単一馬券のみ: {bet_type}")
    if payoffs.empty:
        return {}
    sub = payoffs[payoffs["bet_type"] == bet_type]
    lookup: dict = {}
    for rid, ck, pay in zip(sub["race_id"].astype(str), sub["combo_key"].astype(str),
                            pd.to_numeric(sub["payoff_yen"], errors="coerce"), strict=False):
        if ck.isdigit() and pd.notna(pay):
            lookup[(rid, int(ck))] = float(pay)
    return lookup
