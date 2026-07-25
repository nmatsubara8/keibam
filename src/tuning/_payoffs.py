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


def _norm_cell(s: str) -> list[str]:
    """払戻テーブルのセル（br/空白区切り）をトークン列に正規化する。

    旧フォーマット（空白区切り '3 5 7' / '150 170 110'）も br 区切りに寄せて分割。
    払戻側のカンマ（'1,200'）は除去する。
    """
    s = str(s).replace(",", "")
    s = s.replace(" - ", "-").replace(" → ", "→").replace(" ", "br")
    return [t.strip() for t in s.split("br") if t.strip()]


def place_payoff_lookup_from_returns(return_df: pd.DataFrame) -> dict:
    """return_tables（netkeiba 生払戻）から複勝の {(race_id:str, 馬番:int): payoff_yen:float}。

    return_tables は列 0=券種ラベル / 1=当選馬番(br区切り) / 2=払戻(br区切り)、race_id は
    列または index。複勝は 1 レースに最大3頭が入るので br/空白で分解して縦持ちに落とす。
    payoffs.pkl（中央アーカイブ・1986-2021）に無い NAR/最近のレースの複勝決済に使う。
    """
    if return_df is None or return_df.empty:
        return {}
    df = return_df
    rid = df["race_id"].astype(str) if "race_id" in df.columns else df.index.to_series().astype(str)

    def _col(i):
        if i in df.columns:
            return df[i]
        return df[str(i)] if str(i) in df.columns else None

    cat, win, pay = _col(0), _col(1), _col(2)
    if cat is None or win is None or pay is None:
        return {}
    cat = cat.astype(str).str.strip()
    mask = (cat == "複勝").to_numpy()
    lookup: dict = {}
    for r, w, p in zip(rid.to_numpy()[mask], win.astype(str).to_numpy()[mask],
                       pay.astype(str).to_numpy()[mask], strict=False):
        umas = _norm_cell(w)
        pays = _norm_cell(p)
        for u, pv in zip(umas, pays, strict=False):
            if u.isdigit() and pv.replace(".", "", 1).isdigit():
                lookup[(str(r), int(u))] = float(pv)
    return lookup


def merged_fukusho_lookup(payoffs_path: str, return_tables_path: str) -> dict:
    """複勝決済ルックアップを payoffs.pkl（中央archive）＋ return_tables.pkl（NAR/最近）で統合。

    payoffs を土台に return_tables を上書きマージ（同一キーは return_tables 優先＝新しい実取得）。
    どちらか一方でも空ならもう一方のみ。
    """
    import os

    base = single_horse_payoff_lookup(load_payoffs(payoffs_path), "fukusho")
    ret = {}
    if os.path.exists(return_tables_path):
        try:
            ret = place_payoff_lookup_from_returns(pd.read_pickle(return_tables_path))
        except Exception:
            ret = {}
    merged = dict(base)
    merged.update(ret)
    return merged
