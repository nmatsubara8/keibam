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


def trifecta_payoff_lookup(return_df: pd.DataFrame) -> dict:
    """return_tables から三連単の {race_id:str: ((1着,2着,3着):tuple, payoff_yen:float)}。

    確認済み形式: 列 'race_id' / '0'=券種ラベル / '1'=買い目('13 → 15 → 10') /
    '2'=払戻(100円あたり・カンマ有りうる '1,410') / '3'=人気。当選の1組のみ。
    payoff_yen は 100円あたりの払戻金（531.9倍なら 53190）。
    """
    if return_df is None or return_df.empty:
        return {}
    label_col = "0" if "0" in return_df.columns else 0
    combo_col = "1" if "1" in return_df.columns else 1
    pay_col = "2" if "2" in return_df.columns else 2
    sub = return_df[return_df[label_col].astype(str).str.contains("三連単", na=False)]
    rid_series = (sub["race_id"].astype(str) if "race_id" in sub.columns
                  else sub.index.to_series().astype(str))
    out: dict = {}
    for rid, combo, pay in zip(rid_series, sub[combo_col].astype(str),
                               sub[pay_col].astype(str), strict=False):
        parts = [p for p in combo.replace("→", " ").replace("-", " ").split() if p.isdigit()]
        if len(parts) != 3:
            continue
        pay_tok = pay.replace(",", "").split()
        if not pay_tok or not pay_tok[0].isdigit():
            continue
        out[str(rid)] = (tuple(int(x) for x in parts), float(pay_tok[0]))
    return out


_LABELS = {"umaren": "馬連", "umatan": "馬単", "wide": "ワイド",
           "sanrenpuku": "三連複", "sanrentan": "三連単", "wakuren": "枠連"}
_ORDERED = {"umatan", "sanrentan"}
# 枠連の買い目トークンは馬番ではなく枠番(1-8)。それ以外は size と非順序扱いは馬連と同じ。


def multi_bet_payoff_lookup(return_df: pd.DataFrame, bet_type: str) -> dict:
    """return_tables から任意の連系券種の {race_id: [(combo, payoff_yen), ...]}。

    ワイド/複勝は 1 レースに複数当選（最大3）＝複数 (combo, payoff) を返す。順序券種
    （馬単/三連単）は combo が順序付きタプル、非順序（馬連/ワイド/三連複）は昇順ソート。
    形式: 列 '0'=券種 / '1'=買い目（'13 - 15' や '13 → 15 → 10'・複数はスペース連結）/
    '2'=払戻（スペース連結）。区切りは順序→ / 非順序- の混在を数字トークン抽出で吸収。
    """
    if return_df is None or return_df.empty:
        return {}
    label = _LABELS.get(bet_type)
    if label is None:
        return {}
    lc = "0" if "0" in return_df.columns else 0
    cc = "1" if "1" in return_df.columns else 1
    pc = "2" if "2" in return_df.columns else 2
    size = {"umaren": 2, "umatan": 2, "wide": 2, "sanrenpuku": 3, "sanrentan": 3,
            "wakuren": 2}[bet_type]
    sub = return_df[return_df[lc].astype(str).str.strip() == label]
    rid_s = (sub["race_id"].astype(str) if "race_id" in sub.columns
             else sub.index.to_series().astype(str))
    out: dict = {}
    for rid, combo, pay in zip(rid_s, sub[cc].astype(str), sub[pc].astype(str), strict=False):
        nums = [int(x) for x in combo.replace("→", " ").replace("-", " ").split() if x.isdigit()]
        pays = [float(x) for x in pay.replace(",", "").split() if x.replace(".", "").isdigit()]
        # size 個ずつに区切って複数当選を復元（ワイドは 2×3=6 馬番 / 3 払戻）
        combos = [tuple(nums[i:i + size]) for i in range(0, len(nums), size)]
        rows = []
        for i, cmb in enumerate(combos):
            if len(cmb) != size or i >= len(pays):
                continue
            key = cmb if bet_type in _ORDERED else tuple(sorted(cmb))
            rows.append((key, pays[i]))
        if rows:
            out.setdefault(str(rid), []).extend(rows)
    return out


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
