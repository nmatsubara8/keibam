"""券種別オッズから「市場の歪み（market distortion）」を馬ごとに算出する純粋ロジック。

単勝オッズだけが既に特徴量化されているが、複勝・三連複・三連単の確定オッズには
「単勝市場が織り込まない情報（コネ・厩舎の本気度・スマートマネー）」が滲む。本モジュールは
各券種の市場 implied 確率を馬単位の marginal に集約し、単勝由来の Harville 理論確率との
差分（overlay）を取って市場の歪みをシグナル化する。

リーク安全性: 入力は**確定オッズ**（発走前に固まる値）であり、レース結果は使わない。
既に特徴量化済みの ``単勝`` と同じ前提なのでリークしない。

正規化方針（overlay を比較可能なスケールに揃える）:
- 勝ち系（Σ_h = 1）: 単勝 Harville 勝率 / 三連単の1着 marginal。
- 3着内系（Σ_h = 3, 3枠ぶん）: Harville 複勝確率 / 複勝 implied / 三連複・三連単の top3 marginal。

レイヤ: preprocessing。pandas と policies._harville のみ依存（取得・I/O なし）。
"""

from __future__ import annotations

from collections import defaultdict
from typing import Mapping

import pandas as pd

from src.constants._bet_types import BetType
from src.policies import _harville as harville

# featured に乗せる市場歪み特徴量（生値。_z はレース内 zscore で別途付与）。
MARKET_SIGNAL_COLS: list = [
    "fukusho_implied_p",      # 複勝市場の implied 3着内確率（Σ_h=3 に正規化）
    "place_overlay",          # 複勝 implied − Harville複勝（単勝由来）。複勝市場の強気/弱気
    "trio_top3_overlay",      # 三連複 top3 marginal − Harville複勝。連系での3着内過剰評価
    "trifecta_win_overlay",   # 三連単 1着 marginal − Harville勝率。連系での勝ち過剰評価
    "trifecta_top3_overlay",  # 三連単 top3 marginal − Harville複勝
]


def _parse_combo(combo_key: str) -> tuple[int, ...]:
    """``"3-7-11"`` 形式の combo_key を馬番タプルに変換する。"""
    return tuple(int(x) for x in str(combo_key).split("-"))


def race_market_signals(
    win_odds: Mapping[int, float],
    by_type: Mapping[str, Mapping[tuple, float]],
) -> dict[int, dict[str, float]]:
    """1 レース分の市場歪みシグナルを馬番ごとに算出する（純粋関数）。

    Parameters
    ----------
    win_odds : {馬番: 単勝オッズ}。Harville の元になる市場勝率を作る。
    by_type  : {bet_type: {combo(タプル): 確定オッズ}}。複勝/三連複/三連単を使う。

    Returns
    -------
    {馬番: {特徴名: 値}}。データが無い券種の特徴は欠落（後段で NaN）。
    """
    horses = [h for h, o in win_odds.items() if o and float(o) > 0]
    if len(horses) < 3:
        return {}
    win_implied = {h: 1.0 / float(win_odds[h]) for h in horses}
    win_probs = harville.normalize(win_implied)  # Σ_h = 1
    place_p = {h: harville.prob_place(win_probs, h, 3) for h in horses}  # Σ_h ≈ 3

    out: dict[int, dict[str, float]] = {h: {} for h in horses}

    # ── 複勝（馬単位で直接）──
    fuku = by_type.get(BetType.FUKUSHO, {})
    fuku_imp = {c[0]: 1.0 / float(o) for c, o in fuku.items()
                if o and float(o) > 0 and len(c) == 1}
    s = sum(fuku_imp.values())
    if s > 0:
        for h in horses:
            if h in fuku_imp:
                fp = 3.0 * fuku_imp[h] / s  # Σ_h = 3 に正規化
                out[h]["fukusho_implied_p"] = fp
                out[h]["place_overlay"] = fp - place_p[h]

    # ── 三連複: top3 集合への帰属 marginal（各 combo は 3 頭に寄与）──
    trio = by_type.get(BetType.SANRENPUKU, {})
    trio_imp = {c: 1.0 / float(o) for c, o in trio.items()
                if o and float(o) > 0 and len(c) == 3}
    s = sum(trio_imp.values())
    if s > 0:
        marg: dict[int, float] = defaultdict(float)
        for c, imp in trio_imp.items():
            for h in c:
                marg[h] += imp
        for h in horses:
            # marg[h]=Σ_{combo∋h} imp。各 combo は 3 頭に寄与するので Σ_h marg = 3s
            # → marg[h]/s が自然に Σ_h = 3（複勝確率と同スケール）。
            m = marg[h] / s
            out[h]["trio_top3_overlay"] = m - place_p[h]

    # ── 三連単: 1着 marginal（Σ=1）と top3 marginal（Σ=3）──
    tri = by_type.get(BetType.SANRENTAN, {})
    tri_imp = {c: 1.0 / float(o) for c, o in tri.items()
               if o and float(o) > 0 and len(c) == 3}
    s = sum(tri_imp.values())
    if s > 0:
        win_marg: dict[int, float] = defaultdict(float)
        top3_marg: dict[int, float] = defaultdict(float)
        for c, imp in tri_imp.items():
            win_marg[c[0]] += imp           # 先頭=1着のみ
            for h in c:
                top3_marg[h] += imp
        for h in horses:
            # win_marg/s は Σ_h=1（各順列の1着は1頭）、top3_marg/s は Σ_h=3（3頭に寄与）。
            out[h]["trifecta_win_overlay"] = (win_marg[h] / s) - win_probs[h]
            out[h]["trifecta_top3_overlay"] = (top3_marg[h] / s) - place_p[h]

    # 値が空の馬（全券種データ無し）は落とす
    return {h: feats for h, feats in out.items() if feats}


def build_market_signal_frame(
    final_odds_lookup: Mapping[tuple, float],
    win_odds_by_race: Mapping[str, Mapping[int, float]],
) -> pd.DataFrame:
    """確定オッズ lookup と単勝オッズから (race_id, 馬番) 粒度の歪み特徴 DataFrame を作る。

    Parameters
    ----------
    final_odds_lookup : {(race_id, bet_type, combo_key): odds}
        ``_odds_snapshot.build_final_odds_lookup`` の出力。
    win_odds_by_race : {race_id: {馬番: 単勝オッズ}}。

    Returns
    -------
    columns = ["race_id", "馬番", *MARKET_SIGNAL_COLS] の DataFrame（該当無しは空）。
    """
    by_race: dict[str, dict[str, dict[tuple, float]]] = defaultdict(
        lambda: defaultdict(dict)
    )
    for (rid, bt, ck), o in final_odds_lookup.items():
        by_race[str(rid)][bt][_parse_combo(ck)] = o

    rows: list[dict] = []
    for rid, win_odds in win_odds_by_race.items():
        rid = str(rid)
        sig = race_market_signals(win_odds, by_race.get(rid, {}))
        for umaban, feats in sig.items():
            rows.append({"race_id": rid, "馬番": int(umaban), **feats})

    cols = ["race_id", "馬番", *MARKET_SIGNAL_COLS]
    if not rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame(rows).reindex(columns=cols)
