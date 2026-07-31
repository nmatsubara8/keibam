"""騎手＋厩舎ランクを物理シムの ability 加減点 rank_bonus にする（③ 単一スナップ全期間・リーク承知）。

⚠ 重要（リーク前提）: jocrank/tnrank は日付なしスナップショット。単一スナップを全期間へ適用すると
過去レースに「未来の昇格・成績を含む現在ランク」が混入する（leak）。本モジュールは
「過去 ROI をどれだけ動かせるか」を探索する用途で、live(as-of)には transfer しない値である。

設計:
  rank_bonus = zscore(騎手rank) + zscore(厩舎rank)   （騎手＋厩舎の合算）
  物理シムで  ability += rank_gain · rank_bonus       （rank_gain は sweep する唯一のノブ）
方向（良ランクが上/下）と強さは rank_gain の符号・大きさで吸収するため、zscore は素の rank に対して
取り、正負両側に rank_gain を振れば過去 ROI 最大点が見つかる。
"""
from __future__ import annotations

import pandas as pd


def build_rank_z(rank_df, *, code_col: str = "person_code", rank_col: str = "rank",
                 code_to_id: dict | None = None) -> dict:
    """rank_df(person_code, rank) → {id: zscore(rank)}。

    code_to_id（{jrdb_code: netkeiba_id}）があれば netkeiba id をキーに、無ければ person_code を
    そのままキーにする（テスト/直結用）。同一 code の重複は last。空/全欠損は空 dict。
    """
    if rank_df is None or len(rank_df) == 0 or code_col not in rank_df or rank_col not in rank_df:
        return {}
    d = rank_df[[code_col, rank_col]].copy()
    d[rank_col] = pd.to_numeric(d[rank_col], errors="coerce")
    d = d.dropna(subset=[code_col, rank_col]).drop_duplicates(code_col, keep="last")
    if d.empty:
        return {}
    mu = float(d[rank_col].mean())
    sd = float(d[rank_col].std(ddof=0))
    sd = sd if sd > 0 else 1.0
    out: dict = {}
    for code, r in zip(d[code_col].astype(str), d[rank_col]):
        nid = (code_to_id or {}).get(code)
        key = str(nid) if nid is not None else code
        out[key] = (float(r) - mu) / sd
    return out


def attach_rank_bonus(featured, jockey_z: dict, trainer_z: dict, *,
                      jockey_col: str = "jockey_id", trainer_col: str = "trainer_id",
                      out_col: str = "rank_bonus") -> pd.DataFrame:
    """featured に rank_bonus = jockey_z[jockey_id] + trainer_z[trainer_id]（欠損 0）列を付けて返す。

    単一スナップの z を全レースに同一適用する（③・leak 承知）。id は文字列化して突合。
    """
    f = featured.copy()
    if jockey_col in f.columns and jockey_z:
        jb = f[jockey_col].astype(str).map(jockey_z).astype(float).fillna(0.0)
    else:
        jb = pd.Series(0.0, index=f.index)
    if trainer_col in f.columns and trainer_z:
        tb = f[trainer_col].astype(str).map(trainer_z).astype(float).fillna(0.0)
    else:
        tb = pd.Series(0.0, index=f.index)
    f[out_col] = (jb + tb).to_numpy()
    return f
