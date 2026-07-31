"""JRDB SED 払戻を ReturnProcessor 互換の払戻源にする（netkeiba 46%欠損 → JRDB 100%）。

バックテストの評価関数(回収率)を正確化する。netkeiba の raw_return_tables は本プロジェクトの
データ破損で複勝カバレッジ ~46% に留まり、回収率が過小評価される。JRDB SED は
tansho_payoff / fukusho_payoff（確定払戻・円/100円）を (race_id, umaban) 単位で 100% 持つ。

`BettingTickets` は `return_processor.preprocessed_data`（{BetType: DataFrame}）だけを読む。
各テーブルは index=race_id、列 win_0/return_0/... で、単勝・複勝は _SingleStrategy が
win_i=的中馬番・return_i=払戻(円/100円) を走査する。本クラスはその形を SED から組む。
連系（馬連等）は HJC 由来のため本ソースでは空（複勝運用の計測には不要）。

使い方:
    from src.storage._db import get_engine
    src = JrdbReturnSource(get_engine(db_path))
    simulate_model(ai, holdout, "複勝本命(損失最小)", threshold, return_processor=src)
"""
from __future__ import annotations

import pandas as pd

from src.constants._bet_types import BetType


def race_payout_row(placed: list[tuple[int, float]], n_slots: int) -> dict:
    """placed=[(馬番, 払戻円)] → {win_i, return_i}（n_slots 個・不足スロットは 0 埋め）。

    照合(_sum_returns)は全スロットを走査し win==0 を無効扱いするため、順序は不問・0埋め安全。
    """
    row: dict = {}
    for i in range(n_slots):
        if i < len(placed):
            row[f"win_{i}"], row[f"return_{i}"] = int(placed[i][0]), float(placed[i][1])
        else:
            row[f"win_{i}"], row[f"return_{i}"] = 0, 0.0
    return row


def build_single_table(df: pd.DataFrame, payoff_col: str, n_slots: int) -> pd.DataFrame:
    """df(race_id,umaban,payoff_col) → index=race_id の win_i/return_i テーブル（payoff>0 のみ）。"""
    rows: dict = {}
    for rid, g in df.groupby("race_id"):
        placed = [(int(u), float(p)) for u, p in zip(g["umaban"], g[payoff_col], strict=False)
                  if p and float(p) > 0]
        if placed:
            rows[str(rid)] = race_payout_row(placed, n_slots)
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame.from_dict(rows, orient="index")
    for c in [c for c in out.columns if c.startswith("win_")]:
        out[c] = out[c].fillna(0).astype(int)
    # object dtype で保持する。.loc[race_id] は行を homogeneous Series 化するため、
    # 数値dtypeだと win(int) が float "3.0" に格上げされ _match の int(str(win)) が壊れる。
    # object なら各セルの Python 型（int 馬番 / float 払戻）が維持される（netkeiba テーブルと同形）。
    return out.astype(object)


class JrdbReturnSource:
    """ReturnProcessor 互換（.preprocessed_data のみ）の JRDB SED 払戻源。単勝・複勝を供給。"""

    def __init__(self, engine, sed: pd.DataFrame | None = None) -> None:
        if sed is None:
            from sqlalchemy import text
            sed = pd.read_sql(
                text("SELECT race_id, umaban, tansho_payoff, fukusho_payoff FROM raw_jrdb_sed"),
                engine)
        sed = sed.copy()
        sed["race_id"] = sed["race_id"].astype(str).str.split(".").str[0]
        sed["umaban"] = pd.to_numeric(sed["umaban"], errors="coerce")
        sed = sed.dropna(subset=["umaban"])
        for c in ("tansho_payoff", "fukusho_payoff"):
            sed[c] = pd.to_numeric(sed[c], errors="coerce").fillna(0.0)
        empty = pd.DataFrame()
        self._data = {
            BetType.TANSHO: build_single_table(sed, "tansho_payoff", 1),
            BetType.FUKUSHO: build_single_table(sed, "fukusho_payoff", 3),
            BetType.WAKUREN: empty, BetType.UMAREN: empty, BetType.UMATAN: empty,
            BetType.WIDE: empty, BetType.SANRENPUKU: empty, BetType.SANRENTAN: empty,
        }

    @property
    def preprocessed_data(self) -> dict:
        return self._data

    def coverage(self, race_ids, bet_type: BetType = BetType.FUKUSHO) -> float:
        """指定 race_id 群のうち bet_type 払戻テーブルに存在する割合（0.0–1.0）。

        「JRDB SED で 100% のはずの複勝カバレッジが netkeiba 46% 等へ退行していないか」を
        検出する回帰ガード用。空テーブル/空入力は 0.0。
        """
        table = self._data.get(bet_type)
        if table is None or table.empty:
            return 0.0
        want = {str(r).split(".")[0] for r in race_ids}
        if not want:
            return 0.0
        idx = set(map(str, table.index))
        return sum(1 for r in want if r in idx) / len(want)
