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
from src.jrdb._odds import BET_ORDERED, normalize_combo


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


# ─────────────────────── HJC（8券種・確定払戻・100%）→ ReturnProcessor ───────────────────────
# 連系（馬連〜三連単）の ROI バックテストには exotic の確定払戻が要る。JRDB HJC は 1レース1
# レコードに全8券種の (当選組合せ, 払戻) を持つ（`raw_jrdb_hjc`）。`JrdbReturnSource` は単複のみ
# だったため、連系の券種別 ROI（`_backtest.settle_candidates`）が測れなかった。本ソースは HJC を
# `BettingTickets` が読む win_i/return_i 形へ組み、8券種すべてを 100% カバレッジで供給する。

# HJC の券種プレフィックス → (BetType, 組合せ頭数, 1レース最大当選数=OCC)。OCC は L.HJC_GROUPS 準拠。
_HJC_SPEC = [
    ("tansho", BetType.TANSHO, 1, 3),
    ("fukusho", BetType.FUKUSHO, 1, 5),
    ("wakuren", BetType.WAKUREN, 2, 3),
    ("umaren", BetType.UMAREN, 2, 3),
    ("wide", BetType.WIDE, 2, 7),
    ("umatan", BetType.UMATAN, 2, 6),
    ("sanrenpuku", BetType.SANRENPUKU, 3, 3),
    ("sanrentan", BetType.SANRENTAN, 3, 6),
]


def _combo_to_ints(combo: object, size: int, ordered: bool):
    """HJC の連結組合せ '070311' → 馬番タプル (7,3,11)。順不同券種は昇順。無効は None。

    順序券種（馬単/三連単・単複）は着順を保持、順不同（馬連/ワイド/三連複/枠連）は昇順ソート。
    ゼロ埋め未使用スロット・桁不足・重複・0 馬番は None（`_valid_pair` と同方針）。
    """
    if combo is None or not str(combo).strip():
        return None
    canon = normalize_combo(combo, ordered=ordered)
    parts = [p for p in canon.split("-") if p.strip()]
    if len(parts) != size:
        return None
    ints = []
    for p in parts:
        if not p.isdigit():
            return None
        v = int(p)
        if v <= 0:
            return None
        ints.append(v)
    if len(set(ints)) != size:
        return None
    return tuple(ints)


def build_hjc_table(hjc: pd.DataFrame, prefix: str, size: int, occ: int,
                    ordered: bool) -> pd.DataFrame:
    """raw_jrdb_hjc の1券種を index=race_id の win_i/return_i テーブルにする。

    単複（size=1）は win_i=int 馬番（`_SingleStrategy` が int 比較）、連系（size≥2）は
    win_i=馬番タプル（`_Combo/_PermStrategy` が tuple 比較）。当選のみ格納・非当選は 0 埋め。
    object dtype で保持し `.loc` でのタプル/型崩れを防ぐ（`build_single_table` と同方針）。
    """
    rows: dict = {}
    max_slots = 1
    for _, r in hjc.iterrows():
        rid = str(r.get("race_id")).split(".")[0]
        placed = []
        for i in range(1, occ + 1):
            combo = _combo_to_ints(r.get(f"{prefix}_combo{i}"), size, ordered)
            pay = pd.to_numeric(r.get(f"{prefix}_pay{i}"), errors="coerce")
            if combo is None or pd.isna(pay) or pay <= 0:
                continue
            placed.append((combo[0] if size == 1 else combo, float(pay)))
        if placed:
            rows[rid] = placed
            max_slots = max(max_slots, len(placed))
    if not rows:
        return pd.DataFrame()
    out_rows: dict = {}
    for rid, placed in rows.items():
        row: dict = {}
        for i in range(max_slots):
            if i < len(placed):
                row[f"win_{i}"], row[f"return_{i}"] = placed[i][0], placed[i][1]
            else:
                row[f"win_{i}"], row[f"return_{i}"] = 0, 0.0
        out_rows[rid] = row
    return pd.DataFrame.from_dict(out_rows, orient="index").astype(object)


class JrdbHjcReturnSource:
    """ReturnProcessor 互換（.preprocessed_data のみ）の JRDB HJC 払戻源。全8券種を 100% 供給。

    `BettingTickets`/`_backtest.settle_candidates` にそのまま渡せる。連系の券種別 ROI
    バックテスト（sim 着順標本→買い目→確定払戻決済）の決済側はこれ 1 つで賄える。
    """

    def __init__(self, engine, hjc: pd.DataFrame | None = None) -> None:
        if hjc is None:
            from sqlalchemy import text
            hjc = pd.read_sql(text("SELECT * FROM raw_jrdb_hjc"), engine)
        self._data = {}
        for prefix, bet_type, size, occ in _HJC_SPEC:
            ordered = BET_ORDERED.get(prefix, size == 1)
            self._data[bet_type] = build_hjc_table(hjc, prefix, size, occ, ordered)

    @property
    def preprocessed_data(self) -> dict:
        return self._data

    def coverage(self, race_ids, bet_type: BetType = BetType.SANRENTAN) -> float:
        """指定 race_id 群のうち bet_type 払戻テーブルに存在する割合（0.0–1.0）。"""
        table = self._data.get(bet_type)
        if table is None or table.empty:
            return 0.0
        want = {str(r).split(".")[0] for r in race_ids}
        if not want:
            return 0.0
        idx = set(map(str, table.index))
        return sum(1 for r in want if r in idx) / len(want)
