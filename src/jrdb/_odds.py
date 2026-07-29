"""JRDB 基準オッズ（OZ/OW/OU/OT/OV）パーサ — 券種別に全組合せの基準オッズを展開。

各レコードはレース単位で、券種ごとに全組合せ（馬連153/ワイド153/馬単306/3連複816/
3連単4896）の基準オッズ（JRDB のフェアバリュー）を固定長で並べる。列展開は非現実的
（3連単4896列）なので **long 形式**（race_id, bet, combo, odds）で返す。raw_jrdb_* には
保存せず、HJC（実払戻）と対の EV 検証（`scripts/jrdb_odds_ev_check.py`）が読む。

組合せの並び順は仕様書どおり（馬単=1-2..1-18,2-1..最後18-17／3連単=01-02-03..最後
18-17-16）。3連単のみ 0.1倍単位（他は小数点付き）。取消は 9999.9 / 9999999。
"""
from __future__ import annotations

import pandas as pd

from src.jrdb._keys import race_key_to_race_id
from src.jrdb._parser import _records, _slice

_N = 18  # 最大馬番


def _kumi_tan() -> list[tuple[int, ...]]:
    return [(i,) for i in range(1, _N + 1)]                      # 18（単勝/複勝）


def _kumi_umaren() -> list[tuple[int, ...]]:
    return [(i, j) for i in range(1, _N + 1) for j in range(i + 1, _N + 1)]  # 153


def _kumi_umatan() -> list[tuple[int, ...]]:
    # 1-2..1-18, 2-1,2-3..2-18, … 最後 18-17
    return [(i, j) for i in range(1, _N + 1) for j in range(1, _N + 1) if j != i]  # 306


def _kumi_sanrenpuku() -> list[tuple[int, ...]]:
    return [(i, j, k) for i in range(1, _N + 1) for j in range(i + 1, _N + 1)
            for k in range(j + 1, _N + 1)]                       # 816


def _kumi_sanrentan() -> list[tuple[int, ...]]:
    # 01-02-03 .. 最後 18-17-16（相異なる順序付き3つ組）
    return [(i, j, k) for i in range(1, _N + 1) for j in range(1, _N + 1)
            for k in range(1, _N + 1) if len({i, j, k}) == 3]    # 4896


# kind → [(bet, 開始相対, 1件byte, 組合せ列挙関数, 単位)]。単位 dec=小数点付き / tenths=0.1倍単位。
ODDS_SPEC = {
    "OZ": [("tansho", 11, 5, _kumi_tan, "dec"),
           ("fukusho", 101, 5, _kumi_tan, "dec"),
           ("umaren", 191, 5, _kumi_umaren, "dec")],
    "OW": [("wide", 11, 5, _kumi_umaren, "dec")],       # ワイドは馬連と同じ 153 組（i<j）
    "OU": [("umatan", 11, 6, _kumi_umatan, "dec")],
    "OT": [("sanrenpuku", 11, 6, _kumi_sanrenpuku, "dec")],
    "OV": [("sanrentan", 11, 7, _kumi_sanrentan, "tenths")],
}
RECORD_LEN_ODDS = {"OZ": 957, "OW": 780, "OU": 1856, "OT": 4912, "OV": 34288}

# 取消コード（無効オッズ）。
_CANCEL = {"dec": "9999.9", "tenths": "9999999"}


def _parse_val(field: str, mode: str):
    """オッズ1件を float に。空/0/取消は None。tenths は整数×0.1。"""
    s = field.strip()
    if not s or s == _CANCEL[mode]:
        return None
    if mode == "tenths":
        if not s.isdigit():
            return None
        v = int(s) / 10.0
    else:
        try:
            v = float(s)
        except ValueError:
            return None
    return v if v > 0 else None


def parse_odds(path: str, kind: str) -> pd.DataFrame:
    """基準オッズファイルを long 形式 (race_id, bet, combo, odds) にする。

    combo は 2桁ゼロ埋めのハイフン連結（"07" / "03-11" / "01-02-03"）。登録頭数を超える
    馬番を含む組合せ・取消・空欄は除外する。kind ∈ {OZ,OW,OU,OT,OV}。
    """
    kind = kind.upper()
    if kind not in ODDS_SPEC:
        raise ValueError(f"未対応の基準オッズ種別: {kind}（対応: {list(ODDS_SPEC)}）")
    recs = _records(path)
    rows: list[dict] = []
    for r in recs:
        race_id = race_key_to_race_id(_slice(r, 1, 8))
        toroku = pd.to_numeric(_slice(r, 9, 2).strip(), errors="coerce")
        nmax = int(toroku) if pd.notna(toroku) and toroku > 0 else _N
        for bet, start, width, kumi_fn, mode in ODDS_SPEC[kind]:
            for idx, combo in enumerate(kumi_fn()):
                if max(combo) > nmax:            # 登録頭数を超える馬番の組合せは無効
                    continue
                odds = _parse_val(_slice(r, start + idx * width, width), mode)
                if odds is None:
                    continue
                rows.append({"race_id": race_id, "bet": bet,
                             "combo": "-".join(f"{c:02d}" for c in combo), "odds": odds})
    return pd.DataFrame(rows, columns=["race_id", "bet", "combo", "odds"]) if rows \
        else pd.DataFrame(columns=["race_id", "bet", "combo", "odds"])


def check_odds_length(path: str, kind: str, *, tolerance: int = 2):
    """レコード長が仕様（RECORD_LEN_ODDS）と ±tolerance 内か。(ok, dominant, expected)。"""
    from collections import Counter
    recs = _records(path)
    dom = Counter(len(r) for r in recs).most_common(1)[0][0] if recs else 0
    exp = RECORD_LEN_ODDS.get(kind.upper())
    if exp is None or dom == 0:
        return True, dom, exp
    return abs(dom - exp) <= tolerance, dom, exp


def favorites(odds_long: pd.DataFrame, *, top: int = 1) -> pd.DataFrame:
    """race_id×bet ごとに基準オッズ最小（＝JRDB フェア本命）の上位 top 組を返す。

    EV 検証で「JRDB が最も来ると見た組合せ」を賭ける対象にするための抽出。
    """
    if odds_long is None or odds_long.empty:
        return odds_long
    return (odds_long.sort_values(["race_id", "bet", "odds"])
            .groupby(["race_id", "bet"], as_index=False).head(top)
            .reset_index(drop=True))


# 券種の順序性（True=着順あり＝馬単/3連単・単複、False=順不同＝馬連/ワイド/3連複）。
BET_ORDERED = {"tansho": True, "fukusho": True, "umaren": False, "wide": False,
               "umatan": True, "sanrenpuku": False, "sanrentan": True}


def normalize_combo(combo: object, *, ordered: bool) -> str:
    """組合せを正準化（HJC の連結 '070311' と 基準オッズの '07-03-11' を突合可能に）。

    ordered=False（馬連/ワイド/3連複）は馬番を昇順ソートして順序差を吸収する。
    """
    s = str(combo).replace("-", "").strip()
    horses = [s[i:i + 2] for i in range(0, len(s), 2)]
    if not ordered:
        horses = sorted(horses)
    return "-".join(horses)


def _selftest_combos() -> bool:
    """組合せ列挙が仕様の件数・末尾と一致するか（テスト用）。"""
    return (len(_kumi_tan()) == 18 and len(_kumi_umaren()) == 153
            and len(_kumi_umatan()) == 306 and len(_kumi_sanrenpuku()) == 816
            and len(_kumi_sanrentan()) == 4896
            and _kumi_umatan()[-1] == (18, 17)
            and _kumi_sanrentan()[-1] == (18, 17, 16))
