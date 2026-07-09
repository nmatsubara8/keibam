"""JRDB 由来の特徴量を featured に付与する。

- 今走: (race_id, 馬番) で KYI の 基準オッズ・IDM を貼る。
  基準オッズ乖離 = 基準オッズ / 市場単勝（Benter核: 専有フェアバリューと市場の差）。
- 前走: 血統登録番号で馬の履歴を辿り、前走(年月日<今走)の SKB特記(不利/道中外々等)・
  SED出遅 を貼る（リーク無し。merge_asof backward・exact不可）。

featured は代理 horse_id を使わず (race_id,馬番)＋JRDB血統登録番号で連結する。
"""
from __future__ import annotations

import pandas as pd

from src.jrdb._parser import parse

# 「前走で不利/口取りロス」を示す特記コード（不利・接触・詰まり・進路無し系）
TROUBLE_TOKKI = {
    "387",  # 不利
    "718",  # 道中外々
    "876",  # 直線挟る
    "954",  # 位置取りが悪い
    "957",  # 直線で前が壁
    "174",  # ラチ接触
    "309",  # 他馬接触
    "119",  # コーナーワーク×
    "156",  # ふらつく
    "413",  # 躓く
    "221",  # 障害接触
}


def build_kyi(paths: list[str]) -> pd.DataFrame:
    """複数 KYI を結合し (race_id, umaban) 単位で基準オッズ/IDM/血統登録を返す。"""
    dfs = [parse(p, "KYI") for p in paths]
    df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    if df.empty:
        return df
    return df[["race_id", "umaban", "ketto", "idm", "kijun_odds", "kijun_ninki"]].rename(
        columns={"idm": "jrdb_idm", "kijun_odds": "jrdb_kijun_odds"})


def build_history(sed_paths: list[str], skb_paths: list[str]) -> pd.DataFrame:
    """SED/SKB を結合し (ketto, 年月日=date) 単位の過去走トラブル指標を返す。

    列: ketto, hist_date, prev_deokure(0/1), prev_trouble(0/1)。
    """
    frames = []
    for p in sed_paths:
        d = parse(p, "SED")
        d = d[["ketto", "ymd", "deokure"]].copy()
        d["prev_deokure"] = (pd.to_numeric(d["deokure"], errors="coerce") > 0).astype(int)
        d["prev_trouble"] = 0
        frames.append(d[["ketto", "ymd", "prev_deokure", "prev_trouble"]])
    for p in skb_paths:
        d = parse(p, "SKB")
        tk = [c for c in d.columns if c.startswith("tokki")]
        trouble = d[tk].apply(lambda row: int(any(x in TROUBLE_TOKKI for x in row)), axis=1)
        frames.append(pd.DataFrame({"ketto": d["ketto"], "ymd": d["ymd"],
                                    "prev_deokure": 0, "prev_trouble": trouble.to_numpy()}))
    if not frames:
        return pd.DataFrame(columns=["ketto", "hist_date", "prev_deokure", "prev_trouble"])
    h = pd.concat(frames, ignore_index=True)
    # 同一(ketto,ymd)の SED/SKB を集約（どちらかが立てば1）
    g = h.groupby(["ketto", "ymd"], as_index=False)[["prev_deokure", "prev_trouble"]].max()
    g["hist_date"] = pd.to_datetime(g["ymd"], format="%Y%m%d", errors="coerce")
    return g.dropna(subset=["hist_date"])[["ketto", "hist_date", "prev_deokure", "prev_trouble"]]


def attach(featured: pd.DataFrame, kyi: pd.DataFrame, history: pd.DataFrame,
           *, umaban_col: str = "馬番", odds_col: str = "単勝") -> pd.DataFrame:
    """featured に JRDB 列を付与して返す（元は非改変・コピー）。

    追加列: jrdb_idm, jrdb_kijun_odds, jrdb_kijun_gap(=基準/市場), prev_deokure, prev_trouble。
    """
    orig_index = featured.index
    f = featured.reset_index(drop=True).copy()
    f["_pos"] = range(len(f))
    f["_rid"] = orig_index.astype(str)
    f["_uma"] = pd.to_numeric(f[umaban_col], errors="coerce").astype("Int64")

    if kyi is not None and not kyi.empty:
        k = kyi.drop_duplicates(["race_id", "umaban"])
        f = f.merge(k, left_on=["_rid", "_uma"], right_on=["race_id", "umaban"], how="left")
        f = f.sort_values("_pos").reset_index(drop=True)  # 左順を保証
        if odds_col in f.columns:
            mkt = pd.to_numeric(f[odds_col], errors="coerce")
            f["jrdb_kijun_gap"] = f["jrdb_kijun_odds"] / mkt   # >1: 基準が市場より甘い=過小評価
    else:
        f["jrdb_idm"] = pd.NA
        f["jrdb_kijun_odds"] = pd.NA
        f["jrdb_kijun_gap"] = pd.NA

    # 前走トラブル: ketto × (年月日<今走) の直近を merge_asof(backward, exact不可)
    if history is not None and not history.empty and "ketto" in f.columns:
        today = pd.to_datetime(f["date"], errors="coerce")
        sub = pd.DataFrame({"_pos": f["_pos"], "ketto": f["ketto"], "_today": today})
        sub = sub.dropna(subset=["ketto", "_today"]).sort_values("_today")
        hist = history.sort_values("hist_date")
        m = pd.merge_asof(sub, hist, by="ketto", left_on="_today", right_on="hist_date",
                          direction="backward", allow_exact_matches=False)
        pv = m.set_index("_pos")[["prev_deokure", "prev_trouble"]]
        f = f.merge(pv, left_on="_pos", right_index=True, how="left")
    else:
        f["prev_deokure"] = pd.NA
        f["prev_trouble"] = pd.NA

    f = f.sort_values("_pos")
    drop = [c for c in ("_pos", "_rid", "_uma", "_today", "race_id", "umaban", "ketto",
                        "jrdb_kijun_ninki") if c in f.columns]
    f = f.drop(columns=drop)
    f.index = orig_index
    return f
