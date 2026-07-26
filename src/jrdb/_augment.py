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

# KYI パース列 → featured 付与名（jrdb_*）。市場から導けない JRDB 独自指数を優先収録。
# pace_yosou（H/M/S）は jrdb_pace_hms に数値化（S=-1/M=0/H=+1）して別途付与する。
KYI_FEATURE_MAP = {
    "idm": "jrdb_idm",
    "kishu_idx": "jrdb_kishu_idx",
    "joho_idx": "jrdb_joho_idx",
    "sougou_idx": "jrdb_sougou_idx",
    "kyakushitsu": "jrdb_kyakushitsu",
    "kyori_tekisei": "jrdb_kyori_tekisei",
    "joushoudo": "jrdb_joushoudo",
    "rotation": "jrdb_rotation",
    "kijun_odds": "jrdb_kijun_odds",
    "kijun_fukuodds": "jrdb_kijun_fukuodds",
    "ninki_idx": "jrdb_ninki_idx",
    "chokyo_idx": "jrdb_chokyo_idx",
    "kyusha_idx": "jrdb_kyusha_idx",
    "chokyo_yajirushi": "jrdb_chokyo_yajirushi",
    "kyusha_hyoka": "jrdb_kyusha_hyoka",
    "kishu_kitai_rentai": "jrdb_kishu_kitai_rentai",
    "gekiso_idx": "jrdb_gekiso_idx",
    "class_code": "jrdb_class_code",
    "ten_idx": "jrdb_ten_idx",
    "pace_idx": "jrdb_pace_idx",
    "agari_idx": "jrdb_agari_idx",
    "ichi_idx": "jrdb_ichi_idx",
    "dochu_juni": "jrdb_dochu_juni",
    "go3f_juni": "jrdb_go3f_juni",
    "goal_juni": "jrdb_goal_juni",
    "kakutei_bataijuu": "jrdb_kakutei_bataijuu",
    "kokyu_flag": "jrdb_kokyu_flag",
    "start_idx": "jrdb_start_idx",
    "deokure_rate": "jrdb_deokure_rate",
    "manken_idx": "jrdb_manken_idx",
    "kishu_tansho": "jrdb_kishu_tansho",
    "kishu_3nai": "jrdb_kishu_3nai",
    "nyukyu_days": "jrdb_nyukyu_days",
}
_HMS = {"H": 1.0, "M": 0.0, "S": -1.0}

# 脚質コード（JRDBデータコード表）→ 正準4脚質（Mixture-PL の β(style,z) 行キー）。
# JRDB の 6 分類を 4 に畳む: 5好位差し→sashi(好位からの差し) / 6自在→senko(中庸を先行群へ)。
# pace_median 由来の style_from_pace_ratio より JRDB の明示分類が優先（今走の予想脚質）。
JRDB_STYLE_TO_CANONICAL = {
    1: "nige", 2: "senko", 3: "sashi", 4: "oikomi", 5: "sashi", 6: "senko",
}


def jrdb_style(kyakushitsu_code) -> str | None:
    """jrdb_kyakushitsu（1-6）→ 正準脚質（nige/senko/sashi/oikomi）。不明は None。"""
    try:
        return JRDB_STYLE_TO_CANONICAL.get(int(kyakushitsu_code))
    except (TypeError, ValueError):
        return None

# JRDB 由来の付与列（VOI 評価の A/B で「JRDB あり/なし」を切り替える対象の正本）。
# prev_* は接頭辞が異なるため、train_residual の --drop-jrdb はこの集合を落とす。
JRDB_COLS = (
    list(KYI_FEATURE_MAP.values())
    + ["jrdb_pace_hms", "jrdb_kijun_gap", "prev_deokure", "prev_trouble"]
)

# 走行中の不利＝「着順が実力を過小評価している」隠れ妙味シグナル（卍の核・市場と直交）。
# 公式・特記コード表（2026.04.13）から、外的要因による進路妨害/接触/外々/ブレーキに厳選。
# 除外: スタート系(059/158→prev_slowstart)・脚質/適性/馬場向き・故障/病気（能力や状態であって
# 「不利で負けた」ではないため）・展開恵まれ(820=むしろ逆シグナル)。
TROUBLE_TOKKI = {
    # 接触・妨害
    "174",  # ラチ接触
    "179",  # 外から被せられる×
    "199",  # 外から被せられる×
    "309",  # 他馬と接触
    "387",  # 不利
    "413",  # 躓く
    "448",  # バランス崩す
    "787",  # ゴチャつく
    "806",  # ヨレる
    "876",  # 直線挟まる
    "958",  # 寄られる
    # 進路なし・詰まり・壁
    "955",  # 蓋される
    "956",  # 勝負所で蓋
    "957",  # 直線で前が壁
    "960",  # 囲まれて出れず
    "961",  # 囲まれて追えず
    "964",  # 道中ブレーキ踏む
    "965",  # 直線ブレーキ踏む
    # 外々・距離ロス
    "415",  # 大外回る
    "718",  # 道中外々
    "945",  # コーナー逆手前
    "948",  # 外ラチ沿いを進む
    "949",  # 序盤外回る
    "950",  # 外回りすぎ
    # 位置取り・展開の不利
    "819",  # 展開厳しい
    "954",  # 位置取りが悪い
}

# 前走の出遅れ（スタート悪い/ダッシュ不足）— deokure を SKB からも補完（SED数値が空の場合の保険）。
SLOWSTART_TOKKI = {
    "059",  # スタート悪い
    "158",  # ダッシュ×
    "434",  # スタート芝×
    "703",  # 発進不良
    "716",  # 発進不良
}


def build_kyi(paths: list[str]) -> pd.DataFrame:
    """複数 KYI を結合し (race_id, umaban) 単位で JRDB 指数群（jrdb_*）を返す。

    KYI_FEATURE_MAP の全指数＋ペース予想 H/M/S を数値化した jrdb_pace_hms を含む。
    """
    dfs = [parse(p, "KYI") for p in paths]
    df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    if df.empty:
        return df
    keep = ["race_id", "umaban", "ketto", *KYI_FEATURE_MAP.keys()]
    out = df[[c for c in keep if c in df.columns]].rename(columns=KYI_FEATURE_MAP)
    if "pace_yosou" in df.columns:
        out["jrdb_pace_hms"] = df["pace_yosou"].str.strip().map(_HMS)
    return out


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
        slowstart = d[tk].apply(lambda row: int(any(x in SLOWSTART_TOKKI for x in row)), axis=1)
        frames.append(pd.DataFrame({"ketto": d["ketto"], "ymd": d["ymd"],
                                    "prev_deokure": slowstart.to_numpy(),
                                    "prev_trouble": trouble.to_numpy()}))
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
        if odds_col in f.columns and "jrdb_kijun_odds" in f.columns:
            mkt = pd.to_numeric(f[odds_col], errors="coerce")
            f["jrdb_kijun_gap"] = f["jrdb_kijun_odds"] / mkt   # >1: 基準が市場より甘い=過小評価
    else:
        for c in JRDB_COLS:
            if c not in ("prev_deokure", "prev_trouble"):
                f[c] = pd.NA

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
