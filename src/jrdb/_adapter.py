"""JRDB raw を netkeiba raw スキーマへ写すアダプタ（上書き統合 Phase2）。

netkeiba の年度欠損（2021 部分・2022 全欠）を JRDB(中央) で補完するため、JRDB の
raw_jrdb_* を netkeiba の raw_results / raw_race_info と同じ列・値へ変換する。ここで
生成した行を欠損年ぶんだけ netkeiba raw に union すれば、既存の featured 生成が
そのまま全年で走る（NAR は JRDB に無いので上書きせず温存）。

同一性（horse_id/jockey_id/trainer_id）は `_crosswalk` の対応表で JRDB コードから
付与する。重複年の (race_id,馬番) 突合で作った対応なので決定的。
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from src.jrdb._keys import ketto_to_horse_id

# netkeiba は非完走（異常区分 1取消/2除外/3中止/4失格/5降着…）の着順を NaN で持つ
# （2018 実突合で 419/419 が netkeiba NaN と確認）。マーカーは付けず None にする。


def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _time_str(v: object) -> Optional[str]:
    """SED タイム(1byte分+3byte秒0.1) '1234' → 'M:SS.s'（例 1:23.4）。空/不正は None。"""
    if v is None:
        return None
    s = str(v).strip()
    if len(s) < 4 or not s.isdigit():
        return None
    minutes = int(s[0])
    tenths = int(s[1:4])          # 0.1 秒単位（0〜999）
    if minutes == 0 and tenths == 0:
        return None               # 全ゼロ＝タイム無し（非完走。netkeiba は NaN）
    return f"{minutes}:{tenths / 10:04.1f}"


def _tenths_to_sec(v: object) -> Optional[float]:
    """0.1 秒単位の3桁 '345' → 34.5 秒。空/不正は None。"""
    s = "" if v is None else str(v).strip()
    if not s or not s.lstrip("-").isdigit():
        return None
    return int(s) / 10.0


def _passing(row: pd.Series) -> Optional[str]:
    """コーナー順位1..4 → netkeiba 通過 'a-b-c-d'（空コーナーは詰める）。"""
    vals = []
    for c in ("corner1", "corner2", "corner3", "corner4"):
        x = row.get(c)
        n = pd.to_numeric(x, errors="coerce")
        if pd.notna(n) and n > 0:
            vals.append(str(int(n)))
    return "-".join(vals) if vals else None


def _bataijuu_str(w: object, z: object) -> Optional[str]:
    """馬体重 + 増減 → netkeiba '498(-10)'/'508(0)'。増減が空なら '508'。体重空は None。

    netkeiba は増減0でも '(0)' を残すため、増減が非空なら常に括弧を付ける。
    """
    ws = "" if w is None else str(w).strip()
    zs = "" if z is None else str(z).strip().replace(" ", "")
    if not ws or ws == "0":
        return None
    return f"{ws}({zs})" if zs else ws


def _chakujun_str(chaku: object, ijo: object) -> Optional[str]:
    """非完走（異常区分あり）は None（netkeiba は着順 NaN）。完走は着順数字。"""
    ijs = "" if ijo is None else str(ijo).strip()
    if ijs and ijs != "0":            # 取消/除外/中止/失格/降着 = 非完走
        return None
    n = pd.to_numeric(chaku, errors="coerce")
    return str(int(n)) if pd.notna(n) and n > 0 else None


# 性別コード（KYI/UKC sex_code）→ netkeiba 性齢の先頭文字。
_SEX_BY_CODE = {"1": "牡", "2": "牝", "3": "セ"}


def _seirei(sex_code: object, race_id: object, horse_id: object) -> Optional[str]:
    """性別コード + (race_id, horse_id) → netkeiba 性齢 '牡3'。

    馬齢は数え歳＝レース年 − 生年。生年は horse_id 先頭4桁（= ketto_to_horse_id が
    血統登録番号から復元した生年）。性別/年齢が取れなければ None。
    """
    sei = _SEX_BY_CODE.get(str(sex_code).strip()) if sex_code is not None else None
    if sei is None or horse_id is None:
        return None
    try:
        age = int(str(race_id)[:4]) - int(str(horse_id)[:4])
    except (ValueError, TypeError):
        return None
    return f"{sei}{age}" if 1 <= age <= 30 else None


def _kyi_waku_sex(kyi: Optional[pd.DataFrame]) -> tuple[dict, dict]:
    """KYI（出馬表・race_id×馬番）→ {(race_id,馬番): 枠番}, {(race_id,馬番): 性別コード}。

    SED に無い 枠番・性別を (race_id, 馬番) で結合するためのルックアップ。馬番は int で正準化。
    """
    if kyi is None or kyi.empty or "umaban" not in kyi.columns or "race_id" not in kyi.columns:
        return {}, {}
    rid = kyi["race_id"].astype(str)
    um = pd.to_numeric(kyi["umaban"], errors="coerce")
    waku = pd.to_numeric(kyi["wakuban"], errors="coerce") if "wakuban" in kyi.columns else None
    sexc = kyi["sex_code"].astype(str).str.strip() if "sex_code" in kyi.columns else None
    waku_map: dict = {}
    sex_map: dict = {}
    for i in range(len(kyi)):
        u = um.iat[i]
        if pd.isna(u):
            continue
        key = (rid.iat[i], int(u))
        if waku is not None and pd.notna(waku.iat[i]) and waku.iat[i] > 0:
            waku_map[key] = int(waku.iat[i])
        if sexc is not None:
            sex_map[key] = sexc.iat[i]
    return waku_map, sex_map


def _xwalk_map(xw: Optional[pd.DataFrame], code_col: str, id_col: str) -> dict:
    """crosswalk DataFrame → {jrdb_code: netkeiba_id} 辞書。None/空なら空辞書。"""
    if xw is None or xw.empty or code_col not in xw.columns or id_col not in xw.columns:
        return {}
    d = xw[[code_col, id_col]].dropna()
    return dict(zip(d[code_col].astype(str), d[id_col].astype(str), strict=False))


# ── raw_race_info 用のコード→netkeiba 文字列マップ ──
# 場コード(2桁)→競馬場名（中央01-10。netkeiba と同名）。
PLACE_BY_CODE = {1: "札幌", 2: "函館", 3: "福島", 4: "新潟", 5: "東京",
                 6: "中山", 7: "中京", 8: "京都", 9: "阪神", 10: "小倉"}
RACE_TYPE = {"1": "芝", "2": "ダート", "3": "障害"}
AROUND = {"1": "右", "2": "左", "3": "直線"}   # 9(他)は未マップ→None
# 天候コード（JRDBデータコード表・標準）。重複年で要検証。
WEATHER = {"1": "晴", "2": "曇", "3": "小雨", "4": "雨", "5": "小雪", "6": "雪"}
# 馬場状態コード → going。2018 実突合＋コード表で確定: **十の位**が going
# （1x:良 / 2x:稍重 / 3x:重 / 4x:不良。一の位は 速/遅 のサブレベル）。
GROUND_BY_TENS = {"1": "良", "2": "稍重", "3": "重", "4": "不良"}
# 種別コード → 対象最低年齢（netkeiba age は数値。要 netkeiba 実値検証）。
SHUBETSU_TO_AGE = {"11": "2", "12": "3", "13": "3", "14": "4"}
# 条件コード → race_class（netkeiba 表記。2019 改称 500万下→1勝クラス。要検証）。
JOKEN_TO_CLASS = {
    "04": "1勝クラス", "05": "1勝クラス", "08": "2勝クラス", "09": "2勝クラス",
    "10": "2勝クラス", "15": "3勝クラス", "16": "3勝クラス",
    "A1": "新馬", "A2": "未勝利", "A3": "未勝利", "OP": "オープン",
}


def _col(d: pd.DataFrame, name: str) -> pd.Series:
    """列があれば str 化 strip した Series、無ければ全 NA の Series。"""
    if name in d.columns:
        return d[name].astype(str).str.strip()
    return pd.Series([pd.NA] * len(d), index=d.index, dtype=object)


def _ymd_to_jp(v: object) -> Optional[str]:
    """YYYYMMDD → 'YYYY年MM月DD日'（netkeiba date 形式）。"""
    s = "" if v is None else str(v).strip()
    if len(s) != 8 or not s.isdigit():
        return None
    return f"{s[0:4]}年{s[4:6]}月{s[6:8]}日"


def _hhmm_colon(v: object) -> Optional[str]:
    """HHMM → 'HH:MM'。空/不正は None。"""
    s = "" if v is None else str(v).strip()
    if len(s) < 4 or not s[:4].isdigit():
        return None
    return f"{s[0:2]}:{s[2:4]}"


def _place_id(code2: str) -> Optional[str]:
    return str(int(code2)) if code2.isdigit() else None


# raw_horse_results の距離表記は短縮（芝/ダ/障）。raw_race_info の race_type(芝/ダート)とは別。
RACE_TYPE_SHORT = {"1": "芝", "2": "ダ", "3": "障"}


def _ymd_slash(v: object) -> Optional[str]:
    """YYYYMMDD → 'YYYY/MM/DD'（netkeiba horse_results 日付形式）。"""
    s = "" if v is None else str(v).strip()
    if len(s) != 8 or not s.isdigit():
        return None
    return f"{s[0:4]}/{s[4:6]}/{s[6:8]}"


def _chakusa(sa_raw: object, chakujun: object) -> Optional[float]:
    """1(2)着タイム差(0.1秒) → 着差(秒)。netkeiba は勝ち馬(着順1)の着差を負で持つ。"""
    s = _tenths_to_sec(sa_raw)
    if s is None:
        return None
    c = pd.to_numeric(chakujun, errors="coerce")
    return -s if (pd.notna(c) and c == 1) else s


def _kyori_str(shiba_dirt: object, kyori: object) -> Optional[str]:
    """芝ダ障コード + 距離 → netkeiba '芝1400'。"""
    st = RACE_TYPE_SHORT.get(str(shiba_dirt).strip())
    k = str(kyori).strip()
    if not st or not k.isdigit():
        return None
    return f"{st}{int(k)}"


def build_raw_race_info(sed: pd.DataFrame) -> pd.DataFrame:
    """JRDB SED（レース条件を含む）→ netkeiba raw_race_info 相当（index=race_id）。

    SED は出走馬単位だが、レース条件はレース内で同一なので race_id で畳む。距離/芝ダ/
    回り/馬場/天候/発走時刻＋場コードからレースメタを生成。code→文字列は JRDBデータ
    コード表で確定済み。

    ⚠️ fill 方針: netkeiba は place/around/time/age/race_class を **2023 以降のみ**充填
    （1986-2022 は空）。2021-2022 補完でこれらを埋めると 2023 前後で分布が不連続になる
    ため、fill 時は全年充填される列（race_type/weather/ground_state1/2/course_len/date）
    だけを使い、recent-only 列は NaN に落とすこと（`fill_columns_for_year` 参照）。
    """
    if sed is None or sed.empty:
        return pd.DataFrame()
    d = sed.drop_duplicates("race_id", keep="first").reset_index(drop=True)
    rid = d["race_id"].astype(str)
    code2 = rid.str[4:6]
    out = pd.DataFrame(index=range(len(d)))
    out["race_id"] = rid.to_numpy()
    out["place_id"] = code2.map(_place_id).to_numpy()
    out["place"] = code2.map(lambda c: PLACE_BY_CODE.get(int(c)) if c.isdigit() else None).to_numpy()
    out["times"] = rid.str[6:8].map(lambda c: str(int(c)) if c.isdigit() else None).to_numpy()
    out["days"] = rid.str[8:10].map(lambda c: str(int(c)) if c.isdigit() else None).to_numpy()
    out["date"] = [_ymd_to_jp(v) for v in _col(d, "ymd")]
    out["time"] = [_hhmm_colon(v) for v in _col(d, "hassou_time")]
    out["course_len"] = pd.to_numeric(_col(d, "kyori"), errors="coerce").to_numpy()
    out["race_type"] = _col(d, "shiba_dirt").map(RACE_TYPE).to_numpy()
    out["around"] = _col(d, "migi_hidari").map(AROUND).to_numpy()
    out["weather"] = _col(d, "tenko_code").map(WEATHER).to_numpy()
    gs = _col(d, "baba_state").str[0].map(GROUND_BY_TENS)  # 十の位が going
    out["ground_state1"] = gs.to_numpy()
    out["ground_state2"] = gs.to_numpy()
    out["age"] = _col(d, "shubetsu").map(SHUBETSU_TO_AGE).to_numpy()
    out["race_class"] = _col(d, "joken").map(JOKEN_TO_CLASS).to_numpy()
    return out.set_index("race_id")


def build_raw_horse_results(sed: pd.DataFrame) -> pd.DataFrame:
    """JRDB SED → netkeiba raw_horse_results 相当（馬ごとの過去走履歴。index なし）。

    raw_results と同じ SED 由来だが horse_id×日付キーの馬履歴。2021-2022 の走りを各馬の
    履歴に足すと、その後（2023+）のレースの過去走特徴量も正しくなる。horse_id は
    **ketto_to_horse_id（血統登録番号→生年+下6桁）= netkeiba canonical id**。raw_horse_results
    と直近スクレイプ年の horse_id はこの方式（crosswalk の seed代理 9… ではない）。
    日付='YYYY/MM/DD'・距離='芝1400'・馬場体重='498(-10)'・着差=秒(数値) 等に合わせる。
    """
    if sed is None or sed.empty:
        return pd.DataFrame()
    d = sed.copy()
    rid = d["race_id"].astype(str)
    out = pd.DataFrame(index=range(len(d)))
    out["horse_id"] = _col(d, "ketto").map(ketto_to_horse_id).to_numpy()
    out["日付"] = [_ymd_slash(v) for v in _col(d, "ymd")]
    out["開催"] = rid.str[4:6].map(
        lambda c: PLACE_BY_CODE.get(int(c)) if c.isdigit() else None).to_numpy()
    out["天気"] = _col(d, "tenko_code").map(WEATHER).to_numpy()
    out["R"] = pd.to_numeric(rid.str[10:12], errors="coerce").to_numpy()
    out["レース名"] = _col(d, "race_name").to_numpy()
    out["頭数"] = _num(_col(d, "toushuu")).to_numpy()
    out["馬番"] = _num(_col(d, "umaban")).to_numpy()
    out["オッズ"] = _num(_col(d, "kakutei_tansho")).to_numpy()
    out["人気"] = _num(_col(d, "kakutei_ninki")).to_numpy()
    out["着順"] = [_chakujun_str(c, i) for c, i in
                 zip(_col(d, "chakujun"), _col(d, "ijo_kubun"), strict=False)]
    out["騎手"] = _col(d, "kishu_name").to_numpy()
    out["斤量"] = (_num(_col(d, "futan_juryo")) / 10).to_numpy()
    out["距離"] = [_kyori_str(s, k) for s, k in
                 zip(_col(d, "shiba_dirt"), _col(d, "kyori"), strict=False)]
    out["馬場"] = _col(d, "baba_state").str[0].map(GROUND_BY_TENS).to_numpy()
    out["タイム"] = [_time_str(v) for v in _col(d, "time")]
    out["着差"] = [_chakusa(v, c) for v, c in
                 zip(_col(d, "chaku1_time_sa"), _col(d, "chakujun"), strict=False)]  # 秒・勝ち馬は負
    out["通過"] = [_passing(r) for _, r in d.iterrows()]
    out["上り"] = [_tenths_to_sec(v) for v in _col(d, "ato3f_time")]
    out["馬体重"] = [_bataijuu_str(w, z) for w, z in
                   zip(_col(d, "bataijuu"), _col(d, "bataijuu_zougen"), strict=False)]
    out["勝ち馬(2着馬)"] = _col(d, "chaku1_bamei").to_numpy()
    hon = _num(_col(d, "honshokin"))
    out["賞金"] = hon.where(hon > 0).to_numpy()      # 0/無し は NaN（netkeiba 同様）
    # netkeiba 固有・SED に無い列は欠損で確保
    for c in ("枠番", "水分量", "馬場指数", "ﾀｲﾑ指数", "ペース", "映像", "厩舎ｺﾒﾝﾄ", "備考"):
        out[c] = np.nan
    return out


def build_raw_results(
    sed: pd.DataFrame,
    *,
    jockey_xwalk: Optional[pd.DataFrame] = None,
    trainer_xwalk: Optional[pd.DataFrame] = None,
    kyi: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """JRDB SED → netkeiba raw_results 相当の DataFrame（index=race_id）。

    SED から構造的に埋まる列を生成。**horse_id は ketto_to_horse_id（canonical id・
    horse_results と直近スクレイプ年に一致）**。jockey_id/trainer_id は crosswalk で付与
    （式が無いため）。**kyi を渡すと 枠番・性齢 を KYI（出馬表・race_id×馬番）から補う**
    （枠番=KYI.wakuban、性齢=KYI.sex_code + 数え歳[レース年−生年]）。kyi 無しなら従来どおり欠損。
    残る SED/KYI に無い列（馬主/ﾀｲﾑ指数/着差[マージン]/調教ﾀｲﾑ/厩舎ｺﾒﾝﾄ）は欠損のまま。
    """
    if sed is None or sed.empty:
        return pd.DataFrame()
    d = sed.copy()
    jz = _xwalk_map(jockey_xwalk, "kishu_code", "jockey_id")
    tz = _xwalk_map(trainer_xwalk, "chokyo_code", "trainer_id")

    out = pd.DataFrame(index=range(len(d)))
    out["race_id"] = d["race_id"].astype(str).to_numpy()
    out["馬番"] = _num(_col(d, "umaban")).to_numpy()
    out["着順"] = [_chakujun_str(c, i) for c, i in
                 zip(_col(d, "chakujun"), _col(d, "ijo_kubun"), strict=False)]
    out["馬名"] = _col(d, "bamei").to_numpy()
    out["斤量"] = (_num(_col(d, "futan_juryo")) / 10).to_numpy()          # 0.1kg → kg
    out["騎手"] = _col(d, "kishu_name").to_numpy()
    out["タイム"] = [_time_str(v) for v in _col(d, "time")]
    out["通過"] = [_passing(r) for _, r in d.iterrows()]
    out["上り"] = [_tenths_to_sec(v) for v in _col(d, "ato3f_time")]
    out["単勝"] = _num(_col(d, "kakutei_tansho")).to_numpy()
    out["人気"] = _num(_col(d, "kakutei_ninki")).to_numpy()
    out["馬体重"] = [_bataijuu_str(w, z) for w, z in
                   zip(_col(d, "bataijuu"), _col(d, "bataijuu_zougen"), strict=False)]
    out["備考"] = _col(d, "biko").to_numpy()
    out["調教師"] = _col(d, "chokyoshi_name").to_numpy()
    out["賞金(万円)"] = _num(_col(d, "honshokin")).to_numpy()
    # horse_id は canonical（ketto_to_horse_id）。騎手/調教師は crosswalk（式が無いため）。
    out["horse_id"] = _col(d, "ketto").map(ketto_to_horse_id).to_numpy()
    out["jockey_id"] = _col(d, "kishu_code").map(lambda k: jz.get(k)).to_numpy()
    out["trainer_id"] = _col(d, "chokyo_code").map(lambda k: tz.get(k)).to_numpy()
    # 枠番・性齢は KYI（出馬表）から (race_id,馬番) で補う。KYI 無しなら欠損（従来挙動）。
    waku_map, sex_map = _kyi_waku_sex(kyi)
    if waku_map or sex_map:
        rid_s = out["race_id"].astype(str)
        um_i = pd.to_numeric(out["馬番"], errors="coerce")
        keys = [(r, int(u)) if pd.notna(u) else None
                for r, u in zip(rid_s, um_i, strict=False)]
        out["枠番"] = [waku_map.get(k) if k else None for k in keys]
        out["性齢"] = [_seirei(sex_map.get(k), r, h) if k else None
                     for k, r, h in zip(keys, rid_s, out["horse_id"], strict=False)]
    else:
        out["枠番"] = np.nan
        out["性齢"] = np.nan
    # 残る SED/KYI に無い列は欠損で確保（featured 側の列存在前提を壊さない）
    for c in ("着差", "ﾀｲﾑ指数", "調教ﾀｲﾑ", "厩舎ｺﾒﾝﾄ", "馬主", "owner_id"):
        out[c] = np.nan
    out = out.set_index("race_id")
    return out
