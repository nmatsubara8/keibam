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
    """馬体重 + 増減 → netkeiba '480(+4)'。増減が空なら '480'。両方空なら None。"""
    ws = "" if w is None else str(w).strip()
    zs = "" if z is None else str(z).strip().replace(" ", "")
    if not ws or ws == "0":
        return None
    return f"{ws}({zs})" if zs and zs not in ("0", "+0", "-0") else ws


def _chakujun_str(chaku: object, ijo: object) -> Optional[str]:
    """非完走（異常区分あり）は None（netkeiba は着順 NaN）。完走は着順数字。"""
    ijs = "" if ijo is None else str(ijo).strip()
    if ijs and ijs != "0":            # 取消/除外/中止/失格/降着 = 非完走
        return None
    n = pd.to_numeric(chaku, errors="coerce")
    return str(int(n)) if pd.notna(n) and n > 0 else None


def _xwalk_map(xw: Optional[pd.DataFrame], code_col: str, id_col: str) -> dict:
    """crosswalk DataFrame → {jrdb_code: netkeiba_id} 辞書。None/空なら空辞書。"""
    if xw is None or xw.empty or code_col not in xw.columns or id_col not in xw.columns:
        return {}
    d = xw[[code_col, id_col]].dropna()
    return dict(zip(d[code_col].astype(str), d[id_col].astype(str), strict=False))


def build_raw_results(
    sed: pd.DataFrame,
    *,
    horse_xwalk: Optional[pd.DataFrame] = None,
    jockey_xwalk: Optional[pd.DataFrame] = None,
    trainer_xwalk: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """JRDB SED → netkeiba raw_results 相当の DataFrame（index=race_id）。

    SED から構造的に埋まる列を生成し、horse_id/jockey_id/trainer_id は crosswalk で付与。
    SED に無い列（枠番/性齢/馬主/ﾀｲﾑ指数/着差[マージン]/調教ﾀｲﾑ/厩舎ｺﾒﾝﾄ）は欠損のまま
    （後段で KYI 枠番・UKC 性別/馬主 を結合して補う）。値はアダプタ側で netkeiba 形式へ整形。
    """
    if sed is None or sed.empty:
        return pd.DataFrame()
    d = sed.copy()
    hz = _xwalk_map(horse_xwalk, "ketto", "horse_id")
    jz = _xwalk_map(jockey_xwalk, "kishu_code", "jockey_id")
    tz = _xwalk_map(trainer_xwalk, "chokyo_code", "trainer_id")

    out = pd.DataFrame(index=range(len(d)))
    out["race_id"] = d["race_id"].astype(str).to_numpy()
    out["馬番"] = _num(d["umaban"]).to_numpy()
    out["着順"] = [_chakujun_str(c, i) for c, i in zip(d.get("chakujun"), d.get("ijo_kubun"), strict=False)]
    out["馬名"] = d.get("bamei", pd.Series(index=d.index, dtype=object)).astype(str).str.strip().to_numpy()
    out["斤量"] = (_num(d.get("futan_juryo")) / 10).to_numpy()          # 0.1kg → kg
    out["騎手"] = d.get("kishu_name", pd.Series(dtype=object)).astype(str).str.strip().to_numpy()
    out["タイム"] = [_time_str(v) for v in d.get("time", pd.Series(index=d.index))]
    out["通過"] = [_passing(r) for _, r in d.iterrows()]
    out["上り"] = [_tenths_to_sec(v) for v in d.get("ato3f_time", pd.Series(index=d.index))]
    out["単勝"] = _num(d.get("kakutei_tansho")).to_numpy()
    out["人気"] = _num(d.get("kakutei_ninki")).to_numpy()
    out["馬体重"] = [_bataijuu_str(w, z) for w, z in
                   zip(d.get("bataijuu"), d.get("bataijuu_zougen"), strict=False)]
    out["備考"] = d.get("biko", pd.Series(index=d.index, dtype=object)).astype(str).str.strip().to_numpy()
    out["調教師"] = d.get("chokyoshi_name", pd.Series(dtype=object)).astype(str).str.strip().to_numpy()
    out["賞金(万円)"] = _num(d.get("honshokin")).to_numpy()
    # 同一性（crosswalk）。対応が無いコードは欠損（境界外＝JRDB専有馬など）。
    out["horse_id"] = d.get("ketto", pd.Series(dtype=object)).astype(str).map(lambda k: hz.get(k))
    out["jockey_id"] = d.get("kishu_code", pd.Series(dtype=object)).astype(str).map(lambda k: jz.get(k))
    out["trainer_id"] = d.get("chokyo_code", pd.Series(dtype=object)).astype(str).map(lambda k: tz.get(k))
    # SED に無い列は欠損で確保（featured 側の列存在前提を壊さない）
    for c in ("枠番", "性齢", "着差", "ﾀｲﾑ指数", "調教ﾀｲﾑ", "厩舎ｺﾒﾝﾄ", "馬主", "owner_id"):
        out[c] = np.nan
    out = out.set_index("race_id")
    return out
