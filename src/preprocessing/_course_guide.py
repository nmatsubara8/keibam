"""距離別コースガイドマスタの読込と results への付与（純関数 + load）。

course_guide_master.csv（scripts/scrape_course_master.py が手入力ソース course_guide.csv を
要点抽出して生成）を読み、現レースの (開催, race_type, 距離) に左結合して guide_* 属性列を
付与する。course_master（track 単位）を距離粒度で補完し、脚質/ペース/波乱傾向を距離別に持つ。

course_len は 100m バケット表現（例 14=1400m）でも実距離表現でも受け付け、両者を m に
正規化してから結合する（学習の RaceInfoProcessor はバケット、ソース CSV は実 m で持つ）。
"""

from __future__ import annotations

import logging
import os

import pandas as pd

from src.constants._course_guide import (
    COURSE_GUIDE_KEY_COLS,
    COURSE_GUIDE_VALUE_COLS,
)

logger = logging.getLogger(__name__)


def _norm_place(s: pd.Series) -> pd.Series:
    """開催（place_id Int64 or PLACE コード str）を 2 桁ゼロ埋め文字列に正規化する。"""
    return pd.to_numeric(s, errors="coerce").astype("Int64").astype(str).str.zfill(2)


def _norm_dist_m(s: pd.Series) -> pd.Series:
    """距離を [m] に正規化する（<100 のバケット表現は ×100 で m へ戻す）。"""
    d = pd.to_numeric(s, errors="coerce")
    med = d.median()
    if pd.notna(med) and med < 100:  # バケット表現（//100 済み）
        d = d * 100.0
    # 10 の位以下の丸め差を吸収して結合率を上げる（1400/1401 等を同一視）
    return (d.round(-1)).astype("Int64")


def load_course_guide_master(path: str) -> pd.DataFrame:
    """course_guide_master.csv を読み込む（無ければ空表）。place_code/course_len_m は正規化済み。"""
    empty = pd.DataFrame(columns=COURSE_GUIDE_KEY_COLS + COURSE_GUIDE_VALUE_COLS)
    if not path or not os.path.exists(path):
        return empty
    df = pd.read_csv(path)
    if df.empty or not all(c in df.columns for c in COURSE_GUIDE_KEY_COLS):
        return empty
    df["place_code"] = _norm_place(df["place_code"])
    df["race_type"] = df["race_type"].astype(str)
    df["course_len_m"] = _norm_dist_m(df["course_len_m"])
    return df


def add_course_guide_features(results: pd.DataFrame, guide_master: pd.DataFrame,
                              *, require_coverage: float | None = None) -> pd.DataFrame:
    """results に guide_<attr> 属性列を付与して返す（他 add_* factor と同じ純関数流儀）。

    キー: 開催 × race_type × 距離[m]。guide_master が空/キー欠損なら guide_* 列を NaN で
    生成（学習/ライブの列パリティを保つ）。距離固有のため 100m 粒度で結合する。

    require_coverage を渡すと（例 0.90）、**guide_master が非空なのに join 一致率がそれ未満**の
    ときに RuntimeError（＝master 生成漏れ/キー不整合を silent NaN のまま成功扱いにしない）。
    既定 None は従来どおり（guide 未整備の環境でも動く）＝後方互換。build で 0.90 を渡すのが推奨。
    """
    out = results.copy()
    feat_cols = [f"guide_{c}" for c in COURSE_GUIDE_VALUE_COLS]

    needed = {"開催", "race_type", "course_len"}
    master_missing = guide_master is None or guide_master.empty
    keys_missing = not needed.issubset(out.columns)
    if master_missing or keys_missing:
        # require_coverage 指定時は「master 不存在/空」「join 対象0件(キー欠損)」も明示失敗させる
        # （silent NaN のまま成功扱いにしない）。未指定なら従来どおり NaN で列パリティを保つ。
        if require_coverage is not None:
            why = "guide_master が不存在/空" if master_missing else \
                f"結合キー {sorted(needed - set(out.columns))} が results に無い(join 対象0件)"
            raise RuntimeError(
                f"course guide unavailable: {why}"
                "（course_guide_master.csv を scripts/scrape_course_master.py で生成し、"
                "開催×race_type×course_len が featured に揃っているか確認せよ）。")
        for c in feat_cols:
            out[c] = float("nan")
        return out

    keyed = pd.DataFrame({
        "place_code": _norm_place(out["開催"]),
        "race_type": out["race_type"].astype(str),
        "course_len_m": _norm_dist_m(out["course_len"]),
    }, index=out.index)

    gm = guide_master.rename(columns={c: f"guide_{c}" for c in COURSE_GUIDE_VALUE_COLS})
    gm = gm.drop_duplicates(subset=COURSE_GUIDE_KEY_COLS)
    merged = keyed.merge(gm, on=COURSE_GUIDE_KEY_COLS, how="left")
    merged.index = out.index

    for c in feat_cols:
        out[c] = merged[c].to_numpy() if c in merged.columns else float("nan")
    if require_coverage is not None and feat_cols:
        rpt = _guide_coverage_report(keyed, out[feat_cols[0]])
        if rpt["coverage"] < require_coverage:
            raise RuntimeError(
                f"course guide join coverage too low: {rpt['coverage']:.1%} < {require_coverage:.0%}\n"
                f"  内訳: total={rpt['total_rows']:,} eligible(JRA 芝/ダート)={rpt['eligible_rows']:,} "
                f"complete_key={rpt['complete_key_rows']:,} matched={rpt['matched_rows']:,}\n"
                f"  unmatched key top: {rpt['unmatched_key_top']}\n"
                "（分母=適格かつキー完全な行。course_guide_master.csv の生成漏れ or "
                "開催×race_type×距離 キー不整合を疑う。scripts/scrape_course_master.py を実行）。")
    return out


# JRA 開催コード（01 札幌 … 10 小倉）。地方(NAR)は 5x 台・海外は別体系で guide 対象外。
_JRA_PLACE_CODES = frozenset(f"{i:02d}" for i in range(1, 11))
# guide が定義される馬場（障害=障 はコース形状ガイド対象外）。
_GUIDE_RACE_TYPES = frozenset({"芝", "ダート"})


def _guide_coverage_report(keyed: pd.DataFrame, matched_col: pd.Series) -> dict:
    """guide coverage を「適格(JRA・芝/ダート)かつキー完全な行」を分母に算出する。

    地方/海外/障害/キー欠損/距離不定 は分母から除外（正常でも 90% を割らせないため）。
    """
    total = int(len(keyed))
    eligible_mask = (keyed["place_code"].isin(_JRA_PLACE_CODES)
                     & keyed["race_type"].isin(_GUIDE_RACE_TYPES))
    complete_mask = eligible_mask & keyed["place_code"].notna() & \
        keyed["race_type"].notna() & keyed["course_len_m"].notna()
    complete = int(complete_mask.sum())
    matched_vals = pd.Series(matched_col).to_numpy()
    matched_mask = complete_mask.to_numpy() & pd.notna(matched_vals)
    matched = int(matched_mask.sum())
    # 未一致キーの上位（complete だが matched でない）
    unmatched = keyed[complete_mask.to_numpy() & ~pd.notna(matched_vals)]
    top = [tuple(x) for x in unmatched[COURSE_GUIDE_KEY_COLS].value_counts().head(10).index]
    return {
        "total_rows": total,
        "eligible_rows": int(eligible_mask.sum()),
        "complete_key_rows": complete,
        "matched_rows": matched,
        "coverage": (matched / complete) if complete else 0.0,
        "unmatched_key_top": top,
    }
