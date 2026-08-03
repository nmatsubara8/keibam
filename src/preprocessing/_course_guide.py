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
    if guide_master is None or guide_master.empty or not needed.issubset(out.columns):
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
        match_rate = float(out[feat_cols[0]].notna().mean()) if len(out) else 0.0
        if match_rate < require_coverage:
            raise RuntimeError(
                f"course guide join coverage too low: {match_rate:.1%} < {require_coverage:.0%}"
                "（course_guide_master.csv の生成漏れ or 開催×race_type×距離 キー不整合を疑う。"
                "scripts/scrape_course_master.py を実行し master を再生成せよ）。")
    return out
