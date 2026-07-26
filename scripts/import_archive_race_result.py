"""アーカイブ CSV（19860105-20210731_race_result.csv 等）を pipeline の
results.pkl + race_info.pkl 形式へ変換して取り込む。

背景: 現行 results.pkl は 2023–2026 のみ。過去（1986–2021）の race HTML も DB も
この環境には無いが、race-level を網羅した CSV があれば results/race_info を再構成でき、
rebuild-featured で 1986–2026 の統合 featured を作れる（履歴/人物特徴は 1986–2021 分は
horse_results を持たないため NaN。manji の race-level 因子＝着順/単勝/人気/馬番/性齢/
斤量/馬体重/枠/距離/馬場…は完全復元される）。

CSV の race-level 列（着順・単勝・人気・距離・芝ダ・馬場・競馬場・日付…）から:
  - results.pkl:  ResultsCols 準拠の per-horse テーブル（race_id は列、index は連番）
  - race_info.pkl: RaceInfoProcessor が要求する race_id/course_len/date/place_id/age/sex
                   ＋後段が使う race_type/ground_state1/2/weather

horse_id は CSV に無いため、馬名から安定な代理 ID を合成（実 netkeiba ID 帯と衝突しない
9_000_000_000 台）。履歴は horse_results 依存なので v1 では埋まらない（衝突は無害）。

使い方:
  # 1年だけ試す（rebuild-featured 検証用）
  python scripts/import_archive_race_result.py --csv <path> --only-year 2020 --out-suffix _try
  # 本番（既存 results/race_info とマージ）
  python scripts/import_archive_race_result.py --csv <path> --merge
"""
from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# CSV のカラム名（ユーザ提供のヘッダに準拠）
C_RACE_ID = "レースID"
C_DATE = "レース日付"
C_PLACE = "競馬場コード"
C_DIST = "距離(m)"
C_SURFACE = "芝・ダート区分"
C_HURDLE = "障害区分"
C_GS1 = "馬場状態1"
C_GS2 = "馬場状態2"
C_WEATHER = "天候"
C_SEX = "性別"
C_AGE = "馬齢"
C_WEIGHT = "馬体重"
C_WEIGHT_DIFF = "場体重増減"
CORNERS = ["1コーナー", "2コーナー", "3コーナー", "4コーナー"]


def _surrogate_horse_id(names: pd.Series) -> pd.Series:
    """馬名→安定な数値代理 ID（チャンク跨ぎでも一貫、実 ID 帯と非衝突）。"""
    uniq = names.dropna().astype(str).unique()
    mapping = {
        n: 9_000_000_000 + int(hashlib.md5(n.encode("utf-8")).hexdigest()[:9], 16) % 1_000_000_000
        for n in uniq
    }
    return names.astype(str).map(mapping).astype("Int64")


def _passing(df: pd.DataFrame) -> pd.Series:
    """1–4 コーナー通過順を "7-3-1" 形式に結合（NA は除外）。"""
    cols = [c for c in CORNERS if c in df.columns]
    if not cols:
        return pd.Series(pd.NA, index=df.index, dtype="object")
    s = df[cols].apply(
        lambda r: "-".join(str(int(x)) for x in r if pd.notna(x)) or pd.NA, axis=1
    )
    return s


def _weight_str(df: pd.DataFrame) -> pd.Series:
    """馬体重＋増減を "468(0)" 形式へ（現行 results と同形式）。体重欠損は NA。"""
    if C_WEIGHT not in df.columns:
        return pd.Series(pd.NA, index=df.index, dtype="object")
    w = pd.to_numeric(df[C_WEIGHT], errors="coerce")
    d = pd.to_numeric(df.get(C_WEIGHT_DIFF), errors="coerce") if C_WEIGHT_DIFF in df.columns else None
    out = []
    for i, wv in enumerate(w):
        if pd.isna(wv):
            out.append(pd.NA)
            continue
        dv = 0 if (d is None or pd.isna(d.iloc[i])) else int(d.iloc[i])
        out.append(f"{int(wv)}({dv})")
    return pd.Series(out, index=df.index, dtype="object")


def _race_type(df: pd.DataFrame) -> pd.Series:
    """芝・ダート区分（＋障害区分）→ race_type（芝/ダート/障害）。"""
    surface = df.get(C_SURFACE)
    hurdle = df.get(C_HURDLE)
    base = surface.astype("object") if surface is not None else pd.Series(pd.NA, index=df.index)
    if hurdle is not None:
        is_h = hurdle.notna() & (hurdle.astype(str).str.strip() != "")
        base = base.mask(is_h, "障害")
    return base


def csv_to_results(df: pd.DataFrame) -> pd.DataFrame:
    """CSV → results.pkl スキーマ（race_id は列、index は連番）。"""
    df = df.reset_index(drop=True)   # 列代入の index 整列を保証
    out = pd.DataFrame(index=range(len(df)))
    out["race_id"] = pd.to_numeric(df[C_RACE_ID], errors="coerce").astype("Int64").astype(str)
    out["着順"] = df.get("着順")
    out["枠番"] = df.get("枠番")
    out["馬番"] = df.get("馬番")
    out["馬名"] = df.get("馬名")
    sex = df.get(C_SEX).astype("object").str.strip() if C_SEX in df.columns else pd.NA
    age = pd.to_numeric(df.get(C_AGE), errors="coerce").astype("Int64").astype(str) if C_AGE in df.columns else ""
    out["性齢"] = (sex.fillna("") + age).where(df.get(C_SEX).notna(), pd.NA) if C_SEX in df.columns else pd.NA
    out["斤量"] = df.get("斤量")
    out["騎手"] = df.get("騎手")
    out["タイム"] = df.get("タイム")
    out["着差"] = df.get("着差")
    out["ﾀｲﾑ指数"] = pd.NA
    out["通過"] = _passing(df).to_numpy()
    out["上り"] = df.get("上り")
    out["単勝"] = df.get("単勝")
    out["人気"] = df.get("人気")
    out["馬体重"] = _weight_str(df).to_numpy()
    out["調教ﾀｲﾑ"] = pd.NA
    out["厩舎ｺﾒﾝﾄ"] = pd.NA
    out["備考"] = pd.NA
    out["調教師"] = df.get("調教師")
    out["馬主"] = df.get("馬主")
    out["賞金(万円)"] = df.get("賞金(万円)")
    out["horse_id"] = _surrogate_horse_id(df.get("馬名")).to_numpy()
    out["jockey_id"] = pd.NA
    out["trainer_id"] = pd.NA
    out["owner_id"] = pd.NA
    return out


def csv_to_race_info(df: pd.DataFrame) -> pd.DataFrame:
    """CSV → race_info.pkl スキーマ（race_id 単位・重複排除）。"""
    g = df.drop_duplicates(subset=[C_RACE_ID]).reset_index(drop=True)
    out = pd.DataFrame(index=range(len(g)))
    out["race_id"] = pd.to_numeric(g[C_RACE_ID], errors="coerce").astype("Int64").astype(str)
    out["course_len"] = pd.to_numeric(g.get(C_DIST), errors="coerce")
    # RaceInfoProcessor は "%Y年%m月%d日" でパースするので同形式で保存
    out["date"] = pd.to_datetime(g.get(C_DATE), errors="coerce").dt.strftime("%Y年%m月%d日").to_numpy()
    out["place_id"] = pd.to_numeric(g.get(C_PLACE), errors="coerce").astype("Int64")
    out["race_type"] = _race_type(g).to_numpy()
    out["ground_state1"] = g.get(C_GS1).to_numpy() if C_GS1 in g.columns else pd.NA
    out["ground_state2"] = g.get(C_GS2).to_numpy() if C_GS2 in g.columns else pd.NA
    out["weather"] = g.get(C_WEATHER).to_numpy() if C_WEATHER in g.columns else pd.NA
    out["age"] = pd.NA   # RaceInfoProcessor が drop（列だけ必要）
    out["sex"] = pd.NA   # 同上
    return out


def _load_csv(path: str, only_year: int | None) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str)
    if only_year is not None:
        rid = pd.to_numeric(df[C_RACE_ID], errors="coerce").astype("Int64").astype(str)
        df = df[rid.str.startswith(str(only_year))].reset_index(drop=True)
    return df


def main() -> None:
    ap = argparse.ArgumentParser(description="アーカイブ race_result CSV を results/race_info へ取込")
    ap.add_argument("--csv", required=True, help="race_result CSV のパス")
    ap.add_argument("--only-year", type=int, default=None, help="この年だけ変換（検証用）")
    ap.add_argument("--merge", action="store_true", help="既存 results/race_info とマージ保存")
    ap.add_argument("--out-suffix", default="", help="出力ファイル名の接尾辞（検証用に別名保存）")
    ap.add_argument("--dry-run", action="store_true", help="保存せず件数だけ表示")
    args = ap.parse_args()

    from src.constants._local_paths import LocalPaths
    from src.pipeline._ingestion import load_raw

    df = _load_csv(args.csv, args.only_year)
    print(f"[import] CSV 読込: {len(df):,} 行")
    results = csv_to_results(df)
    race_info = csv_to_race_info(df)
    print(f"[import] results: {len(results):,} 行 / race_info: {len(race_info):,} レース")
    print(f"[import] race_id 範囲: {results['race_id'].min()} 〜 {results['race_id'].max()}")

    if args.merge:
        old_r = load_raw(LocalPaths.RAW_RESULTS_PATH)
        old_i = load_raw(LocalPaths.RAW_RACE_INFO_PATH)
        if not old_r.empty:
            # race_id 重複は既存（新しい実データ）を優先
            dup = set(old_r["race_id"].astype(str)) if "race_id" in old_r.columns else set()
            results = results[~results["race_id"].isin(dup)]
            results = pd.concat([old_r, results], ignore_index=True)
        if not old_i.empty:
            dup_i = set(old_i["race_id"].astype(str)) if "race_id" in old_i.columns else set()
            race_info = race_info[~race_info["race_id"].isin(dup_i)]
            race_info = pd.concat([old_i, race_info], ignore_index=True)
        print(f"[import] マージ後 results: {len(results):,} 行 / race_info: {len(race_info):,} レース")

    if args.dry_run:
        print("[import] --dry-run: 保存しません")
        print(results.head(3).to_string())
        print(race_info.head(3).to_string())
        return

    r_path = LocalPaths.RAW_RESULTS_PATH.replace(".pkl", f"{args.out_suffix}.pkl")
    i_path = LocalPaths.RAW_RACE_INFO_PATH.replace(".pkl", f"{args.out_suffix}.pkl")
    results.to_pickle(r_path)
    race_info.to_pickle(i_path)
    print(f"[import] 保存: {r_path} / {i_path}")


if __name__ == "__main__":
    main()
