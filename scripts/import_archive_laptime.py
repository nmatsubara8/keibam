"""アーカイブ laptime CSV（19860105-20210731_laptime.csv）を race_pace.pkl へ変換する。

CSV は race 単位: ラップタイム1..18 / ペース1..18 / 前半3ハロン / 上がり3ハロン。
これを race_id → ペース要約に落とす:

    race_id | zenhan_3f | agari_3f | pace_diff | pace_type | n_laps

- pace_diff = 上がり3F − 前半3F。正=前傾(前半速い＝ハイペース)、負=後傾(スローの上がり勝負)。
- pace_type = 前傾 / 平坦 / 後傾（±0.8秒の閾値）。
- n_laps = 有効ラップ本数（距離の代理）。

【重要・リーク注意】これは**発走後にしか分からない結果**。予測特徴に「今走のペース」を
そのまま入れてはならない。前進安全に使うには、各馬の**過去走**の race_id に本表を結合し、
「その馬が過去に好走したペース傾向（ペース適性）」as-of 集計として featured に持ち込む。
本スクリプトはそのデータ土台（race_pace.pkl）を作るだけ。

使い方: python scripts/import_archive_laptime.py --csv <path> [--only-year 2020] [--dry-run]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

C_RACE_ID = "レースID"
C_ZENHAN = "前半3ハロン"
C_AGARI = "上がり3ハロン"
LAP_COLS = [f"ラップタイム{i}" for i in range(1, 19)]
PACE_TYPE_THRESH = 0.8   # 秒。|pace_diff| がこれ未満は「平坦」


def csv_to_race_pace(df: pd.DataFrame) -> pd.DataFrame:
    """laptime CSV → race 単位ペース要約（race_id でユニーク）。"""
    g = df.drop_duplicates(subset=[C_RACE_ID]).reset_index(drop=True)
    out = pd.DataFrame(index=range(len(g)))
    out["race_id"] = pd.to_numeric(g[C_RACE_ID], errors="coerce").astype("Int64").astype(str)
    zen = pd.to_numeric(g.get(C_ZENHAN), errors="coerce")
    agari = pd.to_numeric(g.get(C_AGARI), errors="coerce")
    out["zenhan_3f"] = zen.to_numpy()
    out["agari_3f"] = agari.to_numpy()
    diff = agari - zen
    out["pace_diff"] = diff.to_numpy()
    out["pace_type"] = np.where(
        diff.isna(), pd.NA,
        np.where(diff > PACE_TYPE_THRESH, "前傾",
                 np.where(diff < -PACE_TYPE_THRESH, "後傾", "平坦")),
    )
    lap_present = [c for c in LAP_COLS if c in g.columns]
    if lap_present:
        laps = g[lap_present].apply(pd.to_numeric, errors="coerce")
        out["n_laps"] = laps.notna().sum(axis=1).to_numpy()
    else:
        out["n_laps"] = pd.NA
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="アーカイブ laptime CSV を race_pace.pkl へ取込")
    ap.add_argument("--csv", required=True)
    ap.add_argument("--only-year", type=int, default=None)
    ap.add_argument("--merge", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    from src.constants._local_paths import LocalPaths

    df = pd.read_csv(args.csv, dtype=str)
    if args.only_year is not None:
        rid = pd.to_numeric(df[C_RACE_ID], errors="coerce").astype("Int64").astype(str)
        df = df[rid.str.startswith(str(args.only_year))].reset_index(drop=True)
    print(f"[laptime] CSV 読込: {len(df):,} 行")
    pace = csv_to_race_pace(df)
    print(f"[laptime] race_pace: {len(pace):,} レース / pace_type 分布:")
    print(pace["pace_type"].value_counts(dropna=False).to_string())

    out_path = str(Path(LocalPaths.RAW_DIR) / "race_pace.pkl")
    if args.merge and Path(out_path).exists():
        old = pd.read_pickle(out_path)
        dup = set(pace["race_id"])
        old = old[~old["race_id"].isin(dup)]
        pace = pd.concat([old, pace], ignore_index=True)
        print(f"[laptime] マージ後: {len(pace):,} レース")

    if args.dry_run:
        print("[laptime] --dry-run: 保存しません")
        print(pace.head(6).to_string())
        return
    pace.to_pickle(out_path)
    print(f"[laptime] 保存: {out_path}")


if __name__ == "__main__":
    main()
