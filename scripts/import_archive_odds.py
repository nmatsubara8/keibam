"""アーカイブ払戻 CSV（19860105-20210731_odds.csv）を clean な払戻テーブルへ変換する。

CSV は横持ち（券種ごとに 単勝1_馬番/オッズ/人気, 三連単3_組合せ1..3/オッズ/人気 …）。
これを縦持ちの payoffs.pkl（1行=race_id×券種×当選組合せ）に正規化する:

    race_id | bet_type | combo_key | payoff_yen | popularity

- payoff_yen は「100円あたりの払戻金」（CSV の *_オッズ 値。例 単勝=210 → 210円）。
- combo_key は canonical_combo 準拠（順不同券種は昇順、馬単/三連単は順序保持）で、
  EV/決済側の combo と一致する。
- これで manji ハーネスを複勝/馬連等の**決済**に拡張できる（当選 combo と払戻が引ける）。

単勝は race_result 側にも各馬オッズがあるが、複勝以降の払戻は本テーブルにしか無い。
使い方:
  python scripts/import_archive_odds.py --csv <path> [--only-year 2020] [--dry-run]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.constants._bet_types import ORDERED, BetType  # noqa: E402

C_RACE_ID = "レースID"

# 券種 → (CSV接頭辞, インスタンス数, 組合せ列テンプレート)
SPEC = {
    BetType.TANSHO:     ("単勝",   2, ["{p}{i}_馬番"]),
    BetType.FUKUSHO:    ("複勝",   5, ["{p}{i}_馬番"]),
    BetType.WAKUREN:    ("枠連",   2, ["{p}{i}_組合せ1", "{p}{i}_組合せ2"]),
    BetType.UMAREN:     ("馬連",   2, ["{p}{i}_組合せ1", "{p}{i}_組合せ2"]),
    BetType.WIDE:       ("ワイド", 7, ["{p}{i}_組合せ1", "{p}{i}_組合せ2"]),
    BetType.UMATAN:     ("馬単",   2, ["{p}{i}_組合せ1", "{p}{i}_組合せ2"]),
    BetType.SANRENPUKU: ("三連複", 3, ["{p}{i}_組合せ1", "{p}{i}_組合せ2", "{p}{i}_組合せ3"]),
    BetType.SANRENTAN:  ("三連単", 3, ["{p}{i}_組合せ1", "{p}{i}_組合せ2", "{p}{i}_組合せ3"]),
}


def _combo_keys(arr: np.ndarray, bet_type: str) -> list[str]:
    """(n,k) の馬番配列 → combo_key 文字列列（順不同券種は昇順正規化）。"""
    if bet_type not in ORDERED:
        arr = np.sort(arr, axis=1)
    return ["-".join(str(int(x)) for x in row) for row in arr]


def csv_to_payoffs(df: pd.DataFrame) -> pd.DataFrame:
    """横持ち払戻 CSV → 縦持ち payoffs テーブル。"""
    rid = pd.to_numeric(df[C_RACE_ID], errors="coerce").astype("Int64").astype(str)
    frames = []
    for bet_type, (prefix, n, templates) in SPEC.items():
        for i in range(1, n + 1):
            combo_cols = [t.format(p=prefix, i=i) for t in templates]
            odds_col = f"{prefix}{i}_オッズ"
            pop_col = f"{prefix}{i}_人気"
            if odds_col not in df.columns or any(c not in df.columns for c in combo_cols):
                continue
            odds = pd.to_numeric(df[odds_col], errors="coerce")
            ok = odds.notna()
            combos = pd.DataFrame({c: pd.to_numeric(df[c], errors="coerce") for c in combo_cols})
            ok &= combos.notna().all(axis=1)
            if not ok.any():
                continue
            arr = combos.loc[ok].to_numpy()
            keys = _combo_keys(arr, bet_type)
            pop = pd.to_numeric(df[pop_col], errors="coerce") if pop_col in df.columns else pd.Series(pd.NA, index=df.index)
            frames.append(pd.DataFrame({
                "race_id": rid[ok].to_numpy(),
                "bet_type": bet_type,
                "combo_key": keys,
                "payoff_yen": odds[ok].to_numpy(),
                "popularity": pop[ok].to_numpy(),
            }))
    if not frames:
        return pd.DataFrame(columns=["race_id", "bet_type", "combo_key", "payoff_yen", "popularity"])
    out = pd.concat(frames, ignore_index=True)
    # 同一 race×券種×combo の重複は最初を採用（CSV 内重複の保険）
    return out.drop_duplicates(subset=["race_id", "bet_type", "combo_key"], keep="first").reset_index(drop=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="アーカイブ払戻CSVを payoffs.pkl へ取込")
    ap.add_argument("--csv", required=True)
    ap.add_argument("--only-year", type=int, default=None)
    ap.add_argument("--merge", action="store_true", help="既存 payoffs.pkl とマージ")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    from src.constants._local_paths import LocalPaths

    df = pd.read_csv(args.csv, dtype=str)
    if args.only_year is not None:
        rid = pd.to_numeric(df[C_RACE_ID], errors="coerce").astype("Int64").astype(str)
        df = df[rid.str.startswith(str(args.only_year))].reset_index(drop=True)
    print(f"[odds] CSV 読込: {len(df):,} レース")
    payoffs = csv_to_payoffs(df)
    print(f"[odds] payoffs: {len(payoffs):,} 行 / 券種別:")
    print(payoffs.groupby("bet_type").size().to_string())

    out_path = str(Path(LocalPaths.RAW_DIR) / "payoffs.pkl")
    if args.merge and Path(out_path).exists():
        old = pd.read_pickle(out_path)
        key = ["race_id", "bet_type", "combo_key"]
        keep_new = set(map(tuple, payoffs[key].to_numpy()))
        old = old[~old[key].apply(tuple, axis=1).isin(keep_new)]
        payoffs = pd.concat([old, payoffs], ignore_index=True)
        print(f"[odds] マージ後: {len(payoffs):,} 行")

    if args.dry_run:
        print("[odds] --dry-run: 保存しません")
        print(payoffs.head(8).to_string())
        return
    payoffs.to_pickle(out_path)
    print(f"[odds] 保存: {out_path}")


if __name__ == "__main__":
    main()
