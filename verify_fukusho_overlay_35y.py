"""複勝 overlay を 35年（1986-2021）で検証する — odds.csv の複勝オッズ × seed_results。

以前「複勝確定オッズのカバレッジ ~2%(478行) で検証不能」だった複勝 overlay 仮説を、
odds.csv（複勝の人気上位オッズ・35年）で初めて大規模に検証する（favorites 限定）。

overlay の定義（market_signals と同思想）:
  p_mkt   = 複勝市場の implied 3着内確率 = (1-takeout) / (複勝倍率)      ← odds.csv
  p_harv  = 単勝オッズ由来 Harville の 3着内確率                        ← seed_results 単勝
  overlay = p_mkt − p_harv
    overlay>0: 複勝市場が単勝より強気（smart money が複勝で買っている?）
    overlay<0: 複勝が単勝Harvilleより弱気＝複勝オッズが割高＝value?

決済: 複勝倍率 = 複勝オッズ / odds-divisor（既定100=円/100円）。着順≤3で return=倍率、else 0。
     stake=1 なので ROI = mean(return)。overlay の符号/大きさ別・年別に ROI/的中率を出し、
     どの符号が（もしあれば）市場を超えるかを見る。フロック（万馬券依存）も併記。

使い方:
    python verify_fukusho_overlay_35y.py "/mnt/c/Users/Ayaka/Downloads/archive/19860105-20210731_odds.csv"
    python verify_fukusho_overlay_35y.py <odds.csv> --seed-results data/raw/seed_results.pkl --min-mult 1.5
"""

from __future__ import annotations

import argparse
import os
import sys

import numpy as np
import pandas as pd

C_RACE_ID = "レースID"
FUKU_SLOTS = 5  # 複勝1..5


def _read_csv(path: str, nrows=None):
    for enc in ("utf-8-sig", "utf-8", "cp932", "shift_jis"):
        try:
            return pd.read_csv(path, nrows=nrows, encoding=enc, low_memory=False)
        except Exception:  # noqa: BLE001
            continue
    raise RuntimeError("odds.csv 読込失敗（エンコーディング）。")


def _fuku_long(odds: pd.DataFrame) -> pd.DataFrame:
    """複勝1..5_馬番/オッズ を (race_id, 馬番, 複勝オッズ) のロング形式へ。"""
    frames = []
    for k in range(1, FUKU_SLOTS + 1):
        mcol, ocol = f"複勝{k}_馬番", f"複勝{k}_オッズ"
        if mcol not in odds.columns or ocol not in odds.columns:
            continue
        sub = odds[[C_RACE_ID, mcol, ocol]].copy()
        sub.columns = ["race_id", "馬番", "複勝オッズ"]
        frames.append(sub)
    long = pd.concat(frames, ignore_index=True)
    long = long.dropna(subset=["馬番", "複勝オッズ"])
    long["race_id"] = long["race_id"].astype("Int64").astype(str)
    long["馬番"] = pd.to_numeric(long["馬番"], errors="coerce").astype("Int64")
    long["複勝オッズ"] = pd.to_numeric(long["複勝オッズ"], errors="coerce")
    return long.dropna(subset=["馬番", "複勝オッズ"])


def _load_seed(path: str):
    """seed_results から {race_id: {馬番: 単勝}} と {race_id: {馬番: 着順}} を作る。"""
    df = pd.read_pickle(path)
    if df.index.name == "race_id":
        df = df.reset_index()
    df["race_id"] = df["race_id"].astype(str)
    df["_um"] = pd.to_numeric(df["馬番"], errors="coerce")
    df["_tan"] = pd.to_numeric(df["単勝"], errors="coerce")
    df["_rank"] = pd.to_numeric(df["着順"], errors="coerce")
    win, rank = {}, {}
    for rid, g in df.dropna(subset=["_um"]).groupby("race_id"):
        win[rid] = {int(u): float(o) for u, o in zip(g["_um"], g["_tan"]) if o and o > 0}
        rank[rid] = {int(u): float(r) for u, r in zip(g["_um"], g["_rank"]) if pd.notna(r)}
    return win, rank


def run(args) -> int:
    if not os.path.isfile(args.odds):
        print(f"[NG] odds.csv が見つかりません: {args.odds}")
        return 2
    if not os.path.isfile(args.seed_results):
        print(f"[NG] seed_results が見つかりません: {args.seed_results}（先に seed_from_csv.py）")
        return 2

    from src.preprocessing._place_prob import implied_from_odds, prob_place

    print("=" * 82)
    print("複勝 overlay 検証（35年・favorites）")
    print("=" * 82)
    odds = _read_csv(args.odds)
    fuku = _fuku_long(odds)
    win_by_race, rank_by_race = _load_seed(args.seed_results)
    print(f"複勝オッズ点数={len(fuku):,} / seed レース数={len(win_by_race):,}")

    t = args.takeout
    div = args.odds_divisor
    rows = []
    for rid, g in fuku.groupby("race_id"):
        win = win_by_race.get(rid)
        ranks = rank_by_race.get(rid)
        if not win or len(win) < 3 or not ranks:
            continue
        win_probs = implied_from_odds(win, normalized=True)
        for uma, fodds in zip(g["馬番"], g["複勝オッズ"]):
            uma = int(uma)
            if uma not in win_probs or uma not in ranks:
                continue
            mult = float(fodds) / div
            if mult <= 0:
                continue
            p_mkt = (1.0 - t) / mult
            p_harv = prob_place(win_probs, uma, 3)
            top3 = 1 if ranks[uma] <= 3 else 0
            rows.append((rid[:4], overlay := p_mkt - p_harv, mult, top3,
                         mult if top3 else 0.0))
    if not rows:
        print("[NG] 突合できる複勝×単勝データがありません（race_id 不一致?）。")
        return 2
    R = pd.DataFrame(rows, columns=["year", "overlay", "mult", "top3", "ret"])
    print(f"突合済み候補={len(R):,}\n")

    def _report(sub: pd.DataFrame, label: str):
        if sub.empty:
            print(f"  {label:<26} n=0")
            return
        roi = sub["ret"].mean()  # stake=1
        hit = sub["top3"].mean()
        mx = sub["ret"].max()
        roi_extop = (sub["ret"].sum() - mx) / len(sub)
        rel = "✓" if sub["top3"].sum() >= 30 else "参考"
        print(f"  {label:<26} n={len(sub):>7,}  的中={hit*100:5.1f}%  "
              f"ROI={roi*100:6.1f}%  除外後={roi_extop*100:6.1f}%  {rel}")

    print("【overlay 符号別（全帯）】市場を超えるなら ROI>100%")
    _report(R, "全候補")
    _report(R[R.overlay > 0], "overlay>0（複勝強気）")
    _report(R[R.overlay < 0], "overlay<0（複勝割高=value?）")

    print(f"\n【payout倍率≥{args.min_mult}（本命除外・中穴帯）】")
    M = R[R["mult"] >= args.min_mult]
    _report(M, f"mult≥{args.min_mult} 全体")
    _report(M[M.overlay > 0], "  +overlay>0")
    _report(M[M.overlay < 0], "  +overlay<0")

    print("\n【overlay<0 × mult≥閾値 を年別（walk-forward 視点）】")
    V = M[M.overlay < 0]
    for y in sorted(R["year"].unique()):
        _report(V[V.year == y], f"{y}")

    print("\n" + "=" * 82)
    print("判定: いずれかの符号帯で ROI>100% が複数年安定＋的中≥30(✓) なら複勝 overlay にエッジ。")
    print("除外後ROIが大きく下がる帯はフロック（万馬券依存）。全帯 ROI<100% なら複勝も市場効率。")
    print("注意: 複勝は人気上位のみ収録（favorites 限定）。中穴/人気薄の複勝は本データに無い。")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="複勝 overlay 35年検証（odds.csv × seed_results）")
    ap.add_argument("odds", help="odds.csv のパス（WSL は /mnt/c/...）")
    ap.add_argument("--seed-results", default="data/raw/seed_results.pkl")
    ap.add_argument("--takeout", type=float, default=0.2, help="複勝控除率（de-vig 用、既定0.2）")
    ap.add_argument("--odds-divisor", type=float, default=100.0,
                    help="複勝オッズ→倍率の除数（既定100=円/100円表記）")
    ap.add_argument("--min-mult", type=float, default=1.5, help="payout 倍率下限（本命除外）")
    return run(ap.parse_args())


if __name__ == "__main__":
    sys.exit(main())
