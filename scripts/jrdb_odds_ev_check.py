"""JRDB オッズデータ（実市場オッズ）× HJC 払戻の連系 EV 検証ハーネス。

⚠️ 注意（重要な再解釈）: OZ/OW/OU/OT/OV は当初「基準オッズ＝市場直交フェアバリュー」と
誤解していたが、実測（単勝 overround≈1.35＝控除内包）で **実市場オッズ（前売り/前日の早い
時点）** と判明。したがって本ハーネスの「最小オッズの組合せを賭ける」は **市場（前売り）で最も
人気の組合せを賭ける**の意味であり、ROI≈−控除 は「連系の前売り本命は控除に負ける」という
市場効率のトリビアルな確認に過ぎない（JRDB フェア値の妙味を測るものではない）。真の連系エッジは
`docs/exotic_edge_data_design.md` の ΔR²（市場オッズに独立情報を足せるか）で測ること。

方法（シンプルな後付けバックテスト）:
  各レースで市場オッズ最小（＝前売り最人気）の組合せを top-K 賭ける（各100円）。
  HJC の当該券種の当選組合せ・払戻と突合し、的中なら払戻を得る。
  ROI = Σ払戻 / (100×賭け数) − 1、的中率 = 的中数/賭け数 を集計する。
  控除後(exotic は約25-30%控除)＝ ROI ≈ −0.25〜−0.30 が想定（市場本命買いの帰無）。

市場オッズは巨大なため raw_jrdb_* には保存せず、`_odds.parse_odds` でファイルから直接読む。
HJC は raw_jrdb_hjc（取込済）から読む。

使い方:
  python scripts/jrdb_odds_ev_check.py --kind OV --odds-dir data/jrdb_dl/txt
  python scripts/jrdb_odds_ev_check.py --kind OT --odds-dir DIR --top 3 --since-year 2024
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.jrdb._odds import (  # noqa: E402
    BET_ORDERED,
    ODDS_SPEC,
    favorites,
    normalize_combo,
    parse_odds,
)
from src.storage._db import get_engine  # noqa: E402

# 基準オッズ種別 → HJC の券種 prefix（払戻列 {prefix}_combo{i}/{prefix}_pay{i}）。
_KIND_BETS = {"OZ": ["umaren"], "OW": ["wide"], "OU": ["umatan"],
              "OT": ["sanrenpuku"], "OV": ["sanrentan"]}
_HJC_OCC = {"umaren": 3, "wide": 7, "umatan": 6, "sanrenpuku": 3, "sanrentan": 6}


def _load_hjc_winners(engine, bets) -> dict:
    """raw_jrdb_hjc → {race_id: {bet: {正準combo: 払戻}}}。当選組合せの払戻表。"""
    cols = ["race_id"]
    for b in bets:
        for i in range(1, _HJC_OCC[b] + 1):
            cols += [f"{b}_combo{i}", f"{b}_pay{i}"]
    have = pd.read_sql(text("SELECT * FROM raw_jrdb_hjc LIMIT 1"), engine).columns
    sel = [c for c in cols if c in have]
    df = pd.read_sql(text(f'SELECT {",".join(chr(34)+c+chr(34) for c in sel)} '
                          "FROM raw_jrdb_hjc"), engine)
    winners: dict = {}
    for _, row in df.iterrows():
        rid = str(row["race_id"])
        per_bet: dict = {}
        for b in bets:
            ordered = BET_ORDERED[b]
            d = {}
            for i in range(1, _HJC_OCC[b] + 1):
                cc, pc = f"{b}_combo{i}", f"{b}_pay{i}"
                if cc in sel and pc in sel:
                    combo, pay = row.get(cc), pd.to_numeric(row.get(pc), errors="coerce")
                    if combo and str(combo).strip() and pd.notna(pay) and pay > 0:
                        d[normalize_combo(combo, ordered=ordered)] = float(pay)
            if d:
                per_bet[b] = d
        if per_bet:
            winners[rid] = per_bet
    return winners


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="JRDB 基準オッズ × HJC 払戻の EV 検証")
    ap.add_argument("--kind", required=True, choices=list(ODDS_SPEC),
                    help="基準オッズ種別（OZ/OW/OU/OT/OV）")
    ap.add_argument("--odds-dir", required=True, help="基準オッズ .txt を置いたフォルダ")
    ap.add_argument("--db", default=None, help="SQLite（HJC 読込。既定 LocalPaths.DB_PATH）")
    ap.add_argument("--top", type=int, default=1, help="各レースで賭ける本命上位 N 組（既定1）")
    ap.add_argument("--since-year", type=int, default=None, help="対象年下限")
    args = ap.parse_args(argv if argv is not None else sys.argv[1:])

    kind = args.kind.upper()
    bets = _KIND_BETS[kind]
    engine = get_engine(args.db)
    winners = _load_hjc_winners(engine, bets)
    print(f"[ev] HJC 当選表 {len(winners):,} レース（{bets}）")

    files = sorted(Path(args.odds_dir).glob(f"{kind}*.txt"))
    if not files:
        print(f"基準オッズファイルが見つかりません（{args.odds_dir}/{kind}*.txt）。", file=sys.stderr)
        return 1

    n_bet = n_hit = 0
    staked = payoff = 0.0
    n_race = 0
    for fp in files:
        long = parse_odds(str(fp), kind)
        if long.empty:
            continue
        if args.since_year:
            long = long[long["race_id"].str[:4].astype(int) >= args.since_year]
        fav = favorites(long, top=args.top)
        for rid, grp in fav.groupby("race_id"):
            if rid not in winners:
                continue
            n_race += 1
            for _, row in grp.iterrows():
                b = row["bet"]
                win = winners[rid].get(b, {})
                combo = normalize_combo(row["combo"], ordered=BET_ORDERED[b])
                n_bet += 1
                staked += 100.0
                if combo in win:
                    n_hit += 1
                    payoff += win[combo]      # HJC 払戻は 100円あたり
    roi = (payoff / staked - 1) if staked else float("nan")
    print("\n[ev] 結果（市場オッズ本命=前売り最人気を top%d 賭け・各100円）" % args.top)
    print(f"  対象レース: {n_race:,} / 賭け: {n_bet:,} / 的中: {n_hit:,}"
          f"（的中率 {n_hit / n_bet:.2%}）" if n_bet else "  賭け0")
    print(f"  投資 {staked:,.0f}円 / 払戻 {payoff:,.0f}円 / ROI {roi:+.1%}")
    print("  ※ exotic の控除後帰無は ROI≈−25〜30%。これを有意に上回れば連系妙味あり。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
