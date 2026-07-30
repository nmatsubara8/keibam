"""連系エッジ Test A — 馬連の「実市場オッズ」は単勝由来 Harville を上回るか。

`docs/exotic_edge_data_design.md` の第一歩。単勝市場は marginal 勝率をほぼ完璧に価格づける
（効率的）。連系の的中は joint 分布（相関）に依存し、Harville は相関ゼロ近似。よって
「馬連の実市場オッズ（OZ の umaren プール）」が「単勝由来 Harville」より実際の馬連結果
（HJC）をよく当てるなら、**市場は単勝に無い相関情報を連系プールに入れている**＝ joint 構造が
実在し価格化されている（連系エッジを追う価値の上限）。差が無ければ連系も単勝の写しで、この路線は終了。

モデル不要・純データ。OZ（前売り実市場・単勝+馬連）と HJC（確定・馬連当選組合せ）だけを使う。

比較指標: 当選ペアに対する **平均 logloss**（低いほど良い予測器）。
  p_harv(i,j) = 単勝(OZ tansho)由来 Harville（prob_quinella）
  p_mkt(i,j)  = 馬連(OZ umaren)プールの implied（控除を正規化で除去）
  uniform     = 参照（1/組合せ数）

使い方:
  python scripts/jrdb_exotic_dr2_check.py --odds-dir data/jrdb_txt/oz_check
  python scripts/jrdb_exotic_dr2_check.py --odds-dir DIR --since-year 2023 --max-races 5000
"""
from __future__ import annotations

import argparse
import glob
import math
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.jrdb._odds import normalize_combo, parse_odds  # noqa: E402
from src.policies._harville import prob_quinella  # noqa: E402
from src.storage._db import get_engine  # noqa: E402

_EPS = 1e-9


def _combo_to_pair(combo: str) -> tuple[int, int]:
    """'01-02' / '02-01' → (1, 2)（昇順）。"""
    xs = sorted(int(x) for x in str(combo).replace("-", " ").split() if x.strip())
    return (xs[0], xs[1])


def win_probs_from_tansho(tansho: dict[int, float]) -> dict[int, float]:
    """単勝オッズ {馬番: 倍率} → 正規化勝率 {馬番: p}（Σp=1・控除を除去）。"""
    inv = {u: 1.0 / o for u, o in tansho.items() if o and o > 0}
    s = sum(inv.values())
    return {u: v / s for u, v in inv.items()} if s > 0 else {}


def market_pair_probs(umaren: dict[tuple[int, int], float]) -> dict[tuple[int, int], float]:
    """馬連オッズ {(i,j): 倍率} → 正規化 implied {(i,j): p}（控除を除去）。"""
    inv = {k: 1.0 / o for k, o in umaren.items() if o and o > 0}
    s = sum(inv.values())
    return {k: v / s for k, v in inv.items()} if s > 0 else {}


def harville_pair_probs(win_probs: dict[int, float]) -> dict[tuple[int, int], float]:
    """勝率 {馬番: p} → 全ペア Harville 馬連確率 {(i,j): p}（防御的に正規化）。"""
    horses = sorted(win_probs)
    out: dict[tuple[int, int], float] = {}
    for a in range(len(horses)):
        for b in range(a + 1, len(horses)):
            i, j = horses[a], horses[b]
            out[(i, j)] = prob_quinella(win_probs, i, j)
    s = sum(out.values())
    return {k: v / s for k, v in out.items()} if s > 0 else out


def _logloss(pair_probs: dict[tuple[int, int], float], win_pair: tuple[int, int]) -> float:
    p = pair_probs.get(win_pair, 0.0)
    return -math.log(max(p, _EPS))


def _argmax_pair(pair_probs: dict[tuple[int, int], float]):
    return max(pair_probs, key=pair_probs.get) if pair_probs else None


def _valid_pair(combo: object):
    """HJC の馬連 combo 文字列 → 有効な (i,j)（昇順・1..18・相異）なら返す。無効は None。

    '0000'/'00'/空/桁不足 は無効（未使用スロットの 0 埋め等）。
    """
    if combo is None or not str(combo).strip():
        return None
    try:
        i, j = _combo_to_pair(normalize_combo(combo, ordered=False))
    except (ValueError, IndexError):
        return None
    if 1 <= i <= 18 and 1 <= j <= 18 and i != j:
        return (i, j)
    return None


def _load_hjc_umaren_winners(engine) -> dict[str, tuple[int, int]]:
    """raw_jrdb_hjc → {race_id: 当選馬連ペア}。combo1 の有効ペアを採用（同着は稀＝combo1 で近似）。"""
    have = pd.read_sql(text("SELECT * FROM raw_jrdb_hjc LIMIT 1"), engine).columns
    if "umaren_combo1" not in have or "race_id" not in have:
        return {}
    df = pd.read_sql(text('SELECT "race_id","umaren_combo1" FROM raw_jrdb_hjc'), engine)
    winners: dict[str, tuple[int, int]] = {}
    for _, row in df.iterrows():
        pair = _valid_pair(row.get("umaren_combo1"))
        if pair is not None:
            winners[str(row["race_id"])] = pair
    return winners


def _race_odds_from_oz(long_df: pd.DataFrame):
    """OZ の long → {race_id: (tansho{馬番:倍率}, umaren{(i,j):倍率})}。"""
    out: dict[str, tuple[dict, dict]] = {}
    for rid, g in long_df.groupby("race_id"):
        tan = {int(r.combo): r.odds for r in g[g.bet == "tansho"].itertuples()}
        um = {_combo_to_pair(r.combo): r.odds for r in g[g.bet == "umaren"].itertuples()}
        out[str(rid)] = (tan, um)
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="連系 Test A: 馬連実市場 vs 単勝Harville（logloss）")
    ap.add_argument("--odds-dir", default="data/jrdb_txt/oz_check",
                    help="OZ の .txt を置いたフォルダ（既定 data/jrdb_txt/oz_check）")
    ap.add_argument("--db", default=None, help="SQLite（HJC 読込）")
    ap.add_argument("--since-year", type=int, default=None)
    ap.add_argument("--max-races", type=int, default=None, help="評価レース数の上限（動作確認用）")
    args = ap.parse_args(argv if argv is not None else sys.argv[1:])

    engine = get_engine(args.db)
    winners = _load_hjc_umaren_winners(engine)
    print(f"[testA] HJC 馬連当選表 {len(winners):,} レース")

    files = sorted(glob.glob(f"{args.odds_dir}/OZ*.txt") + glob.glob(f"{args.odds_dir}/oz*.txt"))
    if not files:
        print(f"OZ の .txt が見つかりません（{args.odds_dir}/OZ*.txt）。zip を展開してください。",
              file=sys.stderr)
        return 1

    n = 0
    ll_harv = ll_mkt = ll_unif = 0.0
    hit_harv = hit_mkt = 0
    for fp in files:
        long_df = parse_odds(fp, "OZ")
        if long_df.empty:
            continue
        if args.since_year:
            long_df = long_df[long_df["race_id"].str[:4].astype(int) >= args.since_year]
        for rid, (tan, um) in _race_odds_from_oz(long_df).items():
            win_pair = winners.get(rid)
            if win_pair is None or not tan or not um:
                continue
            wp = win_probs_from_tansho(tan)
            p_harv = harville_pair_probs(wp)
            p_mkt = market_pair_probs(um)
            if not p_harv or not p_mkt or win_pair not in p_mkt:
                continue
            n += 1
            ll_harv += _logloss(p_harv, win_pair)
            ll_mkt += _logloss(p_mkt, win_pair)
            ll_unif += math.log(len(p_mkt))
            hit_harv += int(_argmax_pair(p_harv) == win_pair)
            hit_mkt += int(_argmax_pair(p_mkt) == win_pair)
            if args.max_races and n >= args.max_races:
                break
        if args.max_races and n >= args.max_races:
            break

    if n == 0:
        print("評価対象レース0（OZ と HJC の race_id が重ならない可能性）。", file=sys.stderr)
        return 1
    print(f"\n[testA] 評価レース {n:,}")
    print(f"  平均 logloss  Harville(単勝由来) = {ll_harv / n:.4f}")
    print(f"  平均 logloss  Market(馬連実市場)  = {ll_mkt / n:.4f}")
    print(f"  平均 logloss  uniform(参照)       = {ll_unif / n:.4f}")
    d = (ll_harv - ll_mkt) / n
    print(f"  Δ(Harville − Market) = {d:+.4f}  "
          f"（+なら市場が上＝相関情報を価格化／≈0なら馬連は単勝の写し）")
    print(f"  argmax 的中率  Harville = {hit_harv / n:.2%} / Market = {hit_mkt / n:.2%}")
    print("\n[testA] 読み: Δ が有意に正 → joint(相関)構造が実在し市場が価格化＝連系エッジを追う"
          "価値あり（次は p_mkt−p_harv を JRDB 展開/脚質で予測できるか＝Test B）。"
          "Δ≈0 → 馬連も単勝の写しで連系路線も終了。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
