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
from src.policies._blend import blend_diagnostic, fit_blend  # noqa: E402
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


def _pair_id(i: int, j: int) -> int:
    """(i,j) ペア → 一意 int（blend の int キー用）。"""
    return i * 100 + j


def blend_race_from_probs(p_harv: dict, p_mkt: dict, win_pair: tuple[int, int]):
    """(p_fund=単勝Harville, p_public=馬連市場, winner) の BlendRace（共通ペアで再正規化）を作る。

    combining ΔR² 用。市場が値付けした組合せ（＝賭けられる母集団）に揃え、pair を id 化する。
    勝ちペアが共通集合に無い/2ペア未満は None。
    """
    common = [k for k in p_harv if k in p_mkt]
    if win_pair not in p_harv or win_pair not in p_mkt or len(common) < 2:
        return None
    sh = sum(p_harv[k] for k in common)
    sm = sum(p_mkt[k] for k in common)
    if sh <= 0 or sm <= 0:
        return None
    hf = {_pair_id(*k): p_harv[k] / sh for k in common}
    mf = {_pair_id(*k): p_mkt[k] / sm for k in common}
    return (hf, mf, _pair_id(*win_pair))


def _oz_period(fp: str) -> str:
    """OZ{YYMMDD}.txt のファイル名から半期 'H1'(1-6月)/'H2'(7-12月) を返す（期間OOS用）。"""
    name = Path(fp).stem.upper().replace("OZ", "")
    mm = name[2:4] if len(name) >= 4 and name[:6].isdigit() else ""
    return "H1" if (mm.isdigit() and int(mm) <= 6) else "H2"


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
    blend_races: list[tuple[str, tuple]] = []   # (period, BlendRace) for combining ΔR²
    for fp in files:
        long_df = parse_odds(fp, "OZ")
        if long_df.empty:
            continue
        if args.since_year:
            long_df = long_df[long_df["race_id"].str[:4].astype(int) >= args.since_year]
        period = _oz_period(fp)
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
            br = blend_race_from_probs(p_harv, p_mkt, win_pair)
            if br is not None:
                blend_races.append((period, br))
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

    # ── combining ΔR²（本命）: 単勝Harville×馬連市場 の合成が、賭ける相手=馬連市場を超えるか ──
    print("\n[testA'] combining ΔR²（単勝Harville を馬連市場に足すと ΔR²>0 か＝賭ける価値）")
    all_r = [b for _, b in blend_races]
    if len(all_r) < 200:
        print("  合成レースが少なく測定不能（OZ を増やしてください）。")
        return 0
    w = fit_blend(all_r)
    din = blend_diagnostic(all_r, w)
    print(f"  in-sample: R²(馬連市場)={din['r2_public']:.4f} R²(合成)={din['r2_combined']:.4f} "
          f"ΔR²={din['delta_r2']:+.4f} (α={w.alpha:.2f}β={w.beta:.2f}, n={din['n']:,})")
    h1 = [b for p, b in blend_races if p == "H1"]
    h2 = [b for p, b in blend_races if p == "H2"]
    if len(h1) >= 200 and len(h2) >= 200:
        d12 = blend_diagnostic(h2, fit_blend(h1))["delta_r2"]   # H1 で fit → H2 で評価
        d21 = blend_diagnostic(h1, fit_blend(h2))["delta_r2"]   # 逆
        print(f"  OOS(期間): H1→H2 ΔR²={d12:+.4f} / H2→H1 ΔR²={d21:+.4f}"
              f"（両方向+で初めて本物・過学習でない）")
        oos_ok = d12 > 0 and d21 > 0
    else:
        print("  OOS: H1/H2 いずれか薄く期間分割不可（OZ の期間を広げてください）。")
        oos_ok = False
    print("\n[testA'] 読み: **ΔR²(合成−馬連市場) が OOS 両方向で有意に正** なら、単勝Harville に"
          " 馬連市場が織り込めていない情報があり連系で賭ける価値。ただし magnitude が控除"
          "（馬連~22.5%）を超えるかは別途 EV で要確認。")
    if oos_ok:
        print("  → OOS 両方向+。**唯一の生存候補**。magnitude と EV（前売りで賭け→最終払戻）を次に検証。")
    else:
        print("  → OOS で消失 or 測定不能。連系の合成にも独立エッジ無し。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
