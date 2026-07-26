"""地方連系プールの効率性診断: 単勝由来 Harville の本命連系回収率 ＋ 連系echo。

問い: 薄い連系プールは単勝が示す以上に非効率か? その非効率は高い控除率(馬単25%/三連単27.5%)を跨ぐか?
Harville最尤の並び＝勝率降順＝本命連系。本命の馬単/三連単/三連複を毎レース購入した回収率を実測。
echo: 的中三連単の Harville予測確率 vs 市場含意((1-t)/払戻)。連系が単勝Harvilleをなぞるか。

使い方: python scripts/nar_pilot/diagnose_exotic.py --data-dir data/nar_pilot
"""
from __future__ import annotations

import argparse
import itertools
import os

import numpy as np
import pandas as pd

T_UMATAN, T_SANPUKU, T_SANREN = 0.25, 0.25, 0.275


def win_probs(g):
    inv = 1.0 / g["tansho_odds"].values
    return dict(zip(g["umaban"].astype(int).values, inv / inv.sum(), strict=True))


def harville_trifectas(pmap, top=5, k=6):
    horses = sorted(pmap, key=lambda u: -pmap[u])[:top]
    out = []
    for i, j, m in itertools.permutations(horses, 3):
        pi, pj, pm = pmap[i], pmap[j], pmap[m]
        d1, d2 = 1 - pi, 1 - pi - pj
        if d1 <= 1e-9 or d2 <= 1e-9:
            continue
        out.append(((i, j, m), pi * (pj / d1) * (pm / d2)))
    out.sort(key=lambda x: -x[1])
    return out[:k]


def parse_combo(s):
    return tuple(int(x) for x in s.split("-")) if isinstance(s, str) and s else None


def build(data_dir):
    horses = pd.read_csv(os.path.join(data_dir, "nar_pilot.csv"))
    horses = horses[horses["tansho_odds"] > 0]
    pay = pd.read_csv(os.path.join(data_dir, "nar_payoffs.csv")).set_index("race_id")
    recs = []
    for rid, g in horses.groupby("race_id"):
        if rid not in pay.index:
            continue
        pr = pay.loc[rid]
        pmap = win_probs(g)
        fav = sorted(pmap, key=lambda u: -pmap[u])
        if len(fav) < 3:
            continue
        au, asant, asanp = parse_combo(pr["umatan_c"]), parse_combo(pr["sanrentan_c"]), parse_combo(pr["sanrenpuku_c"])
        h6 = [c for c, _ in harville_trifectas(pmap)]
        p_harv_win = dict(harville_trifectas(pmap, top=len(pmap), k=10**9)).get(asant, np.nan) if asant else np.nan
        recs.append({
            "race_id": rid, "place_name": g["place_name"].iloc[0],
            "umatan_hit": int(au == (fav[0], fav[1])), "umatan_pay": pr["umatan_pay"] / 100.0,
            "sanrentan_hit": int(asant == (fav[0], fav[1], fav[2])), "sanrentan_pay": pr["sanrentan_pay"] / 100.0,
            "sanrenpuku_hit": int(asanp is not None and frozenset(asanp) == frozenset(fav[:3])),
            "sanrenpuku_pay": pr["sanrenpuku_pay"] / 100.0,
            "san_top6_hit": int(asant in h6),
            "p_harv_win": p_harv_win,
            "p_mkt_win": (1 - T_SANREN) / (pr["sanrentan_pay"] / 100.0) if pr["sanrentan_pay"] > 0 else np.nan,
        })
    return pd.DataFrame(recs)


def boot_ci(x, n=5000):
    if len(x) == 0:
        return (0.0, 0.0)
    rng = np.random.default_rng(0)
    idx = rng.integers(0, len(x), (n, len(x)))
    return tuple(np.percentile(x[idx].mean(axis=1), [2.5, 97.5]))


def report(name, df):
    print("\n" + "=" * 78)
    print(f"[{name}]  races={len(df)}")
    print("=" * 78)
    rows = [("馬単 本命(1>2)", "umatan_hit", "umatan_pay", 1.0, 1 - T_UMATAN),
            ("三連複 本命(上位3)", "sanrenpuku_hit", "sanrenpuku_pay", 1.0, 1 - T_SANPUKU),
            ("三連単 本命(1>2>3)", "sanrentan_hit", "sanrentan_pay", 1.0, 1 - T_SANREN),
            ("三連単 Harville上位6点", "san_top6_hit", "sanrentan_pay", 6.0, 1 - T_SANREN)]
    print(f"{'戦略':<22}{'的中率':>8}{'回収率':>9}{'95%CI':>18}{'分岐(1-控除)':>12}")
    for label, hc, pc, stake, be in rows:
        ret = df[hc].values * df[pc].values / stake
        lo, hi = boot_ci(ret)
        print(f"{label:<22}{df[hc].mean():>8.3f}{ret.mean():>9.3f}"
              f"{'[%.2f,%.2f]' % (lo, hi):>18}{be:>12.3f}")
    d = df.dropna(subset=["p_harv_win", "p_mkt_win"])
    d = d[(d["p_harv_win"] > 0) & (d["p_mkt_win"] > 0)]
    if len(d) > 10:
        lr = np.corrcoef(np.log(d["p_harv_win"]), np.log(d["p_mkt_win"]))[0, 1]
        ratio = float((d["p_harv_win"] / d["p_mkt_win"]).median())
        print(f"\nHarville echo(的中三連単, n={len(d)}): log-log corr={lr:.3f}, 中央比 p_harv/p_mkt={ratio:.2f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="data/nar_pilot")
    args = ap.parse_args()
    df = build(args.data_dir)
    if df.empty:
        print("払戻データが空。scrape_exotic.py の完了を待ってください。")
        return 1
    report("全場プール", df)
    for pn, g in df.groupby("place_name"):
        report(pn, g)
    print("\n読み取り: 本命連系の回収率が(1-控除)以下・CI下限>1.0なし → 連系も効率的(高控除ぶん更に負け)。")
    print("echo(corr高&比≈1)→連系は単勝Harvilleをなぞる=独立情報を持たない。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
