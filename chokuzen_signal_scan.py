"""未incorporatedの直前/展開signalが三連単順序の較正を改善するかをOOSで走査する。

これまで order signal は goal_juni/ichi_idx の2つだけ。KYIにパース済みで未使用の指数群
（idm/ten/pace/agari/dochu/go3f/gekiso激走/manken万券/joushoudo/start/deokure）が着順の
2/3着に情報を持つかを、単独＋joint で OOS ΔNLL(vs Harville)＋placebo＋bootstrap CI で判定。

規律: 較正改善(ΔNLL<0)は必要条件であって ROI ではない。placebo で消えないもの・多重比較で
偶然でないものだけが「ROI検証(ユーザー環境・return_tables)に進む候補」。CI で締める。

実行: python chokuzen_signal_scan.py --jrdb-dir /tmp/jrdb_all
"""
from __future__ import annotations

import argparse

import numpy as np

from src.policies._harville import prob_trifecta, prob_trifecta_place_strength
from src.simulation._order_model import load_races
from src.simulation._order_model import fit_signal_coef as fit_coef
from src.simulation._order_model import place_probs_from_signals as _place_probs

# 未incorporatedの候補（KYIにパース済み・archiveでOOS検証可能）
CANDIDATES = ("idm", "ten_idx", "pace_idx", "agari_idx", "dochu_juni", "go3f_juni",
              "gekiso_idx", "manken_idx", "joushoudo", "start_idx", "deokure_rate")


def oos_dnll(tr, te, signals, base):
    coef = fit_coef(tr, signals)
    chal = np.array([-np.log(max(prob_trifecta_place_strength(
        r["q"], _place_probs(r["q"], r["sig"], coef), *r["top3"]), 1e-12)) for r in te])
    return coef, chal - base


def placebo_dnll(te, signals, coef, base, rng):
    tep = []
    for r in te:
        keys = list(r["sig"])
        vals = [r["sig"][u] for u in keys]
        rng.shuffle(vals)
        rp = dict(r)
        rp["sig"] = dict(zip(keys, vals, strict=False))
        tep.append(rp)
    chal = np.array([-np.log(max(prob_trifecta_place_strength(
        r["q"], _place_probs(r["q"], r["sig"], coef), *r["top3"]), 1e-12)) for r in tep])
    return (chal - base).mean()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--jrdb-dir", default="/tmp/jrdb_all")
    ap.add_argument("--train-frac", type=float, default=0.6)
    args = ap.parse_args()

    allsig = ("goal_juni", "ichi_idx", *CANDIDATES)
    races, _ = load_races(args.jrdb_dir, signals=allsig)
    n_tr = int(len(races) * args.train_frac)
    tr, te = races[:n_tr], races[n_tr:]
    base = np.array([-np.log(max(prob_trifecta(r["q"], *r["top3"]), 1e-12)) for r in te])
    rng = np.random.default_rng(0)
    print(f"レース {len(races):,}（train {len(tr):,} / test {len(te):,}）  Harville NLL={base.mean():.4f}")

    print("\n【単独signal OOS ΔNLL（vs Harville・負=較正改善）＋placebo】既存2つ含む")
    print(f"{'signal':<14}{'coef':>7}{'ΔNLL':>10}{'CI下限':>9}{'CI上限':>9}{'placebo':>10}  判定")
    print("-" * 68)
    rows = []
    for s in ("goal_juni", "ichi_idx", *CANDIDATES):
        coef, d = oos_dnll(tr, te, (s,), base)
        boot = np.array([d[rng.integers(0, len(d), len(d))].mean() for _ in range(1000)])
        lo, hi = np.percentile(boot, 2.5), np.percentile(boot, 97.5)
        pl = placebo_dnll(te, (s,), coef, base, rng)
        real = hi < 0 and pl > -0.001
        rows.append((d.mean(), s))
        print(f"{s:<14}{coef[s]:>+7.2f}{d.mean():>+10.5f}{lo:>+9.5f}{hi:>+9.5f}{pl:>+10.5f}  "
              + ("★較正改善(placebo消失)" if real else ("要警戒:placebo残" if hi < 0 else "帰無")))

    # joint: 全候補＋既存
    print("\n【joint（全signal同時fit）OOS ΔNLL＋CI＋placebo】")
    coef, d = oos_dnll(tr, te, allsig, base)
    boot = np.array([d[rng.integers(0, len(d), len(d))].mean() for _ in range(2000)])
    lo, hi = np.percentile(boot, 2.5), np.percentile(boot, 97.5)
    pl = placebo_dnll(te, allsig, coef, base, rng)
    nz = {k: round(v, 2) for k, v in coef.items() if abs(v) > 1e-9}
    print(f"  非ゼロ係数: {nz}")
    print(f"  ΔNLL={d.mean():+.5f}  CI95=({lo:+.5f},{hi:+.5f})  placebo={pl:+.5f}")
    print(f"  → {'CI上端<0＝joint較正改善' if hi < 0 else 'CI 0跨ぎ＝有意でない'}"
          f" / {'placebo消失＝本物' if pl > -0.001 else 'placebo残＝要警戒'}")

    print("\n※ ΔNLL改善は較正であってROIではない（控除の壁は別）。改善signalは return_tables ある")
    print("  ユーザー環境で multibet_roi_test に追加し CI下限>1.0 を満たすかで最終判定。")


if __name__ == "__main__":
    main()
